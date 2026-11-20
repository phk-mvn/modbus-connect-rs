//! Connection tracker monitoring online/offline states of individual Modbus slave devices.
//! 
//! Features:
//! - Debounced disconnection events to prevent notification spam on flaky lines.
//! - Thread-safe state tracking.
//! - Instant dispatch of current states to new subscribers.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{oneshot, RwLock};

use super::types::{ConnectionErrorInfo, ConnectionErrorType, DeviceConnectionState};

/// Callback invoked when a device's connection status changes.
/// Arguments: `(slave_id, is_connected, error_info)`.
pub type DeviceStateHandler = Arc<dyn Fn(u8, bool, Option<&ConnectionErrorInfo>) + Send + Sync>;

struct TrackerInner {
    states: HashMap<u8, DeviceConnectionState>,
    handler: Option<DeviceStateHandler>,
    debounce_cancels: HashMap<u8, oneshot::Sender<()>>,
}

/// Tracks the online/offline state of individual Modbus devices with debounce logic.
#[derive(Clone)]
pub struct DeviceConnectionTracker {
    inner: Arc<RwLock<TrackerInner>>,
    debounce_duration: Duration,
}

impl DeviceConnectionTracker {
    /// Creates a new tracker with the specified disconnection debounce duration (default: 500ms).
    pub fn new(debounce_duration: Duration) -> Self {
        Self {
            inner: Arc::new(RwLock::new(TrackerInner {
                states: HashMap::new(),
                handler: None,
                debounce_cancels: HashMap::new(),
            })),
            debounce_duration,
        }
    }

    /// Sets the state change handler.
    /// Immediately notifies the handler of all currently tracked devices.
    pub async fn set_handler<F>(&self, handler: F)
    where
        F: Fn(u8, bool, Option<&ConnectionErrorInfo>) + Send + Sync + 'static,
    {
        let handler_arc: DeviceStateHandler = Arc::new(handler);
        let states_snapshot: Vec<DeviceConnectionState> = {
            let mut lock = self.inner.write().await;
            lock.handler = Some(handler_arc.clone());
            lock.states.values().cloned().collect()
        };

        for state in states_snapshot {
            handler_arc(state.slave_id, state.is_connected, state.error.as_ref());
        }
    }

    /// Removes the active state change handler.
    pub async fn remove_handler(&self) {
        let mut lock = self.inner.write().await;
        lock.handler = None;
    }

    /// Marks a slave device as connected.
    /// Cancels any pending debounce timer for disconnection.
    pub async fn notify_connected(&self, slave_id: u8) {
        let mut handler_to_call = None;

        {
            let mut lock = self.inner.write().await;
            // Cancel pending debounce if device reconnected in time
            if let Some(cancel_tx) = lock.debounce_cancels.remove(&slave_id) {
                let _ = cancel_tx.send(());
            }

            let was_connected = lock.states.get(&slave_id).map(|s| s.is_connected).unwrap_or(false);
            if !was_connected {
                let new_state = DeviceConnectionState {
                    slave_id,
                    is_connected: true,
                    error: None,
                    timestamp: SystemTime::now(),
                };
                lock.states.insert(slave_id, new_state);
                handler_to_call = lock.handler.clone();
            }
        }

        if let Some(handler) = handler_to_call {
            handler(slave_id, true, None);
        }
    }

    /// Triggers deounces disconnection notification for a slave device.
    pub async fn notify_disconnected(&self, slave_id: u8, error_type: ConnectionErrorType, message: impl Into<String>) {
        let (cancel_tx, mut cancel_rx) = oneshot::channel();
        let error_info = ConnectionErrorInfo {
            error_type,
            message: message.into(),
        };

        {
            let mut lock = self.inner.write().await;
            // Cancel any previous denounce timer for this slave
            if let Some(prev) = lock.debounce_cancels.insert(slave_id, cancel_tx) {
                let _ = prev.send(());
            }
        }

        let inner_clone = self.inner.clone();
        let debounce = self.debounce_duration;

        tokio::spawn(async move {
            tokio::select! {
                _ = &mut cancel_rx => {
                    // Reconnection occured before debounce elapsed, do no notify
                }
                _ = tokio::time::sleep(debounce) => {
                    let handler_to_call;
                    {
                        let mut lock = inner_clone.write().await;
                        lock.debounce_cancels.remove(&slave_id);
                        let new_state = DeviceConnectionState {
                            slave_id,
                            is_connected: false,
                            error: Some(error_info.clone()),
                            timestamp: SystemTime::now()
                        };
                        lock.states.insert(slave_id, new_state);
                        handler_to_call = lock.handler.clone();
                    }

                    if let Some(handler) = handler_to_call {
                        handler(slave_id, false, Some(&error_info));
                    }
                }
            }
        });
    }

    /// Returns the current state of a specific slave device.
    pub async fn get_state(&self, slave_id: u8) -> Option<DeviceConnectionState> {
        let lock = self.inner.read().await;
        lock.states.get(&slave_id).cloned()
    }

    /// Returns a list of all slave IDs currently marked as connected.
    pub async fn get_connected_slave_ids(&self) -> Vec<u8> {
        let lock = self.inner.read().await;
        lock.states
            .values()
            .filter(|s| s.is_connected)
            .map(|s| s.slave_id)
            .collect()
    }
}

impl Default for DeviceConnectionTracker {
    fn default() -> Self {
        Self::new(Duration::from_millis(500))
    }
}