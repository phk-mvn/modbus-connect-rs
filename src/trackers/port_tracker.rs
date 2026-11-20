//! Connection tracker monitoring online/offline status of a physical port / transport line.
//! 
//! Features:
//! - Debounced disconnection notifications to prevent false offline alarms.
//! - Tracks active slave IDs on the port.
//! - Thread-safe operations and instant dispatch of initial state to subscribers.

use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{oneshot, RwLock};

use super::types::{ConnectionErrorInfo, ConnectionErrorType, PortConnectionState};

/// Callback invoked when a port's connection status changes.
/// Arguments: `(is_connected, slave_ids, error_info)`.
pub type PortStateHandler = Arc<dyn Fn(bool, &[u8], Option<&ConnectionErrorInfo>) + Send + Sync>;

struct TrackerInner {
    state: PortConnectionState,
    handler: Option<PortStateHandler>,
    debounce_cancel: Option<oneshot::Sender<()>>,
}

/// Tracks the online/offline state of a physical communication port with debounce logic.
#[derive(Clone)]
pub struct PortConnectionTracker {
    inner: Arc<RwLock<TrackerInner>>,
    debounce_duration: Duration,
}

impl PortConnectionTracker {
    /// Creates a new port connection tracker with the specified debounce duration (default: 300ms).
    pub fn new(debounce_duration: Duration) -> Self {
        Self {
            inner: Arc::new(RwLock::new(TrackerInner {
                state: PortConnectionState {
                    is_connected: false,
                    slave_ids: Vec::new(),
                    error: None,
                    timestamp: SystemTime::now(),
                },
                handler: None,
                debounce_cancel: None,
            })),
            debounce_duration,
        }
    }

    /// Sets the state change handler and immediately dispatches the current port state to it.
    pub async fn set_handler<F>(&self, handler: F)
    where
        F: Fn(bool, &[u8], Option<&ConnectionErrorInfo>) + Send + Sync + 'static,
    {
        let handler_arc: PortStateHandler = Arc::new(handler);
        let current_state = {
            let mut lock = self.inner.write().await;
            lock.handler = Some(handler_arc.clone());
            lock.state.clone()
        };

        handler_arc(
            current_state.is_connected,
            &current_state.slave_ids,
            current_state.error.as_ref(),
        );
    }

    /// Remove the active state change handler.
    pub async fn remove_handler(&self) {
        let mut lock = self.inner.write().await;
        lock.handler = None;
    }

    /// Notifies that the port has become connected.
    /// Cancels any pending disconnection debounce timer.
    pub async fn notify_connected(&self, slave_ids: Vec<u8>) {
        let mut handler_to_call = None;
        let mut current_slaves = Vec::new();

        {
            let mut lock = self.inner.write().await;
            if let Some(cancel) = lock.debounce_cancel.take() {
                let _ = cancel.send(());
            }

            let was_connected = lock.state.is_connected;
            let slaves_changed = lock.state.slave_ids != slave_ids;

            lock.state = PortConnectionState {
                is_connected: true,
                slave_ids: slave_ids.clone(),
                error: None,
                timestamp: SystemTime::now(),
            };

            if !was_connected || slaves_changed {
                handler_to_call = lock.handler.clone();
                current_slaves = slave_ids;
            }
        }

        if let Some(handler) = handler_to_call {
            handler(true, &current_slaves, None);
        }
    }

    pub async fn notify_disconnected(&self, error_type: ConnectionErrorType, message: impl Into<String>) {
        let (cancel_tx, mut cancel_rx) = oneshot::channel();
        let error_info = ConnectionErrorInfo {
            error_type,
            message: message.into(),
        };

        {
            let mut lock = self.inner.write().await;
            if let Some(prev) = lock.debounce_cancel.replace(cancel_tx) {
                let _ = prev.send(());
            }
        }

        let inner_clone = self.inner.clone();
        let debounce = self.debounce_duration;

        tokio::spawn(async move {
            tokio::select! {
                _ = &mut cancel_rx => {
                    // Port reconnected before debounce elapsed
                }

                _ = tokio::time::sleep(debounce) => {
                    let handler_to_call;
                    let slaves;

                    {
                        let mut lock = inner_clone.write().await;
                        lock.debounce_cancel = None;
                        lock.state = PortConnectionState {
                            is_connected: false,
                            slave_ids: lock.state.slave_ids.clone(),
                            error: Some(error_info.clone()),
                            timestamp: SystemTime::now(),
                        };
                        handler_to_call = lock.handler.clone();
                        slaves = lock.state.slave_ids.clone();
                    }
    
                    if let Some(handler) = handler_to_call {
                        handler(false, &slaves, Some(&error_info));
                    }
                }

            }
        });
    }

    /// Returns a snaspshot of the current port connection state.
    pub async fn get_state(&self) -> PortConnectionState {
        let lock = self.inner.read().await;
        lock.state.clone()
    }
}

impl Default for PortConnectionTracker {
    fn default() -> Self {
        Self::new(Duration::from_millis(300))
    }
}