//! Central transport controller managing active Modbus sessions, clients, and polling tasks.
//!
//! [`TransportController`] is the primary orchestrator mirroring the TypeScript `modbus-connect` architecture:
//! - Manages multiple communication sessions (RTU serial ports, TCP sockets).
//! - Generates [`ModbusClient`] instances bound to specific slave IDs.
//! - Hosts transport-scoped [`PollingManager`] instances for background telemetry polling.
//! - Attaches optional [`TrafficSniffer`] instances to monitor wire traffic.
//!
//! # Example
//!
//! ```no_run
//! use std::time::Duration;
//! use modbus_connect::transport::TransportController;
//! use modbus_connect::core::ClientOptions;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let controller = TransportController::new();
//!
//!     // Add an RTU serial line
//!     #[cfg(feature = "rtu")]
//!     let port_id = controller.add_rtu("/dev/ttyUSB0", 115200).await?;
//!
//!     // Or add a Modbus TCP connection
//!     #[cfg(feature = "tcp")]
//!     let tcp_id = controller.add_tcp("192.168.1.100:502").await?;
//!
//!     # Ok(())
//! }
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;


use crate::core::client::{ClientOptions, ModbusClient};
use crate::core::framing::FramingKind;
use crate::error::{ModbusError, Result};
use crate::trackers::sniffer::TrafficSniffer;
use crate::transport::queue::PortQueueHandle;
use crate::transport::traits::Transport;
use crate::trackers::device_tracker::DeviceConnectionTracker;
use crate::trackers::port_tracker::PortConnectionTracker;
use crate::trackers::types::ConnectionErrorInfo;
use crate::transport::client_registry::{ClientRegistry, RegisteredClient};
use crate::transport::rs_mode::RsMode;

#[cfg(feature = "polling")]
use crate::polling::manager::{PollAction, PollingEvent, PollingManager, PollingTask};
#[cfg(feature = "polling")]
use tokio::sync::broadcast;

#[cfg(feature = "rtu")]
use crate::transport::rtu::{RtuConfig, RtuTransport};
#[cfg(feature = "tcp")]
use crate::transport::tcp::TcpTransport;

/// Active transport session containing its queue and optional polling manager.
#[derive(Clone)]
pub struct TransportSession {
    /// Unique identifier for this transport session.
    pub id: String,
    pub rs_mode: RsMode,
    /// Framing kind used on this transport (RTU or TCP).
    pub framing: FramingKind,
    /// Queue handle serializing access to the underlying physical port.
    pub queue: PortQueueHandle,
    pub port_tracker: PortConnectionTracker,
    pub device_tracker: DeviceConnectionTracker,
    /// Background telemetry poller associated with this transport.
    #[cfg(feature = "polling")]
    pub polling_manager: PollingManager,
}

/// Central controller managing registered transports, clients, and polling routines.
#[derive(Clone, Default)]
pub struct TransportController {
    sessions: Arc<RwLock<HashMap<String, TransportSession>>>,
    global_sniffer: Arc<RwLock<Option<TrafficSniffer>>>,
    client_registry: ClientRegistry,
}

/// Configuration options for creating a managed `ModbusClient` through the controller.
pub struct CreateClientOptions {
    /// Target Modbus slave address (1-247).
    pub slave_id: u8,
    /// Explicit transport ID. If omitted, controller auto-routes based on `rs_mode`.
    pub transport_id: Option<String>,
    /// Unique client identifier (e.g., "flow-meter-1"). auto-generated if omitted.
    pub client_id: Option<String>,
    /// Target physical mode used for auto-routing when `transport_id` if omitted
    pub rs_mode: Option<RsMode>,
    /// Allow duplicate slave ID on the ssame transport port (default: false).
    pub allow_duplicate_slave_id: bool,
    /// Request timeout and retry options.
    pub client_options: ClientOptions,
}

impl CreateClientOptions {
    pub fn new(slave_id: u8) -> Self {
        Self {
            slave_id,
            transport_id: None,
            client_id: None,
            rs_mode: None,
            allow_duplicate_slave_id: false,
            client_options: ClientOptions::default(),
        }
    }

    pub fn transport_id(mut self, id: impl Into<String>) -> Self {
        self.transport_id = Some(id.into());
        self
    }

    pub fn client_id(mut self, id: impl Into<String>) -> Self {
        self.client_id = Some(id.into());
        self
    }

    pub fn rs_mode(mut self, mode: RsMode) -> Self {
        self.rs_mode = Some(mode);
        self
    }

    pub fn allow_duplicate_slave_id(mut self, allow: bool) -> Self {
        self.allow_duplicate_slave_id = allow;
        self
    }

    pub fn options(mut self, options: ClientOptions) -> Self {
        self.client_options = options;
        self
    }
}

/// Snapshot summary of a transport line status.
#[derive(Debug, Clone)]
pub struct TransportStatus {
    pub id: String,
    pub rs_mode: RsMode,
    pub is_connected: bool,
    pub slave_ids: Vec<u8>,
    pub clients_count: usize,
    pub last_error: Option<ConnectionErrorInfo>,
}

impl TransportController {
    /// Creates a new, empty [`TransportController`].
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            global_sniffer: Arc::new(RwLock::new(None)),
            client_registry: ClientRegistry::new(),
        }
    }

    /// Creates a controller configured with a global [`TrafficSniffer`] of the given ring buffer capacity.
    pub fn with_sniffer(sniffer_capacity: usize) -> Self {
        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            global_sniffer: Arc::new(RwLock::new(Some(TrafficSniffer::new(sniffer_capacity)))),
            client_registry: ClientRegistry::new()
        }
    }

    /// Returns a clone of the global traffic sniffer, if one is configured.
    pub async fn sniffer(&self) -> Option<TrafficSniffer> {
        self.global_sniffer.read().await.clone()
    }

    /// Registers a custom [`Transport`] under the specified unique session ID.
    pub async fn add_transport<T: Transport + 'static>(
        &self,
        id: impl Into<String>,
        framing: FramingKind,
        transport: T,
        queue_capacity: usize,
    ) -> Result<()> {
        let sniffer = self.global_sniffer.read().await.clone();
        self.add_transport_internal(id, framing, transport, queue_capacity, sniffer).await
    }

    /// Registers a custom [`Transport`] with a dedicated [`TrafficSniffer`].
    pub async fn add_transport_with_sniffer<T: Transport + 'static>(
        &self,
        id: impl Into<String>,
        framing: FramingKind,
        transport: T,
        queue_capacity: usize,
        sniffer: Option<TrafficSniffer>,
    ) -> Result<()> {
        self.add_transport_internal(id, framing, transport, queue_capacity, sniffer).await
    }

    async fn add_transport_internal<T: Transport + 'static>(
        &self,
        id: impl Into<String>,
        framing: FramingKind,
        transport: T,
        queue_capacity: usize,
        sniffer: Option<TrafficSniffer>,
    ) -> Result<()> {
        let id = id.into();
        let mut sessions = self.sessions.write().await;

        if sessions.contains_key(&id) {
            return Err(ModbusError::InvalidFrame(format!(
                "Transport with id '{}' already exists",
                id
            )));
        }

        let queue = PortQueueHandle::with_sniffer(transport, queue_capacity, sniffer);
        #[cfg(feature = "polling")]
        let polling_manager = PollingManager::new(256);

        let rs_mode = match framing {
            FramingKind::Rtu => RsMode::Rs485,
            FramingKind::Tcp => RsMode::Tcp,
        };
        let port_tracker = PortConnectionTracker::default();
        let device_tracker = DeviceConnectionTracker::default();

        let session = TransportSession {
            id: id.clone(),
            rs_mode,
            framing,
            queue,
            port_tracker,
            device_tracker,
            #[cfg(feature = "polling")]
            polling_manager,
        };

        sessions.insert(id, session);
        Ok(())
    }

    /// Opens and registers an RTU serial line using an explicit [`RtuConfig`].
    ///
    /// Requires the `rtu` crate feature.
    #[cfg(feature = "rtu")]
    pub async fn add_rtu_transport(
        &self,
        id: impl Into<String>,
        config: RtuConfig,
    ) -> Result<()> {
        let transport = RtuTransport::open(config)?;
        self.add_transport(id, FramingKind::Rtu, transport, 100).await
    }

    /// Ergonomic shortcut to open an RTU serial port passing the device path directly.
    ///
    /// Uses 8N1 default settings and the path as the transport ID.
    /// Returns the registered transport ID (e.g. `"/dev/ttyUSB0"`).
    ///
    /// Requires the `rtu` crate feature.
    #[cfg(feature = "rtu")]
    pub async fn add_rtu(
        &self,
        path: impl Into<String>,
        baud_rate: u32,
    ) -> Result<String> {
        let path = path.into();
        let id = path.clone();
        let config = RtuConfig::new(&path, baud_rate);
        self.add_rtu_transport(&id, config).await?;
        Ok(id)
    }

    /// Connects and registers a Modbus TCP transport with an explicit ID.
    ///
    /// Requires the `tcp` crate feature.
    #[cfg(feature = "tcp")]
    pub async fn add_tcp_transport(
        &self,
        id: impl Into<String>,
        addr: impl Into<String>,
    ) -> Result<()> {
        let transport = TcpTransport::connect(addr).await?;
        self.add_transport(id, FramingKind::Tcp, transport, 100).await
    }

    /// Ergonomic shortcut to connect a Modbus TCP transport directly via host/address string.
    ///
    /// Uses the address string as the transport ID (e.g. `"127.0.0.1:502"`).
    /// Returns the registered transport ID.
    ///
    /// Requires the `tcp` crate feature.
    #[cfg(feature = "tcp")]
    pub async fn add_tcp(
        &self,
        addr: impl Into<String>,
    ) -> Result<String> {
        let addr = addr.into();
        let id = addr.clone();
        self.add_tcp_transport(&id, &addr).await?;
        Ok(id)
    }

    /// Retrieves the [`PortQueueHandle`] for the specified transport ID.
    pub async fn get_queue(&self, id: &str) -> Result<PortQueueHandle> {
        let sessions = self.sessions.read().await;
        sessions
            .get(id)
            .map(|s| s.queue.clone())
            .ok_or_else(|| ModbusError::NotConnected(format!("Transport '{}' not found", id)))
    }

    /// Retrieves the [`PollingManager`] for the specified transport ID.
    ///
    /// Requires the `polling` crate feature.
    #[cfg(feature = "polling")]
    pub async fn get_polling_manager(&self, transport_id: &str) -> Result<PollingManager> {
        let sessions = self.sessions.read().await;
        sessions
            .get(transport_id)
            .map(|s| s.polling_manager.clone())
            .ok_or_else(|| ModbusError::NotConnected(format!("Transport '{}' not found", transport_id)))
    }

    /// Registers a [`PollingTask`] on the specified transport session.
    ///
    /// Requires the `polling` crate feature.
    #[cfg(feature = "polling")]
    pub async fn add_polling_task(
        &self,
        transport_id: &str,
        task: PollingTask,
    ) -> Result<()> {
        let poller = self.get_polling_manager(transport_id).await?;
        poller.add_task(task).await
    }

    /// Registers a simple closure or action as a recurring polling task.
    ///
    /// Requires the `polling` crate feature.
    #[cfg(feature = "polling")]
    pub async fn add_simple_polling_task<A: PollAction>(
        &self,
        transport_id: &str,
        task_id: impl Into<String>,
        interval: Duration,
        action: A,
    ) -> Result<()> {
        let poller = self.get_polling_manager(transport_id).await?;
        poller.add_simple_task(task_id, interval, action).await
    }

    /// Subscribes to polling events (e.g. data updates, errors) for the specified transport.
    ///
    /// Requires the `polling` crate feature.
    #[cfg(feature = "polling")]
    pub async fn subscribe_polling(&self, transport_id: &str) -> Result<broadcast::Receiver<PollingEvent>> {
        let poller = self.get_polling_manager(transport_id).await?;
        Ok(poller.subscribe())
    }

    /// Pauses an active polling task by ID.
    ///
    /// Requires the `polling` crate feature.
    #[cfg(feature = "polling")]
    pub async fn pause_polling(&self, transport_id: &str, task_id: &str) -> Result<()> {
        let poller = self.get_polling_manager(transport_id).await?;
        poller.pause_task(task_id).await
    }

    /// Resumes a paused polling task by ID.
    ///
    /// Requires the `polling` crate feature.
    #[cfg(feature = "polling")]
    pub async fn resume_polling(&self, transport_id: &str, task_id: &str) -> Result<()> {
        let poller = self.get_polling_manager(transport_id).await?;
        poller.resume_task(task_id).await
    }

    /// Restarts a polling task with its original configured timer.
    ///
    /// Requires the `polling` crate feature.
    #[cfg(feature = "polling")]
    pub async fn restart_polling(&self, transport_id: &str, task_id: &str) -> Result<()> {
        let poller = self.get_polling_manager(transport_id).await?;
        poller.restart_task(task_id).await
    }

    /// Dynamically alters the repeat interval of a polling task.
    ///
    /// Requires the `polling` crate feature.
    #[cfg(feature = "polling")]
    pub async fn set_polling_interval(&self, transport_id: &str, task_id: &str, interval: Duration) -> Result<()> {
        let poller = self.get_polling_manager(transport_id).await?;
        poller.set_task_interval(task_id, interval).await
    }

    /// Returns `true` if the specified polling task is currently running.
    ///
    /// Requires the `polling` crate feature.
    #[cfg(feature = "polling")]
    pub async fn is_polling_running(&self, transport_id: &str, task_id: &str) -> Result<bool> {
        let poller = self.get_polling_manager(transport_id).await?;
        Ok(poller.is_task_running(task_id).await)
    }

    /// Returns `true` if the specified polling task is currently paused.
    ///
    /// Requires the `polling` crate feature.
    #[cfg(feature = "polling")]
    pub async fn is_polling_paused(&self, transport_id: &str, task_id: &str) -> Result<bool> {
        let poller = self.get_polling_manager(transport_id).await?;
        Ok(poller.is_task_paused(task_id).await)
    }

    /// Returns `true` if a polling task with the given ID exists on this transport.
    ///
    /// Requires the `polling` crate feature.
    #[cfg(feature = "polling")]
    pub async fn has_polling_task(&self, transport_id: &str, task_id: &str) -> Result<bool> {
        let poller = self.get_polling_manager(transport_id).await?;
        Ok(poller.has_task(task_id).await)
    }

    /// Lists all polling task IDs registered on the specified transport.
    ///
    /// Requires the `polling` crate feature.
    #[cfg(feature = "polling")]
    pub async fn get_polling_task_ids(&self, transport_id: &str) -> Result<Vec<String>> {
        let poller = self.get_polling_manager(transport_id).await?;
        Ok(poller.get_task_ids().await)
    }

    /// Pauses all polling tasks running on the specified transport.
    ///
    /// Requires the `polling` crate feature.
    #[cfg(feature = "polling")]
    pub async fn pause_all_polling(&self, transport_id: &str) -> Result<()> {
        let poller = self.get_polling_manager(transport_id).await?;
        poller.pause_all().await;
        Ok(())
    }

    /// Resumes all paused polling tasks on the specified transport.
    ///
    /// Requires the `polling` crate feature.
    #[cfg(feature = "polling")]
    pub async fn resume_all_polling(&self, transport_id: &str) -> Result<()> {
        let poller = self.get_polling_manager(transport_id).await?;
        poller.resume_all().await;
        Ok(())
    }

    /// Stops and removes all polling tasks on the specified transport.
    ///
    /// Requires the `polling` crate feature.
    #[cfg(feature = "polling")]
    pub async fn stop_all_polling(&self, transport_id: &str) -> Result<()> {
        let poller = self.get_polling_manager(transport_id).await?;
        poller.clear_all().await;
        Ok(())
    }

    /// Alias for [`stop_all_polling`](Self::stop_all_polling).
    ///
    /// Requires the `polling` crate feature.
    #[cfg(feature = "polling")]
    pub async fn clear_all_polling(&self, transport_id: &str) -> Result<()> {
        let poller = self.get_polling_manager(transport_id).await?;
        poller.clear_all().await;
        Ok(())
    }

    /// Removes an individual polling task by ID.
    ///
    /// Requires the `polling` crate feature.
    #[cfg(feature = "polling")]
    pub async fn remove_polling_task(&self, transport_id: &str, task_id: &str) -> Result<()> {
        let poller = self.get_polling_manager(transport_id).await?;
        poller.remove_task(task_id).await
    }

    /// Creates a [`ModbusClient`] bound to the specified transport and `slave_id`.
    ///
    /// # Errors
    ///
    /// Returns [`ModbusError::NotConnected`] if `transport_id` is not registered,
    /// or [`ModbusError::InvalidSlaveId`] if `slave_id` is invalid.
    pub async fn create_client(
        &self,
        transport_id: &str,
        slave_id: u8,
        options: ClientOptions,
    ) -> Result<ModbusClient> {
        let opts = CreateClientOptions::new(slave_id)
            .transport_id(transport_id)
            .options(options);
        self.create_managed_client(opts).await
    }

    /// Creates a fully managed ModbusClient enforcing RS-mode constraints, client uniqueness,
    /// and auto-routing.
    pub async fn create_managed_client(&self, opts: CreateClientOptions) -> Result<ModbusClient> {
        let (target_transport_id, framing, queue) = {
            let sessions = self.sessions.read().await;

            let target_id = if let Some(ref tid) = opts.transport_id {
                tid.clone()
            } else {
                let target_mode = opts.rs_mode.unwrap_or(RsMode::Rs485);
                let mut found = None;
                for (id, s) in sessions.iter() {
                    if s.rs_mode == target_mode {
                        if let Some(max) = s.rs_mode.max_devices() {
                            let count = self.client_registry.list(Some(id)).await.len();
                            if count >= max {
                                continue;
                            }
                        }
                        found = Some(id.clone());
                        break;
                    }
                }
                found.ok_or_else(|| {
                    ModbusError::NotConnected("No suitable transport session found for auto-routing".into())
                })?
            };

            let session = sessions.get(&target_id).ok_or_else(|| {
                ModbusError::NotConnected(format!("Transport '{}' not found", target_id))
            })?;

            // Enforce RS232 single device constraint
            if let Some(max_devices) = session.rs_mode.max_devices() {
                let existing_clients = self.client_registry.list(Some(&target_id)).await;
                if existing_clients.len() >= max_devices {
                    return Err(ModbusError::RsModeConstraint(format!(
                        "Transport '{}' is in {:?} mode which strictly permits at most {} device(s)",
                        target_id, session.rs_mode, max_devices
                    )));
                }
            }

            (target_id, session.framing, session.queue.clone())
        };

        let client_id = opts.client_id.unwrap_or_else(|| {
            format!("client-{}-{}", target_transport_id, opts.slave_id)
        });

        let client = ModbusClient::new(opts.slave_id, framing, queue, opts.client_options)?;

        self.client_registry
            .register(
                &client_id,
                opts.slave_id,
                &target_transport_id,
                client.clone(),
                opts.allow_duplicate_slave_id,
            )
            .await?;

        // Update port and device trackers
        if let Some(session) = self.sessions.read().await.get(&target_transport_id) {
            session.device_tracker.notify_connected(opts.slave_id).await;
            let connected_slaves = session.device_tracker.get_connected_slave_ids().await;
            session.port_tracker.notify_connected(connected_slaves).await;
        }

        Ok(client)
    }

    /// Sets the physical RS mode (e.g. `RsMode::Rs232`) on an existing transport session.
    pub async fn set_rs_mode(&self, transport_id: &str, mode: RsMode) -> Result<()> {
        let mut sessions = self.sessions.write().await;
        let session = sessions.get_mut(transport_id).ok_or_else(|| {
            ModbusError::NotConnected(format!("Transport '{}' not found", transport_id))
        })?;
        session.rs_mode = mode;
        Ok(())
    }

    /// Retrieves a client by its unique client ID.
    pub async fn get_client(&self, client_id: &str) -> Option<ModbusClient> {
        self.client_registry.get(client_id).await
    }

    /// Lists all registered clients, optionally filtered by `transport_id`.
    pub async fn list_clients(&self, transport_id: Option<&str>) -> Vec<RegisteredClient> {
        self.client_registry.list(transport_id).await
    }

    /// Removes a client from the registry and updates device trackers.
    pub async fn remove_client(&self, client_id: &str) -> Result<()> {
        if let Some(removed) = self.client_registry.remove(client_id).await {
            if let Some(session) = self.sessions.read().await.get(&removed.transport_id) {
                let connected_slaves = self
                    .client_registry
                    .list(Some(&removed.transport_id))
                    .await
                    .into_iter()
                    .map(|c| c.slave_id)
                    .collect();
                session.port_tracker.notify_connected(connected_slaves).await;
            }
            Ok(())
        } else {
            Err(ModbusError::ClientNotFound(client_id.into()))
        }
    }

    /// Reassigns an existing client to a new slave ID.
    pub async fn reassign_client(
        &self,
        client_id: &str,
        new_slave_id: u8,
        allow_duplicate: bool,
    ) -> Result<()> {
        let registered = self
            .client_registry
            .get_entry(client_id)
            .await
            .ok_or_else(|| ModbusError::ClientNotFound(client_id.into()))?;

        // Remove old registration
        self.client_registry.remove(client_id).await;

        // Create new client with updated slave ID
        let sessions = self.sessions.read().await;
        let session = sessions.get(&registered.transport_id).ok_or_else(|| {
            ModbusError::NotConnected(format!("Transport '{}' not found", registered.transport_id))
        })?;

        let new_client = ModbusClient::new(
            new_slave_id,
            session.framing,
            session.queue.clone(),
            ClientOptions::default(),
        )?;

        self.client_registry
            .register(
                client_id,
                new_slave_id,
                &registered.transport_id,
                new_client,
                allow_duplicate,
            )
            .await?;

        session.device_tracker.notify_connected(new_slave_id).await;
        let connected_slaves = session.device_tracker.get_connected_slave_ids().await;
        session.port_tracker.notify_connected(connected_slaves).await;

        Ok(())
    }

    /// Directly transmits raw bytes to a transport's port queue with immediate priority.
    pub async fn write_to_port(
        &self,
        transport_id: &str,
        raw_frame: Vec<u8>,
        timeout: Duration,
    ) -> Result<Vec<u8>> {
        let queue = self.get_queue(transport_id).await?;
        queue.send_frame(raw_frame, crate::transport::Priority::Immediate, timeout).await
    }

    /// Returns a diagnostic status snapshot for a given transport.
    pub async fn get_status(&self, transport_id: &str) -> Result<TransportStatus> {
        let sessions = self.sessions.read().await;
        let session = sessions.get(transport_id).ok_or_else(|| {
            ModbusError::NotConnected(format!("Transport '{}' not found", transport_id))
        })?;

        let port_state = session.port_tracker.get_state().await;
        let clients_count = self.client_registry.list(Some(transport_id)).await.len();

        Ok(TransportStatus {
            id: transport_id.into(),
            rs_mode: session.rs_mode,
            is_connected: port_state.is_connected,
            slave_ids: port_state.slave_ids,
            clients_count,
            last_error: port_state.error,
        })
    }

    /// Returns the number of currently active / open transports.
    pub async fn get_active_transport_count(&self) -> usize {
        let sessions = self.sessions.read().await;
        let mut active = 0;
        for s in sessions.values() {
            if s.port_tracker.get_state().await.is_connected {
                active += 1;
            }
        }
        active
    }

    /// Pauses transaction processing on the specified transport queue.
    pub async fn pause_transport(&self, id: &str) -> Result<()> {
        let queue = self.get_queue(id).await?;
        queue.pause().await
    }

    /// Resumes transaction processing on the specified transport queue.
    pub async fn resume_transport(&self, id: &str) -> Result<()> {
        let queue = self.get_queue(id).await?;
        queue.resume().await
    }

    /// Closes and unregisters a transport session.
    pub async fn remove_transport(&self, id: &str) -> Result<()> {
        let mut sessions = self.sessions.write().await;
        if let Some(session) = sessions.remove(id) {
            let _ = session.queue.close().await;
            self.client_registry.remove_by_transport(id).await;
            Ok(())
        } else {
            Err(ModbusError::NotConnected(format!(
                "Transport '{}' not found",
                id
            )))
        }
    }

    /// Returns a list of all currently registered transport session IDs.
    pub async fn list_transports(&self) -> Vec<String> {
        let sessions = self.sessions.read().await;
        sessions.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use crate::core::crc::crc16;

    struct MockControllerTransport {
        slave_id: u8,
    }

    #[async_trait]
    impl Transport for MockControllerTransport {
        async fn exchange(&mut self, _request: &[u8], _timeout: Duration) -> Result<Vec<u8>> {
            let mut resp = vec![self.slave_id, 0x03, 0x02, 0x11, 0x22];
            let crc = crc16(&resp);
            resp.push((crc & 0xFF) as u8);
            resp.push((crc >> 8) as u8);
            Ok(resp)
        }
        async fn flush(&mut self) -> Result<()> { Ok(()) }
        async fn close(&mut self) -> Result<()> { Ok(()) }
        fn is_open(&self) -> bool { true }
    }

    #[tokio::test]
    async fn test_transport_controller_lifecycle() {
        let controller = TransportController::new();
        controller
            .add_transport(
                "RTU_LINE_1",
                FramingKind::Rtu,
                MockControllerTransport { slave_id: 10 },
                50,
            )
            .await
            .unwrap();
        assert_eq!(controller.list_transports().await, vec!["RTU_LINE_1"]);
        let client = controller
            .create_client("RTU_LINE_1", 10, ClientOptions::default())
            .await
            .unwrap();
        assert_eq!(client.slave_id(), 10);
        let data = client.read_holding_registers(0, 1).await.unwrap();
        assert_eq!(data.get_u16(0).unwrap(), 0x1122);
        controller.remove_transport("RTU_LINE_1").await.unwrap();
        assert!(controller.list_transports().await.is_empty());
    }

    #[tokio::test]
    async fn test_controller_client_registry_and_rs232_constraint() {
        let controller = TransportController::new();
        controller
            .add_transport(
                "RS232_PORT",
                FramingKind::Rtu,
                MockControllerTransport { slave_id: 1 },
                50,
            )
            .await
            .unwrap();

        controller.set_rs_mode("RS232_PORT", RsMode::Rs232).await.unwrap();

        // Register first client - should succeed
        let client1 = controller
            .create_managed_client(
                CreateClientOptions::new(1)
                    .transport_id("RS232_PORT")
                    .client_id("sensor-1"),
            )
            .await
            .unwrap();
        assert_eq!(client1.slave_id(), 1);

        // Attempt second client on RS232 line - must fail with RsModeConstraint
        let err = controller
            .create_managed_client(
                CreateClientOptions::new(2)
                    .transport_id("RS232_PORT")
                    .client_id("sensor-2"),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, ModbusError::RsModeConstraint(_)));

        // Status check
        let status = controller.get_status("RS232_PORT").await.unwrap();
        assert_eq!(status.clients_count, 1);
        assert_eq!(status.rs_mode, RsMode::Rs232);

        // Remove client
        controller.remove_client("sensor-1").await.unwrap();
        assert!(controller.get_client("sensor-1").await.is_none());
    }

    #[tokio::test]
    async fn test_controller_duplicate_slave_and_reassign() {
        let controller = TransportController::new();
        controller
            .add_transport(
                "RS485_PORT",
                FramingKind::Rtu,
                MockControllerTransport { slave_id: 10 },
                50,
            )
            .await
            .unwrap();

        // Register client with id
        controller
            .create_managed_client(
                CreateClientOptions::new(10)
                    .transport_id("RS485_PORT")
                    .client_id("dev-10"),
            )
            .await
            .unwrap();

        // Register duplicate slave on same transport without flag -> should fail
        let err = controller
            .create_managed_client(
                CreateClientOptions::new(10)
                    .transport_id("RS485_PORT")
                    .client_id("dev-10-bis"),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, ModbusError::DuplicateSlaveId(10, _)));

        // Reassign client to new slave ID
        controller
            .reassign_client("dev-10", 15, false)
            .await
            .unwrap();
        let clients = controller.list_clients(Some("RS485_PORT")).await;
        assert_eq!(clients.len(), 1);
        assert_eq!(clients[0].slave_id, 15);

        // Write raw bytes to port
        let mut raw = vec![10, 0x03, 0x00, 0x00, 0x00, 0x01];
        let crc = crc16(&raw);
        raw.push((crc & 0xFF) as u8);
        raw.push((crc >> 8) as u8);
        let resp = controller
            .write_to_port("RS485_PORT", raw, Duration::from_millis(500))
            .await
            .unwrap();
        assert!(!resp.is_empty());
    }
}
