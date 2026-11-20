//! Synchronous blocking wrappers for applications that do not use async/await.
//!
//! This module provides blocking versions of core primitives ([`ModbusClient`],
//! [`TransportController`], [`PollingManager`], [`PollingTask`], and [`BoundSchema`]).
//! Each blocking struct embeds and manages a Tokio [`Runtime`], executing asynchronous
//! futures via [`Runtime::block_on`] transparently.
//!
//! # Example
//!
//! ```no_run
//! use modbus_connect::blocking::TransportController;
//! use modbus_connect::polling::PollingTask;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let controller = TransportController::new()?;
//!
//!     #[cfg(feature = "rtu")]
//!     let port_id = controller.add_rtu("/dev/ttyUSB0", 115200)?;
//!
//!     # #[cfg(feature = "rtu")]
//!     let client = controller.create_client(&port_id, 1)?;
//!     # #[cfg(feature = "rtu")]
//!     let data = client.read_holding_registers(0, 2)?;
//!     # #[cfg(feature = "rtu")]
//!     println!("Register 0: {}", data[0]);
//!
//!     Ok(())
//! }
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;

use crate::core::client::{ClientOptions, ModbusClient as AsyncModbusClient};
use crate::core::framing::FramingKind;
use crate::core::register_data::RegisterData;
use crate::error::{ModbusError, Result};
use crate::transport::controller::TransportController as AsyncTransportController;
use crate::transport::traits::Transport;
use crate::transport::{CreateClientOptions, RegisteredClient, RsMode, TransportStatus};

#[cfg(feature = "rtu")]
use crate::transport::rtu::RtuConfig;

#[cfg(feature = "schema")]
use crate::schema::bound::BoundSchema as AsyncBoundSchema;
#[cfg(feature = "schema")]
use crate::schema::types::FieldValue;

#[cfg(feature = "polling")]
use crate::polling::manager::{PollingEvent, PollingManager as AsyncPollingManager, PollingTask as AsyncPollingTask};

/// Synchronous, blocking Modbus client.
///
/// Wraps an underlying [`AsyncModbusClient`] and drives asynchronous operations using an embedded Tokio [`Runtime`].
#[derive(Clone)]
pub struct ModbusClient {
    runtime: Arc<Runtime>,
    inner: AsyncModbusClient,
}

impl std::fmt::Debug for ModbusClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ModbusClient")
            .field("inner", &self.inner)
            .finish()
    }
}

impl ModbusClient {
    /// Creates a blocking client from an existing async client by initializing a new single/multi-thread Tokio runtime.
    pub fn new(inner: AsyncModbusClient) -> Result<Self> {
        let runtime = Runtime::new().map_err(ModbusError::Io)?;
        Ok(Self {
            runtime: Arc::new(runtime),
            inner,
        })
    }

    /// Creates a blocking client sharing an existing Tokio [`Runtime`].
    pub fn with_runtime(inner: AsyncModbusClient, runtime: Arc<Runtime>) -> Self {
        Self { runtime, inner }
    }

    /// Constructs a blocking client from an arbitrary underlying [`Transport`].
    pub fn from_transport<T: Transport + 'static>(
        slave_id: u8,
        framing: FramingKind,
        transport: T,
        options: ClientOptions,
    ) -> Result<Self> {
        let runtime = Runtime::new().map_err(ModbusError::Io)?;
        let _guard = runtime.enter();
        let async_client = AsyncModbusClient::from_transport(slave_id, framing, transport, options)?;
        Ok(Self {
            runtime: Arc::new(runtime),
            inner: async_client,
        })
    }

    /// Constructs a blocking client from an underlying transport with a traffic sniffer attached.
    pub fn with_sniffer<T: Transport + 'static>(
        slave_id: u8,
        framing: FramingKind,
        transport: T,
        sniffer: Option<crate::trackers::sniffer::TrafficSniffer>,
        options: ClientOptions,
    ) -> Result<Self> {
        let runtime = Runtime::new().map_err(ModbusError::Io)?;
        let _guard = runtime.enter();
        let async_client = AsyncModbusClient::with_sniffer(slave_id, framing, transport, sniffer, options)?;
        Ok(Self {
            runtime: Arc::new(runtime),
            inner: async_client,
        })
    }

    /// Connects to a serial RTU port with default 8N1 options.
    ///
    /// Requires the `rtu` crate feature.
    #[cfg(feature = "rtu")]
    pub fn rtu(path: impl Into<String>, baud_rate: u32, slave_id: u8) -> Result<Self> {
        Self::rtu_with_options(path, baud_rate, slave_id, ClientOptions::default())
    }

    /// Connects to a serial RTU port with custom client options.
    ///
    /// Requires the `rtu` crate feature.
    #[cfg(feature = "rtu")]
    pub fn rtu_with_options(
        path: impl Into<String>,
        baud_rate: u32,
        slave_id: u8,
        options: ClientOptions,
    ) -> Result<Self> {
        let runtime = Runtime::new().map_err(ModbusError::Io)?;
        let _guard = runtime.enter();
        let config = RtuConfig::new(path, baud_rate);
        let async_client = AsyncModbusClient::rtu(config, slave_id, options)?;
        Ok(Self {
            runtime: Arc::new(runtime),
            inner: async_client,
        })
    }

    /// Connects to a serial RTU port with a traffic sniffer attached.
    ///
    /// Requires the `rtu` crate feature.
    #[cfg(feature = "rtu")]
    pub fn rtu_with_sniffer(
        path: impl Into<String>,
        baud_rate: u32,
        slave_id: u8,
        sniffer: Option<crate::trackers::sniffer::TrafficSniffer>,
        options: ClientOptions,
    ) -> Result<Self> {
        let runtime = Runtime::new().map_err(ModbusError::Io)?;
        let _guard = runtime.enter();
        let config = RtuConfig::new(path, baud_rate);
        let async_client = AsyncModbusClient::rtu_with_sniffer(config, slave_id, sniffer, options)?;
        Ok(Self {
            runtime: Arc::new(runtime),
            inner: async_client,
        })
    }

    /// Connects to a Modbus TCP server with default client options.
    ///
    /// Requires the `tcp` crate feature.
    #[cfg(feature = "tcp")]
    pub fn tcp(addr: impl Into<String>, slave_id: u8) -> Result<Self> {
        Self::tcp_with_options(addr, slave_id, ClientOptions::default())
    }

    /// Connects to a Modbus TCP server with custom client options.
    ///
    /// Requires the `tcp` crate feature.
    #[cfg(feature = "tcp")]
    pub fn tcp_with_options(
        addr: impl Into<String>,
        slave_id: u8,
        options: ClientOptions,
    ) -> Result<Self> {
        let runtime = Runtime::new().map_err(ModbusError::Io)?;
        let addr = addr.into();
        let async_client = {
            let _guard = runtime.enter();
            runtime.block_on(AsyncModbusClient::tcp(addr, slave_id, options))?
        };
        Ok(Self {
            runtime: Arc::new(runtime),
            inner: async_client,
        })
    }

    /// Connects to a Modbus TCP server with a traffic sniffer attached.
    ///
    /// Requires the `tcp` crate feature.
    #[cfg(feature = "tcp")]
    pub fn tcp_with_sniffer(
        addr: impl Into<String>,
        slave_id: u8,
        sniffer: Option<crate::trackers::sniffer::TrafficSniffer>,
        options: ClientOptions,
    ) -> Result<Self> {
        let runtime = Runtime::new().map_err(ModbusError::Io)?;
        let addr = addr.into();
        let async_client = {
            let _guard = runtime.enter();
            runtime.block_on(AsyncModbusClient::tcp_with_sniffer(addr, slave_id, sniffer, options))?
        };
        Ok(Self {
            runtime: Arc::new(runtime),
            inner: async_client,
        })
    }

    #[cfg(feature = "plugin")]
    pub fn use_plugin<P: crate::plugin::ModbusPlugin>(&self, plugin: P) {
        self.inner.use_plugin(plugin);
    }

    #[cfg(feature = "plugin")]
    pub fn execute_custom_function(&self, function_name: &str, args: &[u8]) -> Result<Vec<u8>> {
        self.runtime.block_on(self.inner.execute_custom_function(function_name, args))
    }

    pub fn raw_exchange(&self, pdu: &[u8], priority: crate::transport::Priority) -> Result<Vec<u8>> {
        self.raw_exchange_opt(pdu, priority, false)
    }

    pub fn raw_exchange_opt(
        &self,
        pdu: &[u8],
        priority: crate::transport::Priority,
        ignore_no_response: bool,
    ) -> Result<Vec<u8>> {
        self.runtime.block_on(self.inner.raw_exchange_opt(pdu, priority, ignore_no_response))
    }

    /// Returns the target Modbus slave/unit ID.
    pub fn slave_id(&self) -> u8 {
        self.inner.slave_id()
    }

    /// Dynamically updates the Modbus slave address for subsequent requests.
    pub fn set_slave_id(&mut self, new_slave_id: u8) -> Result<()> {
        self.inner.set_slave_id(new_slave_id)
    }

    /// Attaches an isolated device state callback invoked when connection to this slave is lost or restored.
    pub fn set_device_state_handler<F>(&self, handler: F)
    where
        F: Fn(u8, bool, Option<&crate::trackers::ConnectionErrorInfo>) + Send + Sync + 'static,
    {
        self.runtime.block_on(self.inner.set_device_state_handler(handler))
    }

    /// Accesses the underlying asynchronous [`AsyncModbusClient`].
    pub fn inner(&self) -> &AsyncModbusClient {
        &self.inner
    }

    /// Accesses the underlying Tokio [`Runtime`].
    pub fn runtime(&self) -> &Arc<Runtime> {
        &self.runtime
    }

    /// Synchronously reads holding registers (FC 03) starting at `address`.
    pub fn read_holding_registers(&self, address: u16, quantity: u16) -> Result<RegisterData> {
        self.runtime
            .block_on(self.inner.read_holding_registers(address, quantity))
    }

    /// Synchronously reads input registers (FC 04) starting at `address`.
    pub fn read_input_registers(&self, address: u16, quantity: u16) -> Result<RegisterData> {
        self.runtime
            .block_on(self.inner.read_input_registers(address, quantity))
    }

    /// Synchronously reads coils (FC 01) starting at `address`.
    pub fn read_coils(&self, address: u16, quantity: u16) -> Result<Vec<bool>> {
        self.runtime
            .block_on(self.inner.read_coils(address, quantity))
    }

    /// Synchronously reads discrete inputs (FC 02) starting at `address`.
    pub fn read_discrete_inputs(&self, address: u16, quantity: u16) -> Result<Vec<bool>> {
        self.runtime
            .block_on(self.inner.read_discrete_inputs(address, quantity))
    }

    /// Synchronously writes a single 16-bit register (FC 06) at `address`.
    pub fn write_single_register(&self, address: u16, value: u16) -> Result<u16> {
        self.runtime
            .block_on(self.inner.write_single_register(address, value))
    }

    /// Synchronously writes a single coil (FC 05) at `address`.
    pub fn write_single_coil(&self, address: u16, value: bool) -> Result<bool> {
        self.runtime
            .block_on(self.inner.write_single_coil(address, value))
    }

    /// Synchronously writes multiple registers (FC 16 / 0x10) starting at `address`.
    pub fn write_multiple_registers(&self, address: u16, values: &[u16]) -> Result<u16> {
        self.runtime
            .block_on(self.inner.write_multiple_registers(address, values))
    }

    /// Synchronously writes multiple coils (FC 15 / 0x0F) starting at `address`.
    pub fn write_multiple_coils(&self, address: u16, values: Vec<bool>) -> Result<u16> {
        self.runtime
            .block_on(self.inner.write_multiple_coils(address, values))
    }

    /// Creates a synchronous [`PollingManager`] using the client's internal runtime.
    #[cfg(feature = "polling")]
    pub fn create_polling_manager(&self, capacity: usize) -> PollingManager {
        PollingManager {
            runtime: self.runtime.clone(),
            inner: AsyncPollingManager::new(capacity),
        }
    }

    /// Schedules recurring background polling of holding registers.
    #[cfg(feature = "polling")]
    pub fn poll_holding_registers(
        &self,
        poller: &PollingManager,
        id: impl Into<String>,
        address: u16,
        quantity: u16,
        interval: std::time::Duration,
    ) -> Result<()> {
        let inner = self.inner.clone();
        let task = AsyncPollingTask::new(id, interval, move || {
            let inner = inner.clone();
            async move {
                inner.read_holding_registers(address, quantity).await
            }
        });
        self.runtime.block_on(poller.inner.add_task(task))
    }

    /// Schedules recurring background polling of input registers.
    #[cfg(feature = "polling")]
    pub fn poll_input_registers(
        &self,
        poller: &PollingManager,
        id: impl Into<String>,
        address: u16,
        quantity: u16,
        interval: std::time::Duration,
    ) -> Result<()> {
        let inner = self.inner.clone();
        let task = AsyncPollingTask::new(id, interval, move || {
            let inner = inner.clone();
            async move {
                inner.read_input_registers(address, quantity).await
            }
        });
        self.runtime.block_on(poller.inner.add_task(task))
    }
}

/// Synchronous definition of a periodic polling task.
#[cfg(feature = "polling")]
#[derive(Clone)]
pub struct PollingTask {
    /// Unique identifier for this task.
    pub id: String,
    /// Execution interval duration.
    pub interval: std::time::Duration,
    /// Execution priority.
    pub priority: u32,
    /// Synchronous closure returning [`RegisterData`].
    pub action: Arc<dyn Fn() -> Result<RegisterData> + Send + Sync>,
    /// Callback invoked when data is successfully acquired.
    pub on_data: Option<Arc<dyn Fn(RegisterData) + Send + Sync>>,
    /// Callback invoked when an error occurs.
    pub on_error: Option<Arc<dyn Fn(&ModbusError) + Send + Sync>>,
}

#[cfg(feature = "polling")]
impl PollingTask {
    /// Creates a synchronous polling task from a closure returning `Result<RegisterData>`.
    pub fn new<F>(id: impl Into<String>, interval: std::time::Duration, action: F) -> Self
    where
        F: Fn() -> Result<RegisterData> + Send + Sync + 'static,
    {
        Self {
            id: id.into(),
            interval,
            priority: 10,
            action: Arc::new(action),
            on_data: None,
            on_error: None,
        }
    }

    /// Attaches a success callback invoked upon data reception.
    pub fn on_data<H>(mut self, handler: H) -> Self
    where
        H: Fn(RegisterData) + Send + Sync + 'static,
    {
        self.on_data = Some(Arc::new(handler));
        self
    }

    /// Attaches an error callback invoked upon communication failures.
    pub fn on_error<H>(mut self, handler: H) -> Self
    where
        H: Fn(&ModbusError) + Send + Sync + 'static,
    {
        self.on_error = Some(Arc::new(handler));
        self
    }

    /// Sets the task execution priority.
    pub fn priority(mut self, priority: u32) -> Self {
        self.priority = priority;
        self
    }
}

/// Synchronous facade for the [`PollingManager`](crate::polling::PollingManager).
#[cfg(feature = "polling")]
#[derive(Clone)]
pub struct PollingManager {
    runtime: Arc<Runtime>,
    inner: AsyncPollingManager,
}

#[cfg(feature = "polling")]
impl PollingManager {
    /// Creates a new synchronous polling manager.
    pub fn new(capacity: usize) -> Result<Self> {
        let runtime = Runtime::new().map_err(ModbusError::Io)?;
        Ok(Self {
            runtime: Arc::new(runtime),
            inner: AsyncPollingManager::new(capacity),
        })
    }

    /// Adds a synchronous [`PollingTask`] running periodically in the background.
    pub fn add_task(&self, task: PollingTask) -> Result<()> {
        let action = task.action;
        let on_data = task.on_data;
        let on_error = task.on_error;

        let mut async_task = AsyncPollingTask::new(task.id, task.interval, move || {
            let act = action.clone();
            async move {
                match tokio::task::spawn_blocking(move || (act)()).await {
                    Ok(res) => res,
                    Err(e) => Err(ModbusError::InvalidFrame(e.to_string())),
                }
            }
        });
        if let Some(cb) = on_data {
            async_task = async_task.on_data(move |d| cb(d));
        }
        if let Some(cb) = on_error {
            async_task = async_task.on_error(move |e| cb(e));
        }
        self.runtime.block_on(self.inner.add_task(async_task))
    }

    pub fn set_inter_task_delay(&self, delay: std::time::Duration) {
        self.runtime.block_on(self.inner.set_inter_task_delay(delay))
    }

    pub fn inter_task_delay(&self) -> std::time::Duration {
        self.runtime.block_on(self.inner.inter_task_delay())
    }

    /// Schedules recurring polling of holding registers using a client.
    pub fn poll_holding_registers(
        &self,
        client: &ModbusClient,
        id: impl Into<String>,
        address: u16,
        quantity: u16,
        interval: std::time::Duration,
    ) -> Result<()> {
        client.poll_holding_registers(self, id, address, quantity, interval)
    }

    /// Schedules recurring polling of input registers using a client.
    pub fn poll_input_registers(
        &self,
        client: &ModbusClient,
        id: impl Into<String>,
        address: u16,
        quantity: u16,
        interval: std::time::Duration,
    ) -> Result<()> {
        client.poll_input_registers(self, id, address, quantity, interval)
    }

    /// Subscribes to the broadcast event stream.
    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<PollingEvent> {
        self.inner.subscribe()
    }

    /// Blocks until the next [`PollingEvent`] is received from the subscription channel.
    pub fn recv(&self, rx: &mut tokio::sync::broadcast::Receiver<PollingEvent>) -> Result<PollingEvent> {
        self.runtime.block_on(async {
            rx.recv().await.map_err(|e| ModbusError::InvalidFrame(e.to_string()))
        })
    }
}

/// Synchronous facade for [`TransportController`](crate::transport::TransportController).
#[derive(Clone)]
pub struct TransportController {
    runtime: Arc<Runtime>,
    inner: AsyncTransportController,
}

impl TransportController {
    /// Creates a new synchronous [`TransportController`] with an isolated Tokio runtime.
    pub fn new() -> Result<Self> {
        let runtime = Runtime::new().map_err(ModbusError::Io)?;
        Ok(Self {
            runtime: Arc::new(runtime),
            inner: AsyncTransportController::new(),
        })
    }

    /// Creates a controller configured with a global traffic sniffer.
    pub fn with_sniffer(sniffer_capacity: usize) -> Result<Self> {
        let runtime = Runtime::new().map_err(ModbusError::Io)?;
        Ok(Self {
            runtime: Arc::new(runtime),
            inner: AsyncTransportController::with_sniffer(sniffer_capacity),
        })
    }

    /// Returns a clone of the global traffic sniffer, if configured.
    pub fn sniffer(&self) -> Option<crate::trackers::sniffer::TrafficSniffer> {
        self.runtime.block_on(self.inner.sniffer())
    }

    /// Registers a custom [`Transport`] implementation under the given ID.
    pub fn add_transport<T: Transport + 'static>(
        &self,
        id: impl Into<String>,
        framing: FramingKind,
        transport: T,
        queue_capacity: usize,
    ) -> Result<()> {
        self.runtime.block_on(self.inner.add_transport(id, framing, transport, queue_capacity))
    }

    /// Opens and registers an RTU serial line with standard 8N1 settings.
    ///
    /// Requires the `rtu` crate feature.
    #[cfg(feature = "rtu")]
    pub fn add_rtu_transport(
        &self,
        id: impl Into<String>,
        path: impl Into<String>,
        baud_rate: u32,
    ) -> Result<()> {
        let config = RtuConfig::new(path, baud_rate);
        self.runtime.block_on(self.inner.add_rtu_transport(id, config))
    }

    /// Ergonomic shortcut to open an RTU serial transport by path directly.
    ///
    /// Returns the registered transport ID (e.g. `"/dev/ttyUSB0"`).
    /// Requires the `rtu` crate feature.
    #[cfg(feature = "rtu")]
    pub fn add_rtu(
        &self,
        path: impl Into<String>,
        baud_rate: u32,
    ) -> Result<String> {
        let path = path.into();
        let id = path.clone();
        self.add_rtu_transport(&id, &path, baud_rate)?;
        Ok(id)
    }

    /// Connects and registers a Modbus TCP transport with an explicit ID.
    ///
    /// Requires the `tcp` crate feature.
    #[cfg(feature = "tcp")]
    pub fn add_tcp_transport(
        &self,
        id: impl Into<String>,
        addr: impl Into<String>,
    ) -> Result<()> {
        self.runtime.block_on(self.inner.add_tcp_transport(id, addr))
    }

    /// Ergonomic shortcut to connect a Modbus TCP transport directly via address string.
    ///
    /// Returns the registered transport ID (e.g. `"192.168.1.100:502"`).
    /// Requires the `tcp` crate feature.
    #[cfg(feature = "tcp")]
    pub fn add_tcp(
        &self,
        addr: impl Into<String>,
    ) -> Result<String> {
        let addr = addr.into();
        let id = addr.clone();
        self.add_tcp_transport(&id, &addr)?;
        Ok(id)
    }

    /// Creates a synchronous [`ModbusClient`] bound to a registered transport and `slave_id`.
    pub fn create_client(
        &self,
        transport_id: &str,
        slave_id: u8,
    ) -> Result<ModbusClient> {
        self.create_client_with_options(transport_id, slave_id, ClientOptions::default())
    }

    /// Creates a synchronous [`ModbusClient`] with custom retry/timeout options.
    pub fn create_client_with_options(
        &self,
        transport_id: &str,
        slave_id: u8,
        options: ClientOptions,
    ) -> Result<ModbusClient> {
        let async_client = self
            .runtime
            .block_on(self.inner.create_client(transport_id, slave_id, options))?;
        Ok(ModbusClient {
            runtime: self.runtime.clone(),
            inner: async_client,
        })
    }

    /// Creates a managed [`ModbusClient`] configured through [`CreateClientOptions`].
    pub fn create_managed_client(&self, opts: CreateClientOptions) -> Result<ModbusClient> {
        let async_client = self.runtime.block_on(self.inner.create_managed_client(opts))?;
        Ok(ModbusClient {
            runtime: self.runtime.clone(),
            inner: async_client,
        })
    }

    /// Sets the physical RS mode (e.g. `RsMode::Rs232`) on an existing transport session.
    pub fn set_rs_mode(&self, transport_id: &str, mode: RsMode) -> Result<()> {
        self.runtime.block_on(self.inner.set_rs_mode(transport_id, mode))
    }

    /// Retrieves a managed client by its unique client ID.
    pub fn get_client(&self, client_id: &str) -> Option<ModbusClient> {
        self.runtime
            .block_on(self.inner.get_client(client_id))
            .map(|inner| ModbusClient {
                runtime: self.runtime.clone(),
                inner,
            })
    }

    /// Lists all registered clients, optionally filtered by `transport_id`.
    pub fn list_clients(&self, transport_id: Option<&str>) -> Vec<RegisteredClient> {
        self.runtime.block_on(self.inner.list_clients(transport_id))
    }

    /// Removes a client from the registry by its client ID.
    pub fn remove_client(&self, client_id: &str) -> Result<()> {
        self.runtime.block_on(self.inner.remove_client(client_id))
    }

    /// Reassigns an existing client to a new slave ID.
    pub fn reassign_client(
        &self,
        client_id: &str,
        new_slave_id: u8,
        allow_duplicate: bool,
    ) -> Result<()> {
        self.runtime.block_on(self.inner.reassign_client(client_id, new_slave_id, allow_duplicate))
    }

    /// Directly transmits raw bytes to a transport's port queue with immediate priority.
    pub fn write_to_port(
        &self,
        transport_id: &str,
        raw_frame: Vec<u8>,
        timeout: Duration,
    ) -> Result<Vec<u8>> {
        self.runtime.block_on(self.inner.write_to_port(transport_id, raw_frame, timeout))
    }

    /// Returns a diagnostic status snapshot for a given transport.
    pub fn get_status(&self, transport_id: &str) -> Result<TransportStatus> {
        self.runtime.block_on(self.inner.get_status(transport_id))
    }

    /// Returns the number of currently active / open transports.
    pub fn get_active_transport_count(&self) -> usize {
        self.runtime.block_on(self.inner.get_active_transport_count())
    }

    /// Adds a synchronous [`PollingTask`] with callbacks to the specified transport session.
    #[cfg(feature = "polling")]
    pub fn add_polling_task(
        &self,
        transport_id: &str,
        task: PollingTask,
    ) -> Result<()> {
        let action = task.action;
        let on_data = task.on_data;
        let on_error = task.on_error;

        let mut async_task = AsyncPollingTask::new(task.id, task.interval, move || {
            let act = action.clone();
            async move {
                match tokio::task::spawn_blocking(move || (act)()).await {
                    Ok(res) => res,
                    Err(e) => Err(ModbusError::InvalidFrame(e.to_string())),
                }
            }
        });
        if let Some(cb) = on_data {
            async_task = async_task.on_data(move |d| cb(d));
        }
        if let Some(cb) = on_error {
            async_task = async_task.on_error(move |e| cb(e));
        }
        self.runtime.block_on(self.inner.add_polling_task(transport_id, async_task))
    }

    /// Schedules recurring polling of holding registers on the specified transport session.
    #[cfg(feature = "polling")]
    pub fn poll_holding_registers(
        &self,
        transport_id: &str,
        client: &ModbusClient,
        task_id: impl Into<String>,
        address: u16,
        quantity: u16,
        interval: std::time::Duration,
    ) -> Result<()> {
        let client_inner = client.inner.clone();
        let task = AsyncPollingTask::new(task_id, interval, move || {
            let c = client_inner.clone();
            async move { c.read_holding_registers(address, quantity).await }
        });
        self.runtime.block_on(self.inner.add_polling_task(transport_id, task))
    }

    /// Schedules recurring polling of input registers on the specified transport session.
    #[cfg(feature = "polling")]
    pub fn poll_input_registers(
        &self,
        transport_id: &str,
        client: &ModbusClient,
        task_id: impl Into<String>,
        address: u16,
        quantity: u16,
        interval: std::time::Duration,
    ) -> Result<()> {
        let client_inner = client.inner.clone();
        let task = AsyncPollingTask::new(task_id, interval, move || {
            let c = client_inner.clone();
            async move { c.read_input_registers(address, quantity).await }
        });
        self.runtime.block_on(self.inner.add_polling_task(transport_id, task))
    }

    /// Subscribes to polling events on the specified transport session.
    #[cfg(feature = "polling")]
    pub fn subscribe_polling(
        &self,
        transport_id: &str,
    ) -> Result<tokio::sync::broadcast::Receiver<PollingEvent>> {
        self.runtime.block_on(self.inner.subscribe_polling(transport_id))
    }

    /// Blocks until the next polling event is received from the channel.
    #[cfg(feature = "polling")]
    pub fn recv_polling_event(
        &self,
        rx: &mut tokio::sync::broadcast::Receiver<PollingEvent>,
    ) -> Result<PollingEvent> {
        self.runtime.block_on(async {
            rx.recv().await.map_err(|e| ModbusError::InvalidFrame(e.to_string()))
        })
    }

    /// Pauses an active polling task on a transport session.
    #[cfg(feature = "polling")]
    pub fn pause_polling(&self, transport_id: &str, task_id: &str) -> Result<()> {
        self.runtime.block_on(self.inner.pause_polling(transport_id, task_id))
    }

    /// Resumes a paused polling task on a transport session.
    #[cfg(feature = "polling")]
    pub fn resume_polling(&self, transport_id: &str, task_id: &str) -> Result<()> {
        self.runtime.block_on(self.inner.resume_polling(transport_id, task_id))
    }

    /// Pauses all polling tasks on the specified transport session.
    #[cfg(feature = "polling")]
    pub fn pause_all_polling(&self, transport_id: &str) -> Result<()> {
        self.runtime.block_on(self.inner.pause_all_polling(transport_id))
    }

    /// Resumes all paused polling tasks on the specified transport session.
    #[cfg(feature = "polling")]
    pub fn resume_all_polling(&self, transport_id: &str) -> Result<()> {
        self.runtime.block_on(self.inner.resume_all_polling(transport_id))
    }

    /// Pauses transmission queue processing on the specified transport.
    pub fn pause_transport(&self, id: &str) -> Result<()> {
        self.runtime.block_on(self.inner.pause_transport(id))
    }

    /// Resumes transmission queue processing on the specified transport.
    pub fn resume_transport(&self, id: &str) -> Result<()> {
        self.runtime.block_on(self.inner.resume_transport(id))
    }

    /// Closes and removes a registered transport session.
    pub fn remove_transport(&self, id: &str) -> Result<()> {
        self.runtime.block_on(self.inner.remove_transport(id))
    }

    /// Lists all registered transport IDs.
    pub fn list_transports(&self) -> Vec<String> {
        self.runtime.block_on(self.inner.list_transports())
    }
}

/// Synchronous facade for high-level schema field access.
#[cfg(feature = "schema")]
#[derive(Clone)]
pub struct BoundSchema {
    runtime: Arc<Runtime>,
    inner: AsyncBoundSchema,
}

#[cfg(feature = "schema")]
impl BoundSchema {
    /// Synchronously reads a schema field value by its name.
    pub fn read_field(&self, name: &str) -> Result<FieldValue> {
        self.runtime.block_on(self.inner.read_field(name))
    }

    /// Synchronously reads all fields in a named group.
    pub fn read_group(&self, name: &str) -> Result<HashMap<String, FieldValue>> {
        self.runtime.block_on(self.inner.read_group(name))
    }

    /// Synchronously writes a value to a schema field by its name.
    pub fn write_field(&self, name: &str, value: FieldValue) -> Result<()> {
        self.runtime.block_on(self.inner.write_field(name, value))
    }

    /// Synchronously writes multiple fields in a named contiguous group.
    pub fn write_group(&self, name: &str, values: &HashMap<String, FieldValue>) -> Result<()> {
        self.runtime.block_on(self.inner.write_group(name, values))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::emulator::SlaveEmulator;

    #[test]
    fn test_blocking_client_with_emulator() {
        let emulator = SlaveEmulator::new(1, FramingKind::Rtu);

        let client = ModbusClient::from_transport(
            1,
            FramingKind::Rtu,
            emulator.clone(),
            ClientOptions::default(),
        )
        .unwrap();

        client.runtime().block_on(async {
            emulator.set_holding_register(10, 777).await;
        });

        let data = client.read_holding_registers(10, 1).unwrap();
        assert_eq!(data.get_u16(0).unwrap(), 777);

        client.write_single_register(10, 888).unwrap();

        let val = client.runtime().block_on(emulator.get_holding_register(10));
        assert_eq!(val, Some(888));
    }

    #[test]
    fn test_blocking_polling_manager() {
        let emulator = SlaveEmulator::new(1, FramingKind::Rtu);
        let client = ModbusClient::from_transport(
            1,
            FramingKind::Rtu,
            emulator.clone(),
            ClientOptions::default(),
        )
        .unwrap();

        client.runtime().block_on(async {
            emulator.set_holding_register(0, 111).await;
            emulator.set_holding_register(1, 222).await;
        });

        let poller = client.create_polling_manager(10);
        let mut rx = poller.subscribe();

        client
            .poll_holding_registers(&poller, "t1", 0, 2, std::time::Duration::from_millis(10))
            .unwrap();

        let event = poller.recv(&mut rx).unwrap();
        match event {
            PollingEvent::Data { task_id, data } => {
                assert_eq!(task_id, "t1");
                assert_eq!(data.get_u16(0).unwrap(), 111);
                assert_eq!(data.get_u16(1).unwrap(), 222);
            }
            _ => panic!("Expected data event"),
        }
    }

    #[test]
    fn test_blocking_transport_controller() {
        let emulator = SlaveEmulator::new(1, FramingKind::Rtu);
        let controller = TransportController::new().unwrap();
        controller
            .add_transport("LINE1", FramingKind::Rtu, emulator.clone(), 10)
            .unwrap();

        let client = controller.create_client("LINE1", 1).unwrap();

        client.runtime().block_on(async {
            emulator.set_holding_register(0, 333).await;
            emulator.set_holding_register(1, 444).await;
        });

        let mut rx = controller.subscribe_polling("LINE1").unwrap();
        controller
            .poll_holding_registers("LINE1", &client, "task_ctrl", 0, 2, std::time::Duration::from_millis(10))
            .unwrap();

        let event = controller.recv_polling_event(&mut rx).unwrap();
        match event {
            PollingEvent::Data { task_id, data } => {
                assert_eq!(task_id, "task_ctrl");
                assert_eq!(data.get_u16(0).unwrap(), 333);
                assert_eq!(data.get_u16(1).unwrap(), 444);
            }
            _ => panic!("Expected data event"),
        }
    }

    #[test]
    fn test_blocking_polling_task_callbacks() {
        let emulator = SlaveEmulator::new(1, FramingKind::Rtu);
        let controller = TransportController::new().unwrap();
        controller
            .add_transport("LINE2", FramingKind::Rtu, emulator.clone(), 10)
            .unwrap();

        let client = controller.create_client("LINE2", 1).unwrap();

        client.runtime().block_on(async {
            emulator.set_holding_register(0, 555).await;
            emulator.set_holding_register(1, 666).await;
        });

        let received = Arc::new(std::sync::Mutex::new(None));
        let received_clone = received.clone();

        let task = PollingTask::new("task_cb", std::time::Duration::from_millis(10), {
            let client = client.clone();
            move || client.read_holding_registers(0, 2)
        })
        .on_data(move |data| {
            let mut val = received_clone.lock().unwrap();
            *val = Some(data);
        });

        controller.add_polling_task("LINE2", task).unwrap();

        for _ in 0..50 {
            if received.lock().unwrap().is_some() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        let result = received.lock().unwrap().take().expect("Callback should have received data");
        assert_eq!(result.get_u16(0).unwrap(), 555);
        assert_eq!(result.get_u16(1).unwrap(), 666);
    }

    #[test]
    fn test_blocking_managed_client_and_registry() {
        let emulator = SlaveEmulator::new(1, FramingKind::Rtu);
        let controller = TransportController::new().unwrap();
        controller
            .add_transport("LINE3", FramingKind::Rtu, emulator.clone(), 10)
            .unwrap();

        controller.set_rs_mode("LINE3", RsMode::Rs232).unwrap();

        let client = controller
            .create_managed_client(
                CreateClientOptions::new(1)
                    .transport_id("LINE3")
                    .client_id("c1"),
            )
            .unwrap();
        assert_eq!(client.slave_id(), 1);

        let status = controller.get_status("LINE3").unwrap();
        assert_eq!(status.clients_count, 1);
        assert_eq!(status.rs_mode, RsMode::Rs232);

        let clients = controller.list_clients(Some("LINE3"));
        assert_eq!(clients.len(), 1);
        assert_eq!(clients[0].client_id, "c1");

        controller.remove_client("c1").unwrap();
        assert!(controller.get_client("c1").is_none());
    }
}
