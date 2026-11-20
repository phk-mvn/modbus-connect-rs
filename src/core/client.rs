//! High-level Modbus client with automatic retry logic, timeouts, and prioritized queueing.
//!
//! [`ModbusClient`] interacts with a specific Modbus slave/unit ID over an asynchronous
//! transport pipeline. It abstracts away frame serialisation and parsing, providing
//! dedicated methods for reading and writing coils, discrete inputs, holding registers,
//! and input registers.
//!
//! # Architecture & Retries
//!
//! When executing requests:
//! 1. The client formats the PDU into an RTU or TCP Application Data Unit (ADU).
//! 2. It submits the frame to a [`PortQueueHandle`].
//! 3. If a communication failure occurs (such as a timeout, framing error, or CRC mismatch),
//!    the request is retried according to [`ClientOptions`].
//! 4. If the slave responds with a standard Modbus exception (e.g., `IllegalDataAddress`),
//!    the error is returned immediately without burning further retries.

use std::sync::atomic::{AtomicU16, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[cfg(feature = "plugin")]
use std::collections::HashMap;
#[cfg(feature = "plugin")]
use std::sync::RwLock;
#[cfg(feature = "plugin")]
use crate::plugin::{CustomFunctionHandler, ModbusPlugin};

use crate::core::framing::{FramingKind, RtuFramer, TcpFramer};
use crate::core::pdu::{Request, Response};
use crate::core::register_data::RegisterData;
use crate::error::{ModbusError, Result};
use crate::trackers::device_tracker::DeviceConnectionTracker;
use crate::trackers::types::{ConnectionErrorInfo, ConnectionErrorType};
use crate::transport::queue::{PortQueueHandle, Priority};

/// Configuration options for [`ModbusClient`] request timeouts and retry policy.
///
/// # Example
///
/// ```
/// use std::time::Duration;
/// use modbus_connect::core::ClientOptions;
///
/// let options = ClientOptions {
///     timeout: Duration::from_millis(500),
///     retry_count: 3,
///     retry_delay: Duration::from_millis(50),
///     total_timeout: Some(Duration::from_millis(3000)),
/// };
/// assert_eq!(options.retry_count, 3);
/// ```
#[derive(Debug, Clone)]
pub struct ClientOptions {
    /// Timeout duration for each individual request attempt.
    pub timeout: Duration,
    /// Number of retry attempts when communication fails before giving up.
    pub retry_count: u32,
    /// Delay pause between consecutive retry attempts.
    pub retry_delay: Duration,
    /// Optional total cumulative timeout limit across all retries for a single operation.
    pub total_timeout: Option<Duration>,
}

impl Default for ClientOptions {
    /// Provides standard default options:
    /// - `timeout`: 1000 ms
    /// - `retry_count`: 2 attempts
    /// - `retry_delay`: 100 ms
    /// - `total_timeout`: 5000 ms
    fn default() -> Self {
        Self {
            timeout: Duration::from_millis(1000),
            retry_count: 2,
            retry_delay: Duration::from_millis(100),
            total_timeout: Some(Duration::from_millis(5000)),
        }
    }
}

/// Asynchronous Modbus client targeting a specific unit/slave ID.
///
/// Can be cloned cheaply because internal handles and transaction counters
/// are wrapped in [`Arc`].
///
/// # Example
///
/// ```no_run
/// use modbus_connect::core::{ModbusClient, ClientOptions};
/// use modbus_connect::core::framing::FramingKind;
/// use modbus_connect::transport::queue::PortQueueHandle;
/// # async fn doc() -> Result<(), Box<dyn std::error::Error>> {
/// # let mock_transport = modbus_connect::transport::emulator::SlaveEmulator::new(1, FramingKind::Rtu);
/// # let queue = PortQueueHandle::new(mock_transport, 10);
/// let client = ModbusClient::new(1, FramingKind::Rtu, queue, ClientOptions::default())?;
///
/// // Read holding registers 0..2
/// let registers = client.read_holding_registers(0, 2).await?;
/// println!("Register 0: {}", registers[0]);
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct ModbusClient {
    slave_id: Arc<AtomicU8>,
    framing: FramingKind,
    queue: PortQueueHandle,
    options: ClientOptions,
    next_tx_id: Arc<AtomicU16>,
    device_tracker: DeviceConnectionTracker,
    #[cfg(feature = "plugin")]
    custom_functions: Arc<RwLock<HashMap<String, Arc<dyn CustomFunctionHandler>>>>,
}

impl std::fmt::Debug for ModbusClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ModbusClient")
            .field("slave_id", &self.slave_id())
            .field("framing", &self.framing)
            .field("options", &self.options)
            .finish()
    }
}

impl ModbusClient {
    /// Creates a new [`ModbusClient`] bound to a given `slave_id` and framing mode.
    ///
    /// # Errors
    ///
    /// Returns [`ModbusError::InvalidSlaveId`] if `slave_id` is 0 (broadcast) or greater than 247.
    pub fn new(
        slave_id: u8,
        framing: FramingKind,
        queue: PortQueueHandle,
        options: ClientOptions,
    ) -> Result<Self> {
        if slave_id == 0 || slave_id > 247 {
            return Err(ModbusError::InvalidSlaveId(slave_id));
        }

        Ok(Self {
            slave_id: Arc::new(AtomicU8::new(slave_id)),
            framing,
            queue,
            options,
            next_tx_id: Arc::new(AtomicU16::new(1)),
            device_tracker: DeviceConnectionTracker::default(),
            #[cfg(feature = "plugin")]
            custom_functions: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Returns the target Modbus slave/unit ID.
    pub fn slave_id(&self) -> u8 {
        self.slave_id.load(Ordering::SeqCst)
    }

    /// Dynamically updates the Modbus slave address for subsequent requests.
    ///
    /// # Errors
    ///
    /// Returns [`ModbusError::InvalidSlaveId`] if `new_slave_id` is 0 or greater than 247.
    pub fn set_slave_id(&mut self, new_slave_id: u8) -> Result<()> {
        if new_slave_id == 0 || new_slave_id > 247 {
            return Err(ModbusError::InvalidSlaveId(new_slave_id));
        }
        self.slave_id.store(new_slave_id, Ordering::SeqCst);
        Ok(())
    }

    /// Dynamically updates the Modbus slave address when sharing across threads (`&self`).
    pub fn set_slave_id_shared(&self, new_slave_id: u8) -> Result<()> {
        if new_slave_id == 0 || new_slave_id > 247 {
            return Err(ModbusError::InvalidSlaveId(new_slave_id));
        }
        self.slave_id.store(new_slave_id, Ordering::SeqCst);
        Ok(())
    }

    /// Returns a reference to the client's internal [`DeviceConnectionTracker`].
    pub fn device_tracker(&self) -> &DeviceConnectionTracker {
        &self.device_tracker
    }

    /// Attaches an isolated device state callback invoked when connection to this slave is lost or restored.
    pub async fn set_device_state_handler<F>(&self, handler: F)
    where
        F: Fn(u8, bool, Option<&ConnectionErrorInfo>) + Send + Sync + 'static,
    {
        self.device_tracker.set_handler(handler).await;
    }

    /// Creates a client from an arbitrary underlying [`Transport`](crate::transport::traits::Transport).
    ///
    /// A [`PortQueueHandle`] with a default channel capacity of 100 will be automatically spawned.
    pub fn from_transport<T: crate::transport::traits::Transport + 'static>(
        slave_id: u8,
        framing: FramingKind,
        transport: T,
        options: ClientOptions,
    ) -> Result<Self> {
        let queue = PortQueueHandle::new(transport, 100);
        Self::new(slave_id, framing, queue, options)
    }

    /// Creates a client from an underlying transport with an optional traffic sniffer attached.
    pub fn with_sniffer<T: crate::transport::traits::Transport + 'static>(
        slave_id: u8,
        framing: FramingKind,
        transport: T,
        sniffer: Option<crate::trackers::sniffer::TrafficSniffer>,
        options: ClientOptions,
    ) -> Result<Self> {
        let queue = PortQueueHandle::with_sniffer(transport, 100, sniffer);
        Self::new(slave_id, framing, queue, options)
    }

    /// Opens an RTU serial transport and creates a client for it.
    ///
    /// Requires the `rtu` crate feature.
    #[cfg(feature = "rtu")]
    pub fn rtu(
        config: crate::transport::rtu::RtuConfig,
        slave_id: u8,
        options: ClientOptions,
    ) -> Result<Self> {
        let transport = crate::transport::rtu::RtuTransport::open(config)?;
        Self::from_transport(slave_id, FramingKind::Rtu, transport, options)
    }

    /// Opens an RTU serial transport with a traffic sniffer and creates a client for it.
    ///
    /// Requires the `rtu` crate feature.
    #[cfg(feature = "rtu")]
    pub fn rtu_with_sniffer(
        config: crate::transport::rtu::RtuConfig,
        slave_id: u8,
        sniffer: Option<crate::trackers::sniffer::TrafficSniffer>,
        options: ClientOptions,
    ) -> Result<Self> {
        let transport = crate::transport::rtu::RtuTransport::open(config)?;
        Self::with_sniffer(slave_id, FramingKind::Rtu, transport, sniffer, options)
    }

    /// Connects to a TCP Modbus server and creates a client for it.
    ///
    /// Requires the `tcp` crate feature.
    #[cfg(feature = "tcp")]
    pub async fn tcp(
        addr: impl Into<String>,
        slave_id: u8,
        options: ClientOptions,
    ) -> Result<Self> {
        let transport = crate::transport::tcp::TcpTransport::connect(addr).await?;
        Self::from_transport(slave_id, FramingKind::Tcp, transport, options)
    }

    /// Connects to a TCP Modbus server with a traffic sniffer and creates a client for it.
    ///
    /// Requires the `tcp` crate feature.
    #[cfg(feature = "tcp")]
    pub async fn tcp_with_sniffer(
        addr: impl Into<String>,
        slave_id: u8,
        sniffer: Option<crate::trackers::sniffer::TrafficSniffer>,
        options: ClientOptions,
    ) -> Result<Self> {
        let transport = crate::transport::tcp::TcpTransport::connect(addr).await?;
        Self::with_sniffer(slave_id, FramingKind::Tcp, transport, sniffer, options)
    }

    // ==================== Base Request Loop with Retries ====================

    /// Executes an arbitrary Modbus [`Request`] with the specified [`Priority`].
    ///
    /// This method manages frame assembly, dispatching to the port queue, timeout enforcement,
    /// response parsing, and retry handling.
    ///
    /// If an exception response is returned by the slave, execution halts immediately
    /// with [`ModbusError::Exception`] (exceptions are valid protocol responses and are not retried).
    pub async fn execute(&self, request: Request, priority: Priority) -> Result<Response> {
        let current_slave = self.slave_id();
        let start_time = Instant::now();
        let mut attempts = 0;

        loop {
            attempts += 1;

            if let Some(total) = self.options.total_timeout {
                if start_time.elapsed() >= total {
                    let err = ModbusError::Timeout(total);
                    self.device_tracker.notify_disconnected(current_slave, ConnectionErrorType::Timeout, err.to_string()).await;
                    return Err(err);
                }
            }

            let tx_id = self.next_tx_id.fetch_add(1, Ordering::SeqCst);
            let frame = match self.framing {
                FramingKind::Rtu => RtuFramer::build_frame(current_slave, &request),
                FramingKind::Tcp => TcpFramer::build_frame(tx_id, current_slave, &request),
            };

            let exchange_res = self
                .queue
                .send_frame(frame, priority, self.options.timeout)
                .await;

            match exchange_res {
                Ok(raw_response) => {
                    let parse_res = match self.framing {
                        FramingKind::Rtu => {
                            RtuFramer::parse_response(current_slave, &request, &raw_response)
                        }
                        FramingKind::Tcp => {
                            TcpFramer::parse_response(tx_id, current_slave, &request, &raw_response)
                        }
                    };

                    match parse_res {
                        Ok(response) => {
                            self.device_tracker.notify_connected(current_slave).await;
                            return Ok(response);
                        }
                        Err(ModbusError::Exception(exc)) => {
                            self.device_tracker.notify_connected(current_slave).await;
                            return Err(ModbusError::Exception(exc));
                        }
                        Err(err) => {
                            if attempts > self.options.retry_count {
                                let err_type = match &err {
                                    ModbusError::Timeout(_) => ConnectionErrorType::Timeout,
                                    ModbusError::InvalidFrame(m) if m.contains("CRC") => ConnectionErrorType::CrcError,
                                    _ => ConnectionErrorType::UnknownError,
                                };
                                self.device_tracker.notify_disconnected(current_slave, err_type, err.to_string()).await;
                                return Err(err);
                            }
                        }
                    }
                }

                Err(ModbusError::ScanActive) => return Err(ModbusError::ScanActive),

                Err(err) => {
                    if attempts > self.options.retry_count {
                        let err_type = match &err {
                            ModbusError::Timeout(_) => ConnectionErrorType::Timeout,
                            ModbusError::InvalidFrame(m) if m.contains("CRC") => ConnectionErrorType::CrcError,
                            _ => ConnectionErrorType::UnknownError,
                        };
                        self.device_tracker.notify_disconnected(current_slave, err_type, err.to_string()).await;
                        return Err(err);
                    }
                }
            }

            tokio::time::sleep(self.options.retry_delay).await;
        }
    }

    // ==================== Raw PDU & Custom Function Execution =====================

    /// Transmits an arbitrary raw Modbus PDU and returns the response PDU bytes.
    /// 
    /// Handles framing (RTU CRC / TCP MBAP), slave ID addressing, retries, and exceptions.
    pub async fn raw_exchange(&self, pdu: &[u8], priority: Priority) -> Result<Vec<u8>> {
        self.raw_exchange_opt(pdu, priority, false).await
    }

    /// Transmits an arbitrary raw Modbus PDU with fire-and-forget option (`ignore_no_response`).
    ///
    /// If `ignore_no_response` is true, only writes to transport and resolves immediately with an empty vector.
    pub async fn raw_exchange_opt(
        &self,
        pdu: &[u8],
        priority: Priority,
        ignore_no_response: bool,
    ) -> Result<Vec<u8>> {
        if pdu.is_empty() {
            return Err(ModbusError::BufferUnderrun { expected: 1, actual: 0 });
        }
        let current_slave = self.slave_id();
        let start_time = Instant::now();
        let mut attempts = 0;

        loop {
            attempts += 1;
            if let Some(total) = self.options.total_timeout {
                if start_time.elapsed() >= total {
                    let err = ModbusError::Timeout(total);
                    self.device_tracker.notify_disconnected(current_slave, ConnectionErrorType::Timeout, err.to_string()).await;
                    return Err(err);
                }
            }

            let tx_id = self.next_tx_id.fetch_add(1, Ordering::SeqCst);
            let frame = match self.framing {
                FramingKind::Rtu => {
                    let mut f = Vec::with_capacity(1 + pdu.len() + 2);
                    f.push(current_slave);
                    f.extend_from_slice(pdu);
                    let crc = crate::core::crc::crc16(&f);
                    f.push((crc & 0xFF) as u8);
                    f.push((crc >> 8) as u8);
                    f
                }
                FramingKind::Tcp => {
                    let mut f = Vec::with_capacity(7 + pdu.len());
                    f.extend_from_slice(&tx_id.to_be_bytes());
                    f.extend_from_slice(&0u16.to_be_bytes());
                    let len = (1 + pdu.len()) as u16;
                    f.extend_from_slice(&len.to_be_bytes());
                    f.push(current_slave);
                    f.extend_from_slice(pdu);
                    f
                }
            };

            if ignore_no_response {
                self.queue.send_frame_opt(frame, priority, self.options.timeout, true).await?;
                self.device_tracker.notify_connected(current_slave).await;
                return Ok(Vec::new());
            }

            let exchange_res = self.queue.send_frame(frame, priority, self.options.timeout).await;

            match exchange_res {
                Ok(raw_response) => {
                    let pdu_res = match self.framing {
                        FramingKind::Rtu => {
                            if raw_response.len() < 4 {
                                Err(ModbusError::BufferUnderrun { expected: 4, actual: raw_response.len() })
                            } else if !crate::core::crc::check_crc(&raw_response) {
                                Err(ModbusError::InvalidFrame("CRC mismatch in RTU response".into()))
                            } else if raw_response[0] != current_slave {
                                Err(ModbusError::InvalidFrame(format!("Slave ID mismatch: expected {}, got {}", current_slave, raw_response[0])))
                            } else {
                                let resp_fc = raw_response[1];
                                if (resp_fc & 0x80) != 0 {
                                    let exc_code = crate::error::ExceptionCode::from(raw_response[2]);
                                    Err(ModbusError::Exception(exc_code))
                                } else {
                                    Ok(raw_response[1..raw_response.len() - 2].to_vec())
                                }
                            }
                        }
                        FramingKind::Tcp => {
                            if raw_response.len() < 8 {
                                Err(ModbusError::BufferUnderrun { expected: 8, actual: raw_response.len() })
                            } else {
                                let resp_tx_id = u16::from_be_bytes([raw_response[0], raw_response[1]]);
                                let resp_unit = raw_response[6];
                                if resp_tx_id != tx_id {
                                    Err(ModbusError::InvalidFrame(format!("Transaction ID mismatch: expected {}, got {}", tx_id, resp_tx_id)))
                                } else if resp_unit != current_slave {
                                    Err(ModbusError::InvalidFrame(format!("Unit ID mismatch: expected {}, got {}", current_slave, resp_unit)))
                                } else {
                                    let resp_fc = raw_response[7];
                                    if (resp_fc & 0x80) != 0 {
                                        let exc_code = crate::error::ExceptionCode::from(raw_response[8]);
                                        Err(ModbusError::Exception(exc_code))
                                    } else {
                                        Ok(raw_response[7..].to_vec())
                                    }
                                }
                            }
                        }
                    };

                    match pdu_res {
                        Ok(resp_pdu) => {
                            self.device_tracker.notify_connected(current_slave).await;
                            return Ok(resp_pdu);
                        }
                        Err(ModbusError::Exception(exc)) => {
                            self.device_tracker.notify_connected(current_slave).await;
                            return Err(ModbusError::Exception(exc));
                        }
                        Err(err) => {
                            if attempts > self.options.retry_count {
                                let err_type = match &err {
                                    ModbusError::Timeout(_) => ConnectionErrorType::Timeout,
                                    ModbusError::InvalidFrame(m) if m.contains("CRC") => ConnectionErrorType::CrcError,
                                    _ => ConnectionErrorType::UnknownError,
                                };
                                self.device_tracker.notify_disconnected(current_slave, err_type, err.to_string()).await;
                                return Err(err);
                            }
                        }
                    }
                }

                Err(ModbusError::ScanActive) => return Err(ModbusError::ScanActive),

                Err(err) => {
                    if attempts > self.options.retry_count {
                        let err_type = match &err {
                            ModbusError::Timeout(_) => ConnectionErrorType::Timeout,
                            ModbusError::InvalidFrame(m) if m.contains("CRC") => ConnectionErrorType::CrcError,
                            _ => ConnectionErrorType::UnknownError,
                        };
                        self.device_tracker.notify_disconnected(current_slave, err_type, err.to_string()).await;
                        return Err(err);
                    }
                }
            }

            tokio::time::sleep(self.options.retry_delay).await;
        }
    }

    /// Registers a custom plugin providing proprietary function codes.
    #[cfg(feature = "plugin")]
    pub fn use_plugin<P: ModbusPlugin>(&self, plugin: P) {
        if let Ok(mut map) = self.custom_functions.write() {
            for (name, handler) in plugin.custom_functions() {
                map.insert(name, handler);
            }
        }
    }

    /// Fluent builder method to attach a plugin.
    #[cfg(feature = "plugin")]
    pub fn with_plugin<P: ModbusPlugin>(self, plugin: P) -> self {
        self.use_plugin(plugin);
        self
    }

    /// Executes a custom function code registered via a plugin.
    #[cfg(feature = "plugin")]
    pub async fn execute_custom_function(&self, function_name: &str, args: &[u8]) -> Result<Vec<u8>> {
        let handler = {
            let map = self.custom_functions.read().map_err(|_| {
                ModbusError::InvalidFrame("Plugin registry lock poisoned".into())
            })?;
            map.get(function_name).cloned().ok_or_else(|| {
                ModbusError::InvalidFrame(format!("Custom function '{}' is not registered. Register the plugin first using use_plugin().", function_name))
            })?
        };

        let req_pdu = handler.build_request(args);
        let resp_pdu = self.raw_exchange(&req_pdu, Priority::Normal).await?;
        handler.parse_response(&resp_pdu)
    }

    // ==================== High-Level Modbus Read/Write Methods ====================

    /// Reads consecutive holding registers (FC 03) starting at `address`.
    ///
    /// Returns a [`RegisterData`] wrapper allowing both 0-indexed slicing and multi-register
    /// scalar decoding (u16, u32, f32, f64, strings).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use modbus_connect::core::register_data::WordOrder;
    /// # async fn doc(client: modbus_connect::core::ModbusClient) -> Result<(), Box<dyn std::error::Error>> {
    /// let data = client.read_holding_registers(100, 2).await?;
    /// let val_u16 = data[0];
    /// let val_u32 = data.get_u32(0, WordOrder::BigEndian)?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn read_holding_registers(&self, address: u16, quantity: u16) -> Result<RegisterData> {
        let req = Request::ReadHoldingRegisters { address, quantity };
        let resp = self.execute(req, Priority::Normal).await?;

        match resp {
            Response::ReadHoldingRegisters(regs) => Ok(RegisterData::new(regs)),
            _ => Err(ModbusError::InvalidFrame("Unexpected response variant".into())),
        }
    }

    /// Reads consecutive input registers (FC 04) starting at `address`.
    ///
    /// Returns a [`RegisterData`] wrapper with scalar conversion helpers.
    pub async fn read_input_registers(&self, address: u16, quantity: u16) -> Result<RegisterData> {
        let req = Request::ReadInputRegisters { address, quantity };
        let resp = self.execute(req, Priority::Normal).await?;

        match resp {
            Response::ReadInputRegisters(regs) => Ok(RegisterData::new(regs)),
            _ => Err(ModbusError::InvalidFrame("Unexpected response variant".into())),
        }
    }

    /// Reads consecutive coils (FC 01) starting at `address`.
    ///
    /// Returns a `Vec<bool>` where each entry represents one coil state (`true` for ON, `false` for OFF).
    pub async fn read_coils(&self, address: u16, quantity: u16) -> Result<Vec<bool>> {
        let req = Request::ReadCoils { address, quantity };
        let resp = self.execute(req, Priority::Normal).await?;

        match resp {
            Response::ReadCoils(coils) => Ok(coils),
            _ => Err(ModbusError::InvalidFrame("Unexpected response variant".into())),
        }
    }

    /// Reads consecutive discrete inputs (FC 02) starting at `address`.
    ///
    /// Returns a `Vec<bool>` containing the digital input states.
    pub async fn read_discrete_inputs(&self, address: u16, quantity: u16) -> Result<Vec<bool>> {
        let req = Request::ReadDiscreteInputs { address, quantity };
        let resp = self.execute(req, Priority::Normal).await?;

        match resp {
            Response::ReadDiscreteInputs(inputs) => Ok(inputs),
            _ => Err(ModbusError::InvalidFrame("Unexpected response variant".into())),
        }
    }

    /// Writes a single coil (FC 05) at `address`.
    ///
    /// Dispatched with [`Priority::Immediate`].
    /// Returns the written boolean state echoed by the slave.
    pub async fn write_single_coil(&self, address: u16, value: bool) -> Result<bool> {
        let req = Request::WriteSingleCoil { address, value };
        let resp = self.execute(req, Priority::Immediate).await?;

        match resp {
            Response::WriteSingleCoil { value, .. } => Ok(value),
            _ => Err(ModbusError::InvalidFrame("Unexpected response variant".into())),
        }
    }

    /// Writes a single 16-bit holding register (FC 06) at `address`.
    ///
    /// Dispatched with [`Priority::Immediate`].
    /// Returns the written register value echoed by the slave.
    pub async fn write_single_register(&self, address: u16, value: u16) -> Result<u16> {
        let req = Request::WriteSingleRegister { address, value };
        let resp = self.execute(req, Priority::Immediate).await?;

        match resp {
            Response::WriteSingleRegister { value, .. } => Ok(value),
            _ => Err(ModbusError::InvalidFrame("Unexpected response variant".into())),
        }
    }

    /// Writes multiple consecutive coils (FC 15 / 0x0F) starting at `address`.
    ///
    /// Dispatched with [`Priority::Immediate`].
    /// Returns the quantity of coils successfully written.
    pub async fn write_multiple_coils(&self, address: u16, values: Vec<bool>) -> Result<u16> {
        let req = Request::WriteMultipleCoils { address, values };
        let resp = self.execute(req, Priority::Immediate).await?;

        match resp {
            Response::WriteMultipleCoils { quantity, .. } => Ok(quantity),
            _ => Err(ModbusError::InvalidFrame("Unexpected response variant".into())),
        }
    }

    /// Writes multiple consecutive 16-bit registers (FC 16 / 0x10) starting at `address`.
    ///
    /// Dispatched with [`Priority::Immediate`].
    /// Returns the quantity of registers successfully written.
    pub async fn write_multiple_registers(&self, address: u16, values: &[u16]) -> Result<u16> {
        let req = Request::WriteMultipleRegisters {
            address,
            values: values.to_vec(),
        };
        let resp = self.execute(req, Priority::Immediate).await?;

        match resp {
            Response::WriteMultipleRegisters { quantity, .. } => Ok(quantity),
            _ => Err(ModbusError::InvalidFrame("Unexpected response variant".into())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use crate::core::crc::crc16;
    use crate::transport::traits::Transport;

    struct MockSlaveTransport {
        slave_id: u8,
    }

    #[async_trait]
    impl Transport for MockSlaveTransport {
        async fn exchange(&mut self, _request_frame: &[u8], _timeout: Duration) -> Result<Vec<u8>> {
            let mut response = vec![self.slave_id, 0x03, 0x04, 0x00, 0x64, 0x00, 0xC8];
            let crc = crc16(&response);
            response.push((crc & 0xFF) as u8);
            response.push((crc >> 8) as u8);
            Ok(response)
        }

        async fn flush(&mut self) -> Result<()> {
            Ok(())
        }

        async fn close(&mut self) -> Result<()> {
            Ok(())
        }

        fn is_open(&self) -> bool {
            true
        }
    }

    #[tokio::test]
    async fn test_client_read_holding_registers() {
        let transport = MockSlaveTransport { slave_id: 1 };
        let queue = PortQueueHandle::new(transport, 10);

        let client = ModbusClient::new(
            1,
            FramingKind::Rtu,
            queue,
            ClientOptions::default(),
        ).unwrap();

        let data = client.read_holding_registers(0, 2).await.unwrap();
        assert_eq!(data.len(), 2);
        assert_eq!(data.get_u16(0).unwrap(), 100);
        assert_eq!(data.get_u16(1).unwrap(), 200);
    }

    #[tokio::test]
    async fn test_client_set_slave_id_and_raw_exchange_opt() {
        let transport = MockSlaveTransport { slave_id: 1 };
        let queue = PortQueueHandle::new(transport, 10);

        let mut client = ModbusClient::new(
            1,
            FramingKind::Rtu,
            queue,
            ClientOptions::default(),
        ).unwrap();

        assert_eq!(client.slave_id(), 1);
        client.set_slave_id(42).unwrap();
        assert_eq!(client.slave_id(), 42);

        // Invalid slave ID rejected
        assert!(client.set_slave_id(0).is_err());
        assert!(client.set_slave_id(248).is_err());

        // Fire-and-forget raw exchange (ignore_no_response: true)
        let pdu = vec![0x06, 0x00, 0x01, 0x00, 0x02];
        let resp = client.raw_exchange_opt(&pdu, Priority::Immediate, true).await.unwrap();
        assert!(resp.is_empty());
    }

    #[tokio::test]
    async fn test_client_device_state_handler() {
        let transport = MockSlaveTransport { slave_id: 1 };
        let queue = PortQueueHandle::new(transport, 10);

        let client = ModbusClient::new(
            1,
            FramingKind::Rtu,
            queue,
            ClientOptions::default(),
        ).unwrap();

        let state_called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let state_called_clone = state_called.clone();

        client.set_device_state_handler(move |slave_id, is_connected, _err| {
            if slave_id == 1 && is_connected {
                state_called_clone.store(true, Ordering::SeqCst);
            }
        }).await;

        let _ = client.read_holding_registers(0, 2).await.unwrap();
        assert!(state_called.load(Ordering::SeqCst));
    }
}
