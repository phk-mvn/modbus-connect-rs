//! # modbus-connect
//!
//! High-performance, cross-platform Modbus RTU and Modbus TCP library for Rust.
//!
//! Designed as an architectural counterpart to the TypeScript `modbus-connect` library,
//! `modbus-connect` provides both asynchronous (`tokio`-based) and synchronous (blocking)
//! APIs for robust industrial automation communications.
//!
//! ## Core Architecture
//!
//! - **[`TransportController`]**: Central orchestrator managing physical connections (Serial RTU, TCP, Emulators).
//!   Each connection session owns a dedicated [`PortQueueHandle`] ensuring single serialization per line,
//!   and a personal [`PollingManager`].
//! - **[`ModbusClient`]**: High-level Modbus client for executing standard function codes (FC 01-06, 15, 16)
//!   with automatic retry policy, timeout handling, and request-response validation.
//! - **[`PortQueueHandle`]**: Per-port priority queue (`Immediate`, `Normal`, `Background`) preventing frame collisions
//!   on RS-485 / TCP buses.
//! - **[`PollingManager`]**: Flexible task scheduler supporting recurring polling tasks, exponential backoff,
//!   pause/resume controls, and declarative `on_data` / `on_error` callbacks.
//! - **[`TrafficSniffer`]**: Non-intrusive bus analyzer capturing raw TX/RX frames with timestamps and hex formatting.
//! - **[`blocking`]**: Ergonomic blocking API providing an embedded Tokio runtime under the hood, allowing usage
//!   without `async`/`await` in traditional synchronous applications.
//!
//! ## Quick Start (Blocking API)
//!
//! ```no_run
//! use std::time::Duration;
//! use modbus_connect::blocking::{TransportController, PollingTask};
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let port = "/dev/ttyUSB0";
//!     let controller = TransportController::new()?;
//!     controller.add_rtu(port, 57600)?;
//!
//!     let client = controller.create_client(port, 5)?;
//!
//!     let task = PollingTask::new("poll-task", Duration::from_millis(500), move || {
//!         client.read_holding_registers(0, 2)
//!     })
//!     .on_data(|data| {
//!         println!("Registers: {}", data);
//!         println!("Register 0: {}, Register 1: {}", data[0], data[1]);
//!     })
//!     .on_error(|err| {
//!         eprintln!("Polling error: {}", err);
//!     });
//!
//!     controller.add_polling_task(port, task)?;
//!     Ok(())
//! }
//! ```
//!
//! ## Quick Start (Async API)
//!
//! ```no_run
//! use std::time::Duration;
//! use modbus_connect::{TransportController, PollingTask};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let controller = TransportController::new();
//!     controller.add_tcp("127.0.0.1:502").await?;
//!
//!     let client = controller.create_client("127.0.0.1:502", 1, Default::default()).await?;
//!     let data = client.read_holding_registers(0, 2).await?;
//!     println!("Register 0: {}", data[0]);
//!     Ok(())
//! }
//! ```

pub mod core;
pub mod error;
pub mod transport;
pub mod trackers;

#[cfg(feature = "polling")]
pub mod polling;

#[cfg(feature = "scanner")]
pub mod scanner;

#[cfg(feature = "schema")]
pub mod schema;

#[cfg(feature = "blocking")]
pub mod blocking;

#[cfg(feature = "plugin")]
pub mod plugin;

pub use core::client::{ClientOptions, ModbusClient};
pub use core::framing::{FramingKind, RtuFramer, TcpFramer};
pub use core::pdu::{Request, Response};
pub use core::register_data::{RegisterData, WordOrder};
pub use error::{ExceptionCode, ModbusError, Result};
pub use trackers::{
    ConnectionErrorInfo, ConnectionErrorType, DeviceConnectionState, DeviceConnectionTracker,
    DeviceStateHandler, Direction, PortConnectionState, PortConnectionTracker, PortStateHandler,
    SniffedFrame, TrafficSniffer,
};
pub use transport::{
    ClientRegistry, CreateClientOptions, PortQueueHandle, Priority, RegisteredClient, RsMode,
    Transport, TransportController, TransportStatus,
};

#[cfg(feature = "emulator")]
pub use transport::{EmulatorRegisterType, InfinityChangeParams, SlaveEmulator};

#[cfg(feature = "tcp")]
pub use transport::TcpTransport;

#[cfg(feature = "rtu")]
pub use transport::{RtuConfig, RtuTransport};

#[cfg(feature = "polling")]
pub use polling::{PollAction, PollingEvent, PollingManager, PollingTask, RetryMode};

#[cfg(feature = "scanner")]
pub use scanner::{
    calculate_adaptive_timeout, DiscoveredDevice, ModbusScanner, ScanProfile, ScanReport,
    ScanStats,
};

#[cfg(feature = "schema")]
pub use schema::{
    BoundSchema, DeviceSchema, FieldType, FieldValue, GroupSubfield, SchemaField, SchemaGroup, TableKind,
};

#[cfg(feature = "plugin")]
pub use plugin::{CustomFunctionHandler, ExpectedLengthResolver, ModbusPlugin};