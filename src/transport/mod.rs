//! Transport layer abstractions, queue actor, connection management, and device emulation.
//!
//! This module provides the communication infrastructure for Modbus:
//! - [`traits::Transport`]: Async trait implemented by physical or emulated channels.
//! - [`queue::PortQueueHandle`]: Priority-based actor queue that guarantees serialized access to a shared port.
//! - [`controller::TransportController`]: Central coordinator managing sessions, clients, polling, and traffic sniffers.
//! - [`rtu::RtuTransport`]: Serial port transport (RS-485 / RS-232) via `tokio-serial` (behind `rtu` feature).
//! - [`tcp::TcpTransport`]: Modbus TCP socket transport via `tokio::net::TcpStream` (behind `tcp` feature).
//! - [`emulator::SlaveEmulator`]: In-memory Modbus slave emulator for automated unit testing (behind `emulator` feature).

pub mod client_registry;
pub mod controller;
pub mod queue;
pub mod rs_mode;
pub mod traits;

#[cfg(feature = "emulator")]
pub mod emulator;

#[cfg(feature = "tcp")]
pub mod tcp;

#[cfg(feature = "rtu")]
pub mod rtu;

pub use client_registry::{ClientRegistry, RegisteredClient};
pub use controller::{CreateClientOptions, TransportController, TransportStatus};
pub use queue::{PortQueueHandle, Priority};
pub use rs_mode::RsMode;
pub use traits::Transport;

#[cfg(feature = "emulator")]
pub use emulator::{EmulatorRegisterType, InfinityChangeParams, SlaveEmulator};

#[cfg(feature = "tcp")]
pub use tcp::TcpTransport;

#[cfg(feature = "rtu")]
pub use rtu::{RtuConfig, RtuTransport};
