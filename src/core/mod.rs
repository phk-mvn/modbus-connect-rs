//! Core Modbus primitives, framing parsers, data structures, and client implementation.
//!
//! This module houses the fundamental building blocks of the Modbus protocol:
//! - [`crc`]: CRC-16 calculation for Modbus RTU error checking.
//! - [`framing`]: Modbus RTU and Modbus TCP frame builders and response parsers.
//! - [`pdu`]: Strongly-typed Protocol Data Unit definitions covering standard Function Codes.
//! - [`register_data`]: Convenient container for 16-bit register slices with scalar decoding (u16, u32, f32, f64, strings).
//! - [`client`]: High-level asynchronous [`ModbusClient`] supporting timeouts, retries, and priority-queued execution.

pub mod crc;
pub mod framing;
pub mod pdu;
pub mod register_data;
pub mod client;

pub use client::{ClientOptions, ModbusClient};
