//! Asynchronous Modbus physical transport trait.
//!
//! Any stream or connection capable of Modbus half-duplex packet exchange implements [`Transport`].

use async_trait::async_trait;
use std::time::Duration;
use crate::error::Result;

/// Core interface for sending and receiving raw Modbus frame bytes.
///
/// Modbus communication is fundamentally half-duplex: a master transmits a request
/// frame and waits for a response frame from the slave. Implementors of this trait
/// handle the lower-level I/O mechanics, frame delimitation, and timeout handling.
#[async_trait]
pub trait Transport: Send {
    /// Transmits a request frame over the wire and waits for the complete response frame.
    ///
    /// # Arguments
    ///
    /// * `request_frame` - The fully-assembled raw request bytes (ADU) to transmit.
    /// * `timeout` - Maximum duration to wait for the slave's complete response.
    ///
    /// # Errors
    ///
    /// Returns [`ModbusError::Timeout`](crate::error::ModbusError::Timeout) if the slave does not respond in time,
    /// or [`ModbusError::Io`](crate::error::ModbusError::Io) upon socket/serial errors.
    async fn exchange(&mut self, request_frame: &[u8], timeout: Duration) -> Result<Vec<u8>>;

    /// Transmits a raw request frame without waiting for a response (fire-and-forget).
    async fn write_frame(&mut self, request_frame: &[u8]) -> Result<()> {
        let _ = self.flush().await;
        let _ = self.exchange(request_frame, Duration::from_millis(10)).await;
        Ok(())
    }

    /// Flushes any pending outbound or inbound buffers.
    async fn flush(&mut self) -> Result<()>;

    /// Gracefully closes the underlying communication channel.
    async fn close(&mut self) -> Result<()>;

    /// Returns `true` if the transport is currently open and healthy.
    fn is_open(&self) -> bool;
}
