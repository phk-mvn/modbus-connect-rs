//! Modbus RTU serial transport over RS-485 and RS-232 lines.
//!
//! Powered by [`tokio_serial`], this transport manages native serial port streams,
//! automatic reconnection, stream flushing, and response length prediction.

use async_trait::async_trait;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_serial::{SerialPortBuilderExt, SerialStream};
use crate::error::{ModbusError, Result};
use crate::transport::traits::Transport;

/// Serial port configuration parameters for Modbus RTU communication.
///
/// # Example
///
/// ```
/// use modbus_connect::transport::rtu::RtuConfig;
///
/// let config = RtuConfig::new("/dev/ttyUSB0", 115200);
/// assert_eq!(config.baud_rate, 115200);
/// assert_eq!(config.path, "/dev/ttyUSB0");
/// ```
#[derive(Debug, Clone)]
pub struct RtuConfig {
    /// Filesystem path to the serial device (e.g. `/dev/ttyUSB0`, `/dev/tty.usbserial-10`, or `COM3`).
    pub path: String,
    /// Baud rate (e.g. 9600, 19200, 38400, 115200).
    pub baud_rate: u32,
    /// Number of data bits (defaults to 8).
    pub data_bits: tokio_serial::DataBits,
    /// Number of stop bits (defaults to 1).
    pub stop_bits: tokio_serial::StopBits,
    /// Parity mode (defaults to None).
    pub parity: tokio_serial::Parity,
}

impl RtuConfig {
    /// Creates a new RTU configuration with standard 8N1 settings (8 data bits, no parity, 1 stop bit).
    pub fn new(path: impl Into<String>, baud_rate: u32) -> Self {
        Self {
            path: path.into(),
            baud_rate,
            data_bits: tokio_serial::DataBits::Eight,
            stop_bits: tokio_serial::StopBits::One,
            parity: tokio_serial::Parity::None,
        }
    }
}

/// Asynchronous Modbus RTU transport over a physical or virtual serial port.
pub struct RtuTransport {
    config: RtuConfig,
    stream: Option<SerialStream>,
}

impl RtuTransport {
    /// Opens the serial port using the parameters in [`RtuConfig`].
    ///
    /// # Errors
    ///
    /// Returns [`ModbusError::Io`] if the serial port cannot be opened or configured.
    pub fn open(config: RtuConfig) -> Result<Self> {
        let stream = tokio_serial::new(&config.path, config.baud_rate)
            .data_bits(config.data_bits)
            .stop_bits(config.stop_bits)
            .parity(config.parity)
            .open_native_async()
            .map_err(|e| ModbusError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;

        Ok(Self {
            config,
            stream: Some(stream),
        })
    }

    /// Ensures that the serial port stream is currently connected, attempting re-opening if necessary.
    fn ensure_connected(&mut self) -> Result<&mut SerialStream> {
        if self.stream.is_none() {
            let stream = tokio_serial::new(&self.config.path, self.config.baud_rate)
                .data_bits(self.config.data_bits)
                .stop_bits(self.config.stop_bits)
                .parity(self.config.parity)
                .open_native_async()
                .map_err(|e| ModbusError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
            self.stream = Some(stream);
        }
        Ok(self.stream.as_mut().unwrap())
    }

    /// Predicts the total expected byte length of an RTU response frame from its initial bytes.
    ///
    /// In Modbus RTU, frames do not have an explicit overall length header.
    /// The expected frame length is calculated by inspecting the Function Code:
    /// - Exception responses (`FC | 0x80`): exactly 5 bytes (Slave + FC + ExceptionCode + CRC16).
    /// - Read responses (FC 01, 02, 03, 04): `3 + byte_count + 2` bytes (Slave + FC + ByteCount + Data + CRC16).
    /// - Single write responses (FC 05, 06): exactly 8 bytes (Slave + FC + Address + Value + CRC16).
    /// - Multiple write responses (FC 15, 16): exactly 8 bytes (Slave + FC + Address + Quantity + CRC16).
    ///
    /// Returns `None` if more bytes must be received to determine the length.
    pub fn predict_response_len(buf: &[u8]) -> Option<usize> {
        if buf.len() < 2 {
            return None;
        }

        let function_code = buf[1];

        // Modbus Exception response (MSB is set)
        if (function_code & 0x80) != 0 {
            return Some(5);
        }

        // Read responses: byte count is at index 2
        if matches!(function_code, 0x01 | 0x02 | 0x03 | 0x04) {
            if buf.len() >= 3 {
                let byte_count = buf[2] as usize;
                return Some(1 + 1 + 1 + byte_count + 2);
            }
            return None;
        }

        // Fixed length write responses
        if matches!(function_code, 0x05 | 0x06 | 0x0F | 0x10) {
            return Some(8);
        }

        None
    }
}

#[async_trait]
impl Transport for RtuTransport {
    /// Transmits a raw RTU request frame and reads until the predicted response length is reached.
    async fn exchange(&mut self, request_frame: &[u8], timeout: Duration) -> Result<Vec<u8>> {
        let exchange_future = async {
            let stream = self.ensure_connected()?;

            let _ = stream.flush().await;

            stream.write_all(request_frame).await.map_err(ModbusError::Io)?;
            stream.flush().await.map_err(ModbusError::Io)?;

            let mut response = Vec::with_capacity(256);
            let mut byte_buf = [0u8; 64];

            loop {
                let n = stream.read(&mut byte_buf).await.map_err(ModbusError::Io)?;
                if n == 0 {
                    return Err(ModbusError::NotConnected("Serial port closed by remote".into()));
                }
                response.extend_from_slice(&byte_buf[..n]);

                if let Some(expected_len) = Self::predict_response_len(&response) {
                    if response.len() >= expected_len {
                        response.truncate(expected_len);
                        return Ok(response);
                    }
                }
            }
        };

        match tokio::time::timeout(timeout, exchange_future).await {
            Ok(result) => result,
            Err(_) => Err(ModbusError::Timeout(timeout)),
        }
    }

    async fn write_frame(&mut self, request_frame: &[u8]) -> Result<()> {
        let stream = self.ensure_connected()?;
        let _ = stream.flush().await;
        stream.write_all(request_frame).await.map_err(ModbusError::Io)?;
        stream.flush().await.map_err(ModbusError::Io)?;
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        if let Some(stream) = &mut self.stream {
            stream.flush().await.map_err(ModbusError::Io)?;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        self.stream = None;
        Ok(())
    }

    fn is_open(&self) -> bool {
        self.stream.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_predict_response_len() {
        // Read 2 holding registers response (FC 3): ByteCount = 4
        // Total: 1 (slave) + 1 (FC) + 1 (count) + 4 (data) + 2 (crc) = 9
        let partial_read = [0x01, 0x03, 0x04, 0x00];
        assert_eq!(RtuTransport::predict_response_len(&partial_read), Some(9));

        // Write single register response (FC 6): always 8 bytes
        let write_single = [0x01, 0x06, 0x00, 0x01];
        assert_eq!(RtuTransport::predict_response_len(&write_single), Some(8));

        // Exception response (FC 0x83): always 5 bytes
        let exception = [0x01, 0x83, 0x02];
        assert_eq!(RtuTransport::predict_response_len(&exception), Some(5));
    }
}
