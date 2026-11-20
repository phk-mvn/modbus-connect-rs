//! Modbus TCP transport implemented with Tokio asynchronous TCP sockets.
//!
//! Handles MBAP (Modbus Application Protocol) header transmission and frame assembly.

use async_trait::async_trait;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use crate::error::{ModbusError, Result};
use crate::transport::traits::Transport;

/// Modbus TCP client transport over a Tokio [`TcpStream`].
///
/// Automatically parses the MBAP header's length field to read exact response frames.
pub struct TcpTransport {
    addr: String,
    stream: Option<TcpStream>,
}

impl TcpTransport {
    /// Connects to a remote Modbus TCP server at the given address (e.g. `"192.168.1.100:502"`).
    ///
    /// # Errors
    ///
    /// Returns [`ModbusError::Io`] if TCP connection fails.
    pub async fn connect(addr: impl Into<String>) -> Result<Self> {
        let addr = addr.into();
        let stream = TcpStream::connect(&addr).await.map_err(ModbusError::Io)?;
        Ok(Self {
            addr,
            stream: Some(stream),
        })
    }

    /// Re-establishes the TCP connection if currently disconnected.
    async fn ensure_connected(&mut self) -> Result<&mut TcpStream> {
        if self.stream.is_none() {
            let stream = TcpStream::connect(&self.addr)
                .await
                .map_err(ModbusError::Io)?;
            self.stream = Some(stream);
        }
        Ok(self.stream.as_mut().unwrap())
    }
}

#[async_trait]
impl Transport for TcpTransport {
    /// Transmits a Modbus TCP frame and awaits the complete response frame according to the MBAP header.
    async fn exchange(&mut self, request_frame: &[u8], timeout: Duration) -> Result<Vec<u8>> {
        let exchange_future = async {
            let stream = self.ensure_connected().await?;

            stream.write_all(request_frame).await.map_err(ModbusError::Io)?;

            let mut header = [0u8; 7];
            stream.read_exact(&mut header).await.map_err(ModbusError::Io)?;

            let length_field = u16::from_be_bytes([header[4], header[5]]) as usize;
            if length_field < 1 {
                return Err(ModbusError::InvalidFrame("MBAP length is zero".into()));
            }

            let pdu_len = length_field - 1;
            let mut pdu = vec![0u8; pdu_len];
            stream.read_exact(&mut pdu).await.map_err(ModbusError::Io)?;

            let mut full_frame = Vec::with_capacity(7 + pdu_len);
            full_frame.extend_from_slice(&header);
            full_frame.extend_from_slice(&pdu);

            Ok(full_frame)
        };

        match tokio::time::timeout(timeout, exchange_future).await {
            Ok(result) => result,
            Err(_) => {
                self.stream = None;
                Err(ModbusError::Timeout(timeout))
            }
        }
    }

    async fn write_frame(&mut self, request_frame: &[u8]) -> Result<()> {
        let stream = self.ensure_connected().await?;
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
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn test_tcp_transport_loopback() {
        // 1. Bind mock TCP server on ephemeral port (port 0)
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap().to_string();

        // 2. Emulate slave response in background task
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut req = vec![0u8; 12];
            socket.read_exact(&mut req).await.unwrap();

            // Echo response: TxId(2), ProtoId=0(2), Len=5(2), Slave(1), FC(1), ByteCount(1), Data(2)
            let response = vec![
                req[0], req[1], 0x00, 0x00, 0x00, 0x05, req[6], 0x03, 0x02, 0x00, 0x42,
            ];
            socket.write_all(&response).await.unwrap();
        });

        let mut transport = TcpTransport::connect(server_addr).await.unwrap();
        let mock_req = vec![
            0x00, 0x01, 0x00, 0x00, 0x00, 0x06, 0x01, 0x03, 0x00, 0x00, 0x00, 0x01,
        ];
        let resp = transport
            .exchange(&mock_req, Duration::from_millis(500))
            .await
            .unwrap();

        assert_eq!(resp.len(), 11);
        assert_eq!(resp[6], 0x01); // Slave ID
        assert_eq!(resp[7], 0x03); // FC
        assert_eq!(resp[10], 0x42); // Value
        transport.close().await.unwrap();
    }
}
