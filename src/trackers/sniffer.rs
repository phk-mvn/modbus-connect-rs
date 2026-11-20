//! Passive traffic sniffer capturing raw transmitted and received Modbus frames.
//!
//! Powered by Tokio broadcast channels, [`TrafficSniffer`] allows real-time protocol
//! debugging, Wireshark-like inspection, or logging without impacting normal operation.

use std::time::SystemTime;
use tokio::sync::broadcast;

/// Direction of a captured Modbus frame on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Frame transmitted by the master (Request).
    Tx,
    /// Frame received from the slave (Response).
    Rx,
}

/// A captured Modbus frame timestamped with its direction and payload bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SniffedFrame {
    /// System clock timestamp when the frame was logged.
    pub timestamp: SystemTime,
    /// Whether the frame was transmitted (`Tx`) or received (`Rx`).
    pub direction: Direction,
    /// Raw bytes of the complete Application Data Unit (ADU).
    pub data: Vec<u8>,
}

impl SniffedFrame {
    /// Formats the raw frame payload as a space-separated uppercase hexadecimal string.
    ///
    /// # Example
    ///
    /// ```
    /// use std::time::SystemTime;
    /// use modbus_connect::trackers::{SniffedFrame, Direction};
    ///
    /// let frame = SniffedFrame {
    ///     timestamp: SystemTime::now(),
    ///     direction: Direction::Tx,
    ///     data: vec![0x01, 0x03, 0x00, 0x00, 0x00, 0x02, 0xC4, 0x0B],
    /// };
    /// assert_eq!(frame.to_hex_string(), "01 03 00 00 00 02 C4 0B");
    /// ```
    pub fn to_hex_string(&self) -> String {
        self.data
            .iter()
            .map(|b| format!("{:02X}", b))
            .collect::<Vec<_>>()
            .join(" ")
    }
}

/// Broadcast-based passive traffic sniffer.
///
/// Can be cloned cheaply; all subscribers receive copies of recorded frames.
///
/// # Example
///
/// ```
/// use modbus_connect::trackers::{TrafficSniffer, Direction};
///
/// # #[tokio::main]
/// # async fn main() {
/// let sniffer = TrafficSniffer::new(100);
/// let mut rx = sniffer.subscribe();
///
/// sniffer.record(Direction::Tx, &[0x01, 0x03, 0x00, 0x00]);
/// let captured = rx.recv().await.unwrap();
/// assert_eq!(captured.direction, Direction::Tx);
/// # }
/// ```
#[derive(Clone)]
pub struct TrafficSniffer {
    sender: broadcast::Sender<SniffedFrame>,
}

impl TrafficSniffer {
    /// Creates a new traffic sniffer with the specified broadcast channel buffer capacity.
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { sender }
    }

    /// Records a frame with the given direction and byte slice.
    pub fn record(&self, direction: Direction, data: &[u8]) {
        let frame = SniffedFrame {
            timestamp: SystemTime::now(),
            direction,
            data: data.to_vec(),
        };
        let _ = self.sender.send(frame);
    }

    /// Subscribes to the live stream of captured frames.
    pub fn subscribe(&self) -> broadcast::Receiver<SniffedFrame> {
        self.sender.subscribe()
    }
}

impl Default for TrafficSniffer {
    /// Creates a default sniffer with a ring buffer capacity of 256 frames.
    fn default() -> Self {
        Self::new(256)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_traffic_sniffer_broadcast() {
        let sniffer = TrafficSniffer::new(10);
        let mut subscriber1 = sniffer.subscribe();
        let mut subscriber2 = sniffer.subscribe();
        sniffer.record(Direction::Tx, &[0x01, 0x03, 0x00, 0x00, 0x00, 0x02]);
        let frame1 = subscriber1.recv().await.unwrap();
        let frame2 = subscriber2.recv().await.unwrap();
        assert_eq!(frame1.direction, Direction::Tx);
        assert_eq!(frame1.data, vec![0x01, 0x03, 0x00, 0x00, 0x00, 0x02]);
        assert_eq!(frame1.to_hex_string(), "01 03 00 00 00 02");
        assert_eq!(frame1, frame2);
    }
}
