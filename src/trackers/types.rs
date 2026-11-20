//! Common types, error categories, and state objects for connection trackers.

use std::time::SystemTime;

/// Categorized connection error types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionErrorType {
    /// Unspecified or unexpected error.
    UnknownError,
    /// Physical port was closed.
    PortClosed,
    /// Reqeust timed out.
    Timeout,
    /// Checksum / CRC mismatch.
    CrcError,
    /// Network connection was lost or reset.
    ConnectionLost,
    /// Target slave device is unreachable / offline.
    DeviceOffline,
    /// Maximum reconnection attempts exceeded.
    MaxReconnect,
    /// Transport was manually disconnected by the user.
    ManualDisconnect,
    /// Transport was destroyed.
    Destroyed,
}

impl std::fmt::Display for ConnectionErrorType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownError => write!(f, "Unknown Error"),
            Self::PortClosed => write!(f, "Port closed"),
            Self::Timeout => write!(f, "Timeout"),
            Self::CrcError => write!(f, "CRC Error"),
            Self::ConnectionLost => write!(f, "Connection Lost"),
            Self::DeviceOffline => write!(f, "Device Offline"),
            Self::MaxReconnect => write!(f, "Max reconnect"),
            Self::ManualDisconnect => write!(f, "Manual disconnect"),
            Self::Destroyed => write!(f, "Destroyed"),
        }
    }
}

/// Detailed error information associated with a connection failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionErrorInfo {
    /// Categorized error type.
    pub error_type: ConnectionErrorType,
    /// Human-readable error description.
    pub message: String,
}

/// Connection state of an individual Modbus slave device.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceConnectionState {
    /// Slave unit identifier (1-247).
    pub slave_id: u8,
    /// Whether the device is currently responsive.
    pub is_connected: bool,
    /// Error information if disconnected.
    pub error: Option<ConnectionErrorInfo>,
    /// Timestamp of the last state change.
    pub timestamp: SystemTime,
}

/// Connection state of a physical port / transport channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortConnectionState {
    /// Whether the physical port/socket is open and healthy.
    pub is_connected: bool,
    /// List of slave device addresses active on this port.
    pub slave_ids: Vec<u8>,
    /// Error information if disconnected.
    pub error: Option<ConnectionErrorInfo>,
    /// Timestamp of the last state change.
    pub timestamp: SystemTime,
}