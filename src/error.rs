//! Modbus protocol exception codes and library error types.

use std::time::Duration;
use thiserror::Error;

/// Standard Modbus protocol exception response codes.
///
/// Returned by a Modbus slave/server when a request cannot be processed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ExceptionCode {
    /// Function code received in the query is not recognized or supported (0x01).
    #[error("Illegal Function (0x01): function not supported by device")]
    IllegalFunction,

    /// Data address received in the query is not allowable for the device (0x02).
    #[error("Illegal Data Address (0x02): register address out of range")]
    IllegalDataAddress,

    /// Value contained in the query data field is not an allowable value (0x03).
    #[error("Illegal Data Value (0x03): invalid data value for register")]
    IllegalDataValue,

    /// Unrecoverable error occurred while server was attempting to perform requested action (0x04).
    #[error("Slave Device Failure (0x04): unrecoverable error on slave device")]
    SlaveDeviceFailure,

    /// Specialized use in conjunction with programming commands (0x05).
    #[error("Acknowledge (0x05): request accepted and executing in background")]
    Acknowledge,

    /// Server is engaged in processing a long-duration program command (0x06).
    #[error("Slave Device Busy (0x06): device is busy, retry request later")]
    SlaveDeviceBusy,

    /// Memory parity error detected in reading extended memory (0x08).
    #[error("Memory Parity Error (0x08): parity failure in device memory")]
    MemoryParityError,

    /// Gateway was unable to allocate an internal communication path (0x0A).
    #[error("Gateway Path Unavailable (0x0A): gateway communication path unavailable")]
    GatewayPathUnavailable,

    /// No response was obtained from the target device behind the gateway (0x0B).
    #[error("Gateway Target Device Failed (0x0B): target device failed to respond")]
    GatewayTargetDeviceFailed,

    /// Unrecognized exception code.
    #[error("Unknown Exception Code (0x{0:02X})")]
    Unknown(u8),
}

impl From<u8> for ExceptionCode {
    fn from(code: u8) -> Self {
        match code {
            0x01 => ExceptionCode::IllegalFunction,
            0x02 => ExceptionCode::IllegalDataAddress,
            0x03 => ExceptionCode::IllegalDataValue,
            0x04 => ExceptionCode::SlaveDeviceFailure,
            0x05 => ExceptionCode::Acknowledge,
            0x06 => ExceptionCode::SlaveDeviceBusy,
            0x08 => ExceptionCode::MemoryParityError,
            0x0A => ExceptionCode::GatewayPathUnavailable,
            0x0B => ExceptionCode::GatewayTargetDeviceFailed,
            other => ExceptionCode::Unknown(other),
        }
    }
}

impl From<ExceptionCode> for u8 {
    fn from(code: ExceptionCode) -> Self {
        match code {
            ExceptionCode::IllegalFunction => 0x01,
            ExceptionCode::IllegalDataAddress => 0x02,
            ExceptionCode::IllegalDataValue => 0x03,
            ExceptionCode::SlaveDeviceFailure => 0x04,
            ExceptionCode::Acknowledge => 0x05,
            ExceptionCode::SlaveDeviceBusy => 0x06,
            ExceptionCode::MemoryParityError => 0x08,
            ExceptionCode::GatewayPathUnavailable => 0x0A,
            ExceptionCode::GatewayTargetDeviceFailed => 0x0B,
            ExceptionCode::Unknown(other) => other,
        }
    }
}

impl ExceptionCode {
    /// Returns the numerical Modbus exception code byte.
    pub fn to_u8(&self) -> u8 {
        u8::from(*self)
    }
}

/// Primary error type for `modbus-connect`.
#[derive(Debug, Error)]
pub enum ModbusError {
    /// CRC16 mismatch detected on an incoming RTU frame.
    #[error("CRC mismatch: expected {expected:#06X}, calculated {calculated:#06X}")]
    CrcError {
        /// Expected CRC16 checksum read from the frame.
        expected: u16,
        /// Calculated CRC16 checksum over frame payload.
        calculated: u16,
    },

    /// Modbus exception returned by the remote slave/server.
    #[error("Modbus exception: {0}")]
    Exception(ExceptionCode),

    /// Buffer contained fewer bytes than expected for frame parsing.
    #[error("Buffer underrun: expected at least {expected} bytes, got {actual}")]
    BufferUnderrun {
        /// Number of bytes expected.
        expected: usize,
        /// Actual number of bytes available.
        actual: usize,
    },

    /// Slave ID outside valid Modbus range (1..=247).
    #[error("Invalid slave ID: {0} (must be between 1 and 247)")]
    InvalidSlaveId(u8),

    /// Requested register or coil quantity is invalid.
    #[error("Invalid quantity: {0}")]
    InvalidQuantity(u16),

    /// Register or coil address is invalid.
    #[error("Invalid address: {0}")]
    InvalidAddress(u16),

    /// Operation timed out after the specified duration.
    #[error("Request timed out after {0:?}")]
    Timeout(Duration),

    /// Port queue capacity exceeded while backpressure policy is active.
    #[error("Port queue overflow (max capacity {0})")]
    QueueOverflow(usize),

    /// Request rejected because the port is temporarily paused for bus discovery/scanning.
    #[error("Operation rejected: port is currently paused for bus scanning")]
    ScanActive,

    /// Transport is not currently connected or session was closed.
    #[error("Transport not connected: {0}")]
    NotConnected(String),

    /// Malformed or unexpected protocol frame format.
    #[error("Invalid frame: {0}")]
    InvalidFrame(String),

    /// Underlying I/O error from serial port or network stream.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Physical mode constraint violation (e.g. RS232 supports at most 1 device).
    #[error("RS-mode constraint error: {0}")]
    RsModeConstraint(String),

    /// Client already exists with the specified client ID.
    #[error("Client already exists: {0}")]
    ClientAlreadyExists(String),

    /// Duplicate slave ID on the same transport port.
    #[error("Duplicate slave ID {0} on transport '{1}'. Set allow_duplicate_slave_id to true to permit.")]
    DuplicateSlaveId(u8, String),
    
    /// Client with the specified ID was not found.
    #[error("Client not found: {0}")]
    ClientNotFound(String),
}

/// Convenience alias for `std::result::Result<T, ModbusError>`.
pub type Result<T> = std::result::Result<T, ModbusError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exception_code_from_u8() {
        assert_eq!(ExceptionCode::from(0x02), ExceptionCode::IllegalDataAddress);
        assert_eq!(ExceptionCode::from(0x06), ExceptionCode::SlaveDeviceBusy);
        assert_eq!(ExceptionCode::from(0x99), ExceptionCode::Unknown(0x99));
    }

    #[test]
    fn test_error_display() {
        let err = ModbusError::InvalidSlaveId(0);
        assert_eq!(err.to_string(), "Invalid slave ID: 0 (must be between 1 and 247)");
    }
}
