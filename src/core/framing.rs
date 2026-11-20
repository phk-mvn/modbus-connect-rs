//! Modbus Application Data Unit (ADU) framing for RTU and TCP protocols.

use crate::core::crc::{check_crc, crc16};
use crate::core::pdu::{Request, Response};
use crate::error::{ModbusError, Result};

/// Protocol framing transport variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FramingKind {
    /// Modbus RTU framing with binary Slave ID header and trailing CRC16.
    Rtu,
    /// Modbus TCP framing with 7-byte MBAP header.
    Tcp,
}

/// Modbus RTU Application Data Unit (ADU) framer and parser.
pub struct RtuFramer;

impl RtuFramer {
    /// Builds a full RTU frame: `[Slave ID] + [PDU] + [CRC16 (little-endian)]`.
    pub fn build_frame(slave_id: u8, request: &Request) -> Vec<u8> {
        let pdu = request.encode_pdu();
        let mut frame = Vec::with_capacity(1 + pdu.len() + 2);
        frame.push(slave_id);
        frame.extend_from_slice(&pdu);

        let crc = crc16(&frame);
        frame.push((crc & 0xFF) as u8);
        frame.push((crc >> 8) as u8);
        frame
    }

    /// Validates CRC16, slave ID, and decodes the response PDU from a raw RTU frame.
    ///
    /// # Errors
    ///
    /// - Returns [`ModbusError::BufferUnderrun`] if the frame is shorter than 4 bytes.
    /// - Returns [`ModbusError::CrcError`] if checksum validation fails.
    /// - Returns [`ModbusError::InvalidFrame`] if the slave ID does not match `expected_slave`.
    pub fn parse_response(
        expected_slave: u8,
        request: &Request,
        frame: &[u8],
    ) -> Result<Response> {
        if frame.len() < 4 {
            return Err(ModbusError::BufferUnderrun {
                expected: 4,
                actual: frame.len(),
            });
        }

        if !check_crc(frame) {
            let data_len = frame.len() - 2;
            let calculated = crc16(&frame[..data_len]);
            let actual_low = frame[data_len];
            let actual_high = frame[data_len + 1];
            let expected = (actual_high as u16) << 8 | (actual_low as u16);
            return Err(ModbusError::CrcError { expected, calculated });
        }

        let slave_id = frame[0];
        if slave_id != expected_slave {
            return Err(ModbusError::InvalidFrame(format!(
                "Slave ID mismatch: expected {}, got {}",
                expected_slave, slave_id
            )));
        }

        let pdu = &frame[1..frame.len() - 2];
        Response::decode_pdu(request, pdu)
    }
}

/// Modbus TCP Application Data Unit (ADU) framer and parser with MBAP header.
pub struct TcpFramer;

impl TcpFramer {
    /// Length of the standard Modbus TCP MBAP header in bytes (7 bytes).
    pub const MBAP_HEADER_LEN: usize = 7;

    /// Builds a full Modbus TCP frame with MBAP header:
    /// `[Transaction ID (2B)] + [Protocol ID = 0 (2B)] + [Length (2B)] + [Unit ID (1B)] + [PDU]`.
    pub fn build_frame(transaction_id: u16, unit_id: u8, request: &Request) -> Vec<u8> {
        let pdu = request.encode_pdu();
        let length = (pdu.len() + 1) as u16;

        let mut frame = Vec::with_capacity(Self::MBAP_HEADER_LEN + pdu.len());
        frame.extend_from_slice(&transaction_id.to_be_bytes());
        frame.extend_from_slice(&0u16.to_be_bytes());
        frame.extend_from_slice(&length.to_be_bytes());
        frame.push(unit_id);
        frame.extend_from_slice(&pdu);
        frame
    }

    /// Validates the MBAP header (transaction ID, protocol ID 0, unit ID) and decodes the response PDU.
    ///
    /// # Errors
    ///
    /// - Returns [`ModbusError::BufferUnderrun`] if the frame is shorter than the MBAP header or payload length.
    /// - Returns [`ModbusError::InvalidFrame`] if transaction ID, protocol ID, or unit ID does not match.
    pub fn parse_response(
        expected_transaction_id: u16,
        expected_unit_id: u8,
        request: &Request,
        frame: &[u8],
    ) -> Result<Response> {
        if frame.len() < Self::MBAP_HEADER_LEN {
            return Err(ModbusError::BufferUnderrun {
                expected: Self::MBAP_HEADER_LEN,
                actual: frame.len(),
            });
        }

        let transaction_id = u16::from_be_bytes([frame[0], frame[1]]);
        let protocol_id = u16::from_be_bytes([frame[2], frame[3]]);
        let length = u16::from_be_bytes([frame[4], frame[5]]) as usize;
        let unit_id = frame[6];

        if transaction_id != expected_transaction_id {
            return Err(ModbusError::InvalidFrame(format!(
                "Transaction ID mismatch: expected {}, got {}",
                expected_transaction_id, transaction_id
            )));
        }

        if protocol_id != 0 {
            return Err(ModbusError::InvalidFrame(format!(
                "Protocol ID must be 0 for Modbus TCP: expected 0, got {}",
                protocol_id
            )));
        }

        if unit_id != expected_unit_id {
            return Err(ModbusError::InvalidFrame(format!(
                "Unit ID mismatch: expected {}, got {}",
                expected_unit_id, unit_id
            )));
        }

        if frame.len() < Self::MBAP_HEADER_LEN + length - 1 {
            return Err(ModbusError::BufferUnderrun {
                expected: Self::MBAP_HEADER_LEN + length - 1,
                actual: frame.len(),
            });
        }

        let pdu = &frame[Self::MBAP_HEADER_LEN..];
        Response::decode_pdu(request, pdu)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rtu_framing_roundtrip() {
        let slave_id = 1;
        let request = Request::ReadHoldingRegisters {
            address: 0,
            quantity: 2,
        };
        let frame = RtuFramer::build_frame(slave_id, &request);
        assert_eq!(frame.len(), 8);
        assert!(check_crc(&frame));

        let mut response_frame = vec![0x01, 0x03, 0x04, 0x00, 0x0A, 0x00, 0x14];
        let crc = crc16(&response_frame);
        response_frame.push((crc & 0xFF) as u8);
        response_frame.push((crc >> 8) as u8);
        let parsed = RtuFramer::parse_response(slave_id, &request, &response_frame).unwrap();
        match parsed {
            Response::ReadHoldingRegisters(regs) => {
                assert_eq!(regs, vec![10, 20]);
            }
            _ => panic!("Expected ReadHoldingRegisters response"),
        }
    }

    #[test]
    fn test_rtu_crc_error() {
        let slave_id = 1;
        let request = Request::ReadHoldingRegisters {
            address: 0,
            quantity: 2,
        };
        let corrupt_frame = vec![0x01, 0x03, 0x04, 0x00, 0x0A, 0x00, 0x14, 0xFF, 0xFF];
        let res = RtuFramer::parse_response(slave_id, &request, &corrupt_frame);
        assert!(matches!(res, Err(ModbusError::CrcError { .. })));
    }

    #[test]
    fn test_tcp_framing_roundtrip() {
        let tx_id = 42;
        let unit_id = 1;
        let request = Request::ReadHoldingRegisters {
            address: 10,
            quantity: 1,
        };
        let frame = TcpFramer::build_frame(tx_id, unit_id, &request);
        assert_eq!(frame.len(), TcpFramer::MBAP_HEADER_LEN + 5);

        let mut response_frame = Vec::new();
        response_frame.extend_from_slice(&tx_id.to_be_bytes());
        response_frame.extend_from_slice(&0u16.to_be_bytes());
        response_frame.extend_from_slice(&4u16.to_be_bytes());
        response_frame.push(unit_id);
        response_frame.extend_from_slice(&[0x03, 0x02, 0x12, 0x34]);
        let parsed = TcpFramer::parse_response(tx_id, unit_id, &request, &response_frame).unwrap();
        match parsed {
            Response::ReadHoldingRegisters(regs) => {
                assert_eq!(regs, vec![0x1234]);
            }
            _ => panic!("Expected ReadHoldingRegisters response"),
        }
    }
}
