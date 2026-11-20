//! Modbus Protocol Data Unit (PDU) encoding, decoding, and representation.

use crate::error::{ExceptionCode, ModbusError, Result};

/// Modbus request issued by a master/client to a slave/server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    /// FC 01: Read Coils (discrete output states).
    ReadCoils {
        /// Starting coil address (0-based).
        address: u16,
        /// Number of coils to read (1..=2000).
        quantity: u16,
    },
    /// FC 02: Read Discrete Inputs (read-only binary inputs).
    ReadDiscreteInputs {
        /// Starting discrete input address (0-based).
        address: u16,
        /// Number of inputs to read (1..=2000).
        quantity: u16,
    },
    /// FC 03: Read Holding Registers (read/write 16-bit registers).
    ReadHoldingRegisters {
        /// Starting holding register address (0-based).
        address: u16,
        /// Number of registers to read (1..=125).
        quantity: u16,
    },
    /// FC 04: Read Input Registers (read-only 16-bit registers).
    ReadInputRegisters {
        /// Starting input register address (0-based).
        address: u16,
        /// Number of registers to read (1..=125).
        quantity: u16,
    },
    /// FC 05: Write Single Coil (set single discrete output).
    WriteSingleCoil {
        /// Coil address (0-based).
        address: u16,
        /// Boolean value (`true` maps to `0xFF00`, `false` maps to `0x0000`).
        value: bool,
    },
    /// FC 06: Write Single Register (set single 16-bit holding register).
    WriteSingleRegister {
        /// Holding register address (0-based).
        address: u16,
        /// 16-bit register value.
        value: u16,
    },
    /// FC 15 (0x0F): Write Multiple Coils (set sequence of discrete outputs).
    WriteMultipleCoils {
        /// Starting coil address (0-based).
        address: u16,
        /// Values to write to coils.
        values: Vec<bool>,
    },
    /// FC 16 (0x10): Write Multiple Registers (set sequence of 16-bit holding registers).
    WriteMultipleRegisters {
        /// Starting register address (0-based).
        address: u16,
        /// Values to write to holding registers.
        values: Vec<u16>,
    },
}

impl Request {
    /// Returns the standard Modbus function code byte.
    pub fn function_code(&self) -> u8 {
        match self {
            Request::ReadCoils { .. } => 0x01,
            Request::ReadDiscreteInputs { .. } => 0x02,
            Request::ReadHoldingRegisters { .. } => 0x03,
            Request::ReadInputRegisters { .. } => 0x04,
            Request::WriteSingleCoil { .. } => 0x05,
            Request::WriteSingleRegister { .. } => 0x06,
            Request::WriteMultipleCoils { .. } => 0x0F,
            Request::WriteMultipleRegisters { .. } => 0x10,
        }
    }

    /// Serializes the request into raw PDU bytes (`[Function Code] + [Payload]`).
    pub fn encode_pdu(&self) -> Vec<u8> {
        let mut pdu = Vec::new();
        pdu.push(self.function_code());

        match self {
            Request::ReadCoils { address, quantity }
            | Request::ReadDiscreteInputs { address, quantity }
            | Request::ReadHoldingRegisters { address, quantity }
            | Request::ReadInputRegisters { address, quantity } => {
                pdu.extend_from_slice(&address.to_be_bytes());
                pdu.extend_from_slice(&quantity.to_be_bytes());
            }

            Request::WriteSingleCoil { address, value } => {
                pdu.extend_from_slice(&address.to_be_bytes());
                let raw_val: u16 = if *value { 0xFF00 } else { 0x0000 };
                pdu.extend_from_slice(&raw_val.to_be_bytes());
            }

            Request::WriteSingleRegister { address, value } => {
                pdu.extend_from_slice(&address.to_be_bytes());
                pdu.extend_from_slice(&value.to_be_bytes());
            }

            Request::WriteMultipleCoils { address, values } => {
                pdu.extend_from_slice(&address.to_be_bytes());
                pdu.extend_from_slice(&(values.len() as u16).to_be_bytes());
                let packed = pack_coils(values);
                pdu.push(packed.len() as u8);
                pdu.extend_from_slice(&packed);
            }

            Request::WriteMultipleRegisters { address, values } => {
                pdu.extend_from_slice(&address.to_be_bytes());
                pdu.extend_from_slice(&(values.len() as u16).to_be_bytes());
                let byte_count = (values.len() * 2) as u8;
                pdu.push(byte_count);
                for reg in values {
                    pdu.extend_from_slice(&reg.to_be_bytes());
                }
            }
        }

        pdu
    }
}

/// Modbus response returned from a slave/server to the master/client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    /// FC 01 response: unpacked boolean states of coils.
    ReadCoils(Vec<bool>),
    /// FC 02 response: unpacked boolean states of discrete inputs.
    ReadDiscreteInputs(Vec<bool>),
    /// FC 03 response: values of holding registers.
    ReadHoldingRegisters(Vec<u16>),
    /// FC 04 response: values of input registers.
    ReadInputRegisters(Vec<u16>),
    /// FC 05 response: echo of coil address and value.
    WriteSingleCoil {
        /// Echoed coil address.
        address: u16,
        /// Echoed boolean value.
        value: bool,
    },
    /// FC 06 response: echo of register address and value.
    WriteSingleRegister {
        /// Echoed register address.
        address: u16,
        /// Echoed register value.
        value: u16,
    },
    /// FC 15 response: echo of starting address and written quantity.
    WriteMultipleCoils {
        /// Echoed starting address.
        address: u16,
        /// Echoed count of written coils.
        quantity: u16,
    },
    /// FC 16 response: echo of starting address and written quantity.
    WriteMultipleRegisters {
        /// Echoed starting address.
        address: u16,
        /// Echoed count of written registers.
        quantity: u16,
    },
}

impl Response {
    /// Decodes raw PDU response bytes given the corresponding request.
    ///
    /// # Errors
    ///
    /// - Returns [`ModbusError::BufferUnderrun`] if the frame is incomplete.
    /// - Returns [`ModbusError::Exception`] if the slave returned an error code.
    /// - Returns [`ModbusError::InvalidFrame`] if function codes mismatch.
    pub fn decode_pdu(request: &Request, pdu: &[u8]) -> Result<Response> {
        if pdu.is_empty() {
            return Err(ModbusError::BufferUnderrun { expected: 1, actual: 0 });
        }

        let function_code = pdu[0];

        if (function_code & 0x80) != 0 {
            if pdu.len() < 2 {
                return Err(ModbusError::BufferUnderrun { expected: 2, actual: pdu.len() });
            }
            return Err(ModbusError::Exception(ExceptionCode::from(pdu[1])));
        }

        if function_code != request.function_code() {
            return Err(ModbusError::InvalidFrame(format!(
                "Function code mismatch: expected {}, got {}",
                request.function_code(),
                function_code
            )));
        }

        let payload = &pdu[1..];

        match request {
            Request::ReadCoils { quantity, .. } => {
                if payload.is_empty() {
                    return Err(ModbusError::BufferUnderrun { expected: 2, actual: pdu.len() });
                }

                let byte_count = payload[0] as usize;
                let data = &payload[1..];

                if data.len() < byte_count {
                    return Err(ModbusError::BufferUnderrun { expected: byte_count + 1, actual: payload.len() });
                }

                let coils = unpack_coils(data, *quantity as usize);
                Ok(Response::ReadCoils(coils))
            }

            Request::ReadDiscreteInputs { quantity, .. } => {
                if payload.is_empty() {
                    return Err(ModbusError::BufferUnderrun { expected: 2, actual: pdu.len() });
                }
                let byte_count = payload[0] as usize;
                let data = &payload[1..];
                if data.len() < byte_count {
                    return Err(ModbusError::BufferUnderrun { expected: byte_count + 1, actual: payload.len() });
                }
                let inputs = unpack_coils(data, *quantity as usize);
                Ok(Response::ReadDiscreteInputs(inputs))
            }
            Request::ReadHoldingRegisters { quantity, .. } => {
                let registers = parse_registers_response(payload, *quantity)?;
                Ok(Response::ReadHoldingRegisters(registers))
            }
            Request::ReadInputRegisters { quantity, .. } => {
                let registers = parse_registers_response(payload, *quantity)?;
                Ok(Response::ReadInputRegisters(registers))
            }
            Request::WriteSingleCoil { .. } => {
                if payload.len() < 4 {
                    return Err(ModbusError::BufferUnderrun { expected: 5, actual: pdu.len() });
                }
                let address = u16::from_be_bytes([payload[0], payload[1]]);
                let raw_val = u16::from_be_bytes([payload[2], payload[3]]);
                Ok(Response::WriteSingleCoil {
                    address,
                    value: raw_val == 0xFF00,
                })
            }
            Request::WriteSingleRegister { .. } => {
                if payload.len() < 4 {
                    return Err(ModbusError::BufferUnderrun { expected: 5, actual: pdu.len() });
                }
                let address = u16::from_be_bytes([payload[0], payload[1]]);
                let value = u16::from_be_bytes([payload[2], payload[3]]);
                Ok(Response::WriteSingleRegister { address, value })
            }
            Request::WriteMultipleCoils { .. } => {
                if payload.len() < 4 {
                    return Err(ModbusError::BufferUnderrun { expected: 5, actual: pdu.len() });
                }
                let address = u16::from_be_bytes([payload[0], payload[1]]);
                let quantity = u16::from_be_bytes([payload[2], payload[3]]);
                Ok(Response::WriteMultipleCoils { address, quantity })
            }
            Request::WriteMultipleRegisters { .. } => {
                if payload.len() < 4 {
                    return Err(ModbusError::BufferUnderrun { expected: 5, actual: pdu.len() });
                }
                let address = u16::from_be_bytes([payload[0], payload[1]]);
                let quantity = u16::from_be_bytes([payload[2], payload[3]]);
                Ok(Response::WriteMultipleRegisters { address, quantity })
            }
        }
    }
}

fn parse_registers_response(payload: &[u8], expected_quantity: u16) -> Result<Vec<u16>> {
    if payload.is_empty() {
        return Err(ModbusError::BufferUnderrun { expected: 1, actual: 0 });
    }

    let byte_count = payload[0] as usize;
    let data = &payload[1..];

    if data.len() < byte_count {
        return Err(ModbusError::BufferUnderrun {
            expected: byte_count + 1,
            actual: payload.len(),
        });
    }

    let mut registers = Vec::with_capacity(expected_quantity as usize);
    for chunk in data.chunks_exact(2).take(expected_quantity as usize) {
        let val = u16::from_be_bytes([chunk[0], chunk[1]]);
        registers.push(val);
    }
    Ok(registers)
}

/// Packs a slice of booleans into bytes (8 coils per byte, LSB-first).
pub fn pack_coils(values: &[bool]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity((values.len() + 7) / 8);
    for chunk in values.chunks(8) {
        let mut byte = 0u8;
        for (i, &val) in chunk.iter().enumerate() {
            if val {
                byte |= 1 << i;
            }
        }
        bytes.push(byte);
    }
    bytes
}

/// Unpacks raw bytes into a vector of boolean coil states.
pub fn unpack_coils(bytes: &[u8], quantity: usize) -> Vec<bool> {
    let mut coils = Vec::with_capacity(quantity);
    for &byte in bytes {
        for bit in 0..8 {
            if coils.len() >= quantity {
                break;
            }
            coils.push((byte & (1 << bit)) != 0);
        }
    }
    coils
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_read_holding_registers() {
        let req = Request::ReadHoldingRegisters { address: 0x0002, quantity: 0x000A };
        let pdu = req.encode_pdu();
        assert_eq!(pdu, vec![0x03, 0x00, 0x02, 0x00, 0x0A]);
    }

    #[test]
    fn test_decode_read_holding_registers_response() {
        let req = Request::ReadHoldingRegisters { address: 0x0000, quantity: 2 };
        let raw_response = [0x03, 0x04, 0x12, 0x34, 0x56, 0x78];
        let resp = Response::decode_pdu(&req, &raw_response).unwrap();
        match resp {
            Response::ReadHoldingRegisters(regs) => {
                assert_eq!(regs, vec![0x1234, 0x5678]);
            }
            _ => panic!("Expected ReadHoldingRegisters response"),
        }
    }

    #[test]
    fn test_decode_exception_response() {
        let req = Request::ReadHoldingRegisters { address: 0x0000, quantity: 2 };
        let raw_exception = [0x83, 0x02];
        let result = Response::decode_pdu(&req, &raw_exception);
        assert!(matches!(result, Err(ModbusError::Exception(ExceptionCode::IllegalDataAddress))));
    }

    #[test]
    fn test_coils_packing() {
        let coils = vec![true, false, true, true, false, false, false, false, true];
        let packed = pack_coils(&coils);
        assert_eq!(packed.len(), 2);
        let unpacked = unpack_coils(&packed, 9);
        assert_eq!(coils, unpacked);
    }
}
