//! Modbus register decoding and conversion to primitive types (`i16`, `u32`, `i32`, `f32`, `f64`, `String`).

use std::fmt;
use std::ops::{Deref, Index};
use crate::error::{ModbusError, Result};

/// Word and byte ordering in multi-register Modbus values (32-bit and 64-bit).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WordOrder {
    /// Big-endian word order (AB CD), standard Modbus convention.
    #[default]
    BigEndian,
    /// Little-endian word order (CD AB), often used by modern PLCs.
    LittleEndian,
    /// Big-endian word order with byte swap (BA DC).
    BigEndianByteSwap,
    /// Little-endian word order with byte swap (DC BA).
    LittleEndianByteSwap,
}

/// Container for 16-bit Modbus registers with high-level conversion methods.
///
/// Implements `Index<usize>`, `Deref<Target = [u16]>`, and `Display` for direct,
/// ergonomic access to register values without boilerplate error-handling.
///
/// # Example
///
/// ```
/// use modbus_connect::core::register_data::{RegisterData, WordOrder};
///
/// let data = RegisterData::new(vec![0x4148, 0x0000]);
/// assert_eq!(data[0], 0x4148);
/// assert_eq!(data[1], 0x0000);
/// assert_eq!(data.len(), 2);
///
/// let float_val = data.get_f32(0, WordOrder::BigEndian).unwrap();
/// assert_eq!(float_val, 12.5);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterData {
    registers: Vec<u16>,
}

impl RegisterData {
    /// Creates a new `RegisterData` instance from a vector of 16-bit words.
    pub fn new(registers: Vec<u16>) -> Self {
        Self { registers }
    }

    /// Creates a new `RegisterData` instance from a slice of 16-bit words.
    pub fn from_slice(slice: &[u16]) -> Self {
        Self {
            registers: slice.to_vec(),
        }
    }

    /// Returns the number of registers contained in the buffer.
    pub fn len(&self) -> usize {
        self.registers.len()
    }

    /// Returns `true` if the register buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.registers.is_empty()
    }

    /// Returns a slice reference to the underlying 16-bit registers.
    pub fn as_slice(&self) -> &[u16] {
        &self.registers
    }

    /// Consumes the container and returns the inner vector of registers.
    pub fn into_vec(self) -> Vec<u16> {
        self.registers
    }

    /// Reads a 16-bit signed integer (`i16`) at `index`.
    pub fn get_i16(&self, index: usize) -> Result<i16> {
        let val = self.get_u16(index)?;
        Ok(val as i16)
    }

    /// Reads a 16-bit unsigned integer (`u16`) at `index`.
    pub fn get_u16(&self, index: usize) -> Result<u16> {
        self.registers
            .get(index)
            .copied()
            .ok_or(ModbusError::BufferUnderrun {
                expected: index + 1,
                actual: self.registers.len(),
            })
    }

    /// Reads a 32-bit unsigned integer (`u32`) starting at `index` using `order`.
    pub fn get_u32(&self, index: usize, order: WordOrder) -> Result<u32> {
        if index + 1 >= self.registers.len() {
            return Err(ModbusError::BufferUnderrun {
                expected: index + 2,
                actual: self.registers.len(),
            });
        }
        let w0 = self.registers[index];
        let w1 = self.registers[index + 1];

        let bytes = assemble_4_bytes(w0, w1, order);
        Ok(u32::from_be_bytes(bytes))
    }

    /// Reads a 32-bit signed integer (`i32`) starting at `index` using `order`.
    pub fn get_i32(&self, index: usize, order: WordOrder) -> Result<i32> {
        let val = self.get_u32(index, order)?;
        Ok(val as i32)
    }

    /// Reads a 32-bit floating point number (`f32`) starting at `index` using `order`.
    pub fn get_f32(&self, index: usize, order: WordOrder) -> Result<f32> {
        let val = self.get_u32(index, order)?;
        Ok(f32::from_bits(val))
    }

    /// Reads a 64-bit floating point number (`f64`) starting at `index` using `order`.
    pub fn get_f64(&self, index: usize, order: WordOrder) -> Result<f64> {
        if index + 3 >= self.registers.len() {
            return Err(ModbusError::BufferUnderrun {
                expected: index + 4,
                actual: self.registers.len(),
            });
        }
        let w = [
            self.registers[index],
            self.registers[index + 1],
            self.registers[index + 2],
            self.registers[index + 3],
        ];

        let bytes = assemble_8_bytes(w, order);
        let bits = u64::from_be_bytes(bytes);
        Ok(f64::from_bits(bits))
    }

    /// Decodes an ASCII/UTF-8 string of `register_count` words starting at `index`.
    ///
    /// Truncates at the first null byte if present.
    pub fn get_string(&self, index: usize, register_count: usize) -> Result<String> {
        if index + register_count > self.registers.len() {
            return Err(ModbusError::BufferUnderrun {
                expected: index + register_count,
                actual: self.registers.len(),
            });
        }

        let mut bytes = Vec::with_capacity(register_count * 2);
        for i in 0..register_count {
            let reg = self.registers[index + i];
            bytes.extend_from_slice(&reg.to_be_bytes());
        }

        if let Some(pos) = bytes.iter().position(|&b| b == 0) {
            bytes.truncate(pos);
        }

        Ok(String::from_utf8_lossy(&bytes).to_string())
    }

    /// Encodes a `u32` value into two 16-bit registers according to `order`.
    pub fn u32_to_registers(val: u32, order: WordOrder) -> [u16; 2] {
        disassemble_4_bytes(val.to_be_bytes(), order)
    }

    /// Encodes an `i32` value into two 16-bit registers according to `order`.
    pub fn i32_to_registers(val: i32, order: WordOrder) -> [u16; 2] {
        Self::u32_to_registers(val as u32, order)
    }

    /// Encodes an `f32` value into two 16-bit registers according to `order`.
    pub fn f32_to_registers(val: f32, order: WordOrder) -> [u16; 2] {
        Self::u32_to_registers(val.to_bits(), order)
    }

    /// Encodes an `f64` value into four 16-bit registers according to `order`.
    pub fn f64_to_registers(val: f64, order: WordOrder) -> [u16; 4] {
        disassemble_8_bytes(val.to_bits().to_be_bytes(), order)
    }

    /// Encodes a string slice into 16-bit registers (two bytes per word).
    pub fn string_to_registers(s: &str) -> Vec<u16> {
        let bytes = s.as_bytes();
        let mut registers = Vec::with_capacity((bytes.len() + 1) / 2);

        for chunk in bytes.chunks(2) {
            if chunk.len() == 2 {
                registers.push(u16::from_be_bytes([chunk[0], chunk[1]]));
            } else {
                registers.push(u16::from_be_bytes([chunk[0], 0]));
            }
        }
        registers
    }

    /// Returns the register value at `index`, or `0` if out of bounds.
    pub fn u16(&self, index: usize) -> u16 {
        self.registers.get(index).copied().unwrap_or(0)
    }

    /// Returns the signed integer value at `index`, or `0` if out of bounds.
    pub fn i16(&self, index: usize) -> i16 {
        self.u16(index) as i16
    }

    /// Returns the first register as `u16`, or `0` if empty.
    pub fn as_u16(&self) -> u16 {
        self.u16(0)
    }

    /// Returns the first register as `i16`, or `0` if empty.
    pub fn as_i16(&self) -> i16 {
        self.i16(0)
    }

    /// Reads registers 0..2 as big-endian `u32`, or `0` on error.
    pub fn as_u32_be(&self) -> u32 {
        self.get_u32(0, WordOrder::BigEndian).unwrap_or(0)
    }

    /// Reads registers 0..2 as little-endian `u32`, or `0` on error.
    pub fn as_u32_le(&self) -> u32 {
        self.get_u32(0, WordOrder::LittleEndian).unwrap_or(0)
    }

    /// Reads registers 0..2 as big-endian `i32`, or `0` on error.
    pub fn as_i32_be(&self) -> i32 {
        self.get_i32(0, WordOrder::BigEndian).unwrap_or(0)
    }

    /// Reads registers 0..2 as little-endian `i32`, or `0` on error.
    pub fn as_i32_le(&self) -> i32 {
        self.get_i32(0, WordOrder::LittleEndian).unwrap_or(0)
    }

    /// Reads registers 0..2 as big-endian `f32`, or `0.0` on error.
    pub fn as_f32_be(&self) -> f32 {
        self.get_f32(0, WordOrder::BigEndian).unwrap_or(0.0)
    }

    /// Reads registers 0..2 as little-endian `f32`, or `0.0` on error.
    pub fn as_f32_le(&self) -> f32 {
        self.get_f32(0, WordOrder::LittleEndian).unwrap_or(0.0)
    }
}

impl Index<usize> for RegisterData {
    type Output = u16;

    fn index(&self, index: usize) -> &Self::Output {
        &self.registers[index]
    }
}

impl Deref for RegisterData {
    type Target = [u16];

    fn deref(&self) -> &Self::Target {
        &self.registers
    }
}

impl fmt::Display for RegisterData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.registers)
    }
}

impl IntoIterator for RegisterData {
    type Item = u16;
    type IntoIter = std::vec::IntoIter<u16>;

    fn into_iter(self) -> Self::IntoIter {
        self.registers.into_iter()
    }
}

impl<'a> IntoIterator for &'a RegisterData {
    type Item = &'a u16;
    type IntoIter = std::slice::Iter<'a, u16>;

    fn into_iter(self) -> Self::IntoIter {
        self.registers.iter()
    }
}

fn assemble_4_bytes(w0: u16, w1: u16, order: WordOrder) -> [u8; 4] {
    let [a, b] = w0.to_be_bytes();
    let [c, d] = w1.to_be_bytes();
    match order {
        WordOrder::BigEndian => [a, b, c, d],
        WordOrder::LittleEndian => [c, d, a, b],
        WordOrder::BigEndianByteSwap => [b, a, d, c],
        WordOrder::LittleEndianByteSwap => [d, c, b, a],
    }
}

fn disassemble_4_bytes(bytes: [u8; 4], order: WordOrder) -> [u16; 2] {
    let [b0, b1, b2, b3] = bytes;
    let (w0_bytes, w1_bytes) = match order {
        WordOrder::BigEndian => ([b0, b1], [b2, b3]),
        WordOrder::LittleEndian => ([b2, b3], [b0, b1]),
        WordOrder::BigEndianByteSwap => ([b1, b0], [b3, b2]),
        WordOrder::LittleEndianByteSwap => ([b3, b2], [b1, b0]),
    };
    [u16::from_be_bytes(w0_bytes), u16::from_be_bytes(w1_bytes)]
}

fn assemble_8_bytes(w: [u16; 4], order: WordOrder) -> [u8; 8] {
    let mut words = w;
    if matches!(
        order,
        WordOrder::BigEndianByteSwap | WordOrder::LittleEndianByteSwap
    ) {
        for word in &mut words {
            *word = word.swap_bytes();
        }
    }
    if matches!(
        order,
        WordOrder::LittleEndian | WordOrder::LittleEndianByteSwap
    ) {
        words.reverse();
    }

    let [a0, a1] = words[0].to_be_bytes();
    let [b0, b1] = words[1].to_be_bytes();
    let [c0, c1] = words[2].to_be_bytes();
    let [d0, d1] = words[3].to_be_bytes();
    [a0, a1, b0, b1, c0, c1, d0, d1]
}

fn disassemble_8_bytes(bytes: [u8; 8], order: WordOrder) -> [u16; 4] {
    let mut words = [0u16; 4];
    for i in 0..4 {
        let b0 = bytes[i * 2];
        let b1 = bytes[i * 2 + 1];
        words[i] = u16::from_be_bytes([b0, b1]);
    }
    if matches!(
        order,
        WordOrder::BigEndianByteSwap | WordOrder::LittleEndianByteSwap
    ) {
        for word in &mut words {
            *word = word.swap_bytes();
        }
    }
    if matches!(
        order,
        WordOrder::LittleEndian | WordOrder::LittleEndianByteSwap
    ) {
        words.reverse();
    }
    words
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_i16_reading() {
        let data = RegisterData::new(vec![0xFFFF, 100]);
        assert_eq!(data.get_i16(0).unwrap(), -1);
        assert_eq!(data.get_i16(1).unwrap(), 100);
    }

    #[test]
    fn test_u32_endianness() {
        let val: u32 = 0x12345678;
        let be_regs = RegisterData::u32_to_registers(val, WordOrder::BigEndian);
        assert_eq!(be_regs, [0x1234, 0x5678]);
        let le_regs = RegisterData::u32_to_registers(val, WordOrder::LittleEndian);
        assert_eq!(le_regs, [0x5678, 0x1234]);
        let data_be = RegisterData::from_slice(&be_regs);
        assert_eq!(data_be.get_u32(0, WordOrder::BigEndian).unwrap(), val);
        let data_le = RegisterData::from_slice(&le_regs);
        assert_eq!(data_le.get_u32(0, WordOrder::LittleEndian).unwrap(), val);
    }

    #[test]
    fn test_f32_reading_and_writing() {
        let original_float: f32 = 123.456;
        let regs = RegisterData::f32_to_registers(original_float, WordOrder::BigEndian);
        let data = RegisterData::from_slice(&regs);
        let parsed_float = data.get_f32(0, WordOrder::BigEndian).unwrap();
        assert!((parsed_float - original_float).abs() < 1e-5);
    }

    #[test]
    fn test_string_conversion() {
        let original_str = "TEST";
        let regs = RegisterData::string_to_registers(original_str);
        assert_eq!(regs, vec![0x5445, 0x5354]);
        let data = RegisterData::from_slice(&regs);
        let parsed_str = data.get_string(0, 2).unwrap();
        assert_eq!(parsed_str, "TEST");
    }

    #[test]
    fn test_indexing_and_display() {
        let data = RegisterData::new(vec![111, 222]);
        assert_eq!(data[0], 111);
        assert_eq!(data[1], 222);
        assert_eq!(data.as_u16(), 111);
        assert_eq!(format!("{}", data), "[111, 222]");
    }
}
