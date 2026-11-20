//! Data types, field representations, and declarative definitions for Modbus schemas.
//!
//! Enables structuring raw register tables into typed variables (integers, floats, booleans, strings)
//! and multi-variable contiguous register groups.

use std::collections::HashMap;
use crate::core::register_data::{RegisterData, WordOrder};
use crate::error::{ModbusError, Result};

/// Modbus memory table classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TableKind {
    /// Read/write 16-bit registers (FC 03 / FC 06 / FC 16).
    #[default]
    Holding,
    /// Read-only 16-bit registers (FC 04).
    Input,
    /// Read/write single-bit boolean coils (FC 01 / FC 05 / FC 15).
    Coil,
    /// Read-only single-bit discrete inputs (FC 02).
    Discrete,
}

/// Primitive data types decodable from Modbus registers or coils.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldType {
    /// 1-bit boolean flag (stored in a coil, discrete input, or 16-bit register non-zero check).
    Bool,
    /// Unsigned 16-bit integer (1 register).
    U16,
    /// Signed 16-bit integer (1 register).
    I16,
    /// Unsigned 32-bit integer (2 registers).
    U32,
    /// Signed 32-bit integer (2 registers).
    I32,
    /// IEEE 754 32-bit single-precision floating point (2 registers).
    F32,
    /// IEEE 754 64-bit double-precision floating point (4 registers).
    F64,
    /// Fixed-length ASCII string stored across `N` consecutive 16-bit registers (2 ASCII chars per register).
    String(usize),
    /// Dictionary of named boolean flags mapped to bit positions (0-15) within a 16-bit register.
    Bitmask(HashMap<String, u8>),
}

impl FieldType {
    /// Returns the number of consecutive 16-bit registers required to represent this data type.
    pub fn register_count(&self) -> u16 {
        match self {
            FieldType::Bool => 1,
            FieldType::U16 | FieldType::I16 => 1,
            FieldType::U32 | FieldType::I32 | FieldType::F32 => 2,
            FieldType::F64 => 4,
            FieldType::String(count) => *count as u16,
            FieldType::Bitmask(_) => 1,
        }
    }
}

/// Typed runtime value decoded from or to be written to a Modbus table.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    /// Boolean flag value.
    Bool(bool),
    /// Unsigned 16-bit integer value.
    U16(u16),
    /// Signed 16-bit integer value.
    I16(i16),
    /// Unsigned 32-bit integer value.
    U32(u32),
    /// Signed 32-bit integer value.
    I32(i32),
    /// Single-precision float value.
    F32(f32),
    /// Double-precision float value.
    F64(f64),
    /// Decoded UTF-8 string value.
    String(String),
    /// Decoded bitmask map of boolean flags unpacked from a 16-bit register.
    Bitmask(HashMap<String, bool>),
}

impl FieldValue {
    /// Extracts the inner [`f32`] value, or returns [`ModbusError::InvalidFrame`] if variant mismatches.
    pub fn as_f32(&self) -> Result<f32> {
        match self {
            FieldValue::F32(v) => Ok(*v),
            _ => Err(ModbusError::InvalidFrame("Expected F32 value".into())),
        }
    }

    /// Extracts the inner [`u16`] value, or returns [`ModbusError::InvalidFrame`] if variant mismatches.
    pub fn as_u16(&self) -> Result<u16> {
        match self {
            FieldValue::U16(v) => Ok(*v),
            _ => Err(ModbusError::InvalidFrame("Expected U16 value".into())),
        }
    }

    /// Extracts the inner [`bool`] value, or returns [`ModbusError::InvalidFrame`] if variant mismatches.
    pub fn as_bool(&self) -> Result<bool> {
        match self {
            FieldValue::Bool(v) => Ok(*v),
            _ => Err(ModbusError::InvalidFrame("Expected Bool value".into())),
        }
    }

    /// Extracts the inner bitmask map, or returns [`ModbusError::InvalidFrame`] if variant mismatches.
    pub fn as_bitmask(&self) -> Result<&HashMap<String, bool>> {
        match self {
            FieldValue::Bitmask(m) => Ok(m),
            _ => Err(ModbusError::InvalidFrame("Expected Bitmask value".into())),
        }
    }
}

/// Declaration of an individual named field in a Modbus device.
#[derive(Debug, Clone)]
pub struct SchemaField {
    /// Symbolic identifier for this field (e.g. `"frequency"`, `"status_word"`).
    pub name: String,
    /// Starting Modbus register address.
    pub address: u16,
    /// Target Modbus memory table.
    pub table: TableKind,
    /// Data type representation.
    pub field_type: FieldType,
    /// Endianness and byte-swapping order.
    pub word_order: WordOrder,
    /// Optional bit position (0-15) for boolean flags in a 16-bit register.
    pub bit: Option<u8>,
}

/// A member subfield residing at an offset within a contiguous [`SchemaGroup`].
#[derive(Debug, Clone)]
pub struct GroupSubfield {
    /// Symbolic identifier for the subfield.
    pub name: String,
    /// Offset (in 16-bit registers) from the group's base address.
    pub offset: u16,
    /// Data type representation.
    pub field_type: FieldType,
    /// Optional bit position (0-15) for boolean flags in a 16-bit register.
    pub bit: Option<u8>,
}

/// Declaration of a contiguous block of registers read in a single Modbus transaction.
///
/// Grouping reduces bus roundtrips by fetching all interrelated metrics simultaneously.
#[derive(Debug, Clone)]
pub struct SchemaGroup {
    /// Symbolic identifier for this group.
    pub name: String,
    /// Base starting register address of the group.
    pub address: u16,
    /// Target table (must be Holding or Input registers).
    pub table: TableKind,
    /// Word ordering applied across all multi-register subfields.
    pub word_order: WordOrder,
    /// Subfield definitions within the block.
    pub subfields: Vec<GroupSubfield>,
}

impl SchemaGroup {
    /// Creates a new empty schema group.
    pub fn new(name: impl Into<String>, address: u16, table: TableKind, word_order: WordOrder) -> Self {
        Self {
            name: name.into(),
            address,
            table,
            word_order,
            subfields: Vec::new(),
        }
    }

    /// Adds a subfield at a specific register offset within the group.
    pub fn add_subfield(mut self, name: impl Into<String>, offset: u16, field_type: FieldType) -> Self {
        self.subfields.push(GroupSubfield {
            name: name.into(),
            offset,
            field_type,
            bit: None,
        });
        self
    }

    /// Adds a boolean subfield targeting a specific bit position within a 16-bit register.
    pub fn add_bit_subfield(mut self, name: impl Into<String>, offset: u16, bit: u8) -> Self {
        self.subfields.push(GroupSubfield {
            name: name.into(),
            offset,
            field_type: FieldType::Bool,
            bit: Some(bit),
        });
        self
    }

    /// Calculates the total number of consecutive 16-bit registers needed to span all subfields.
    pub fn total_registers(&self) -> u16 {
        let mut max_end = 0;
        for sf in &self.subfields {
            let end = sf.offset + sf.field_type.register_count();
            if end > max_end {
                max_end = end;
            }
        }
        max_end
    }

    /// Decodes raw [`RegisterData`] buffer into a map of named [`FieldValue`] items according to subfield offsets.
    pub fn decode_group(&self, data: &RegisterData) -> Result<HashMap<String, FieldValue>> {
        let mut result = HashMap::new();
        for sf in &self.subfields {
            let idx = sf.offset as usize;
            let val = match &sf.field_type {
                FieldType::U16 => FieldValue::U16(data.get_u16(idx)?),
                FieldType::I16 => FieldValue::I16(data.get_i16(idx)?),
                FieldType::U32 => FieldValue::U32(data.get_u32(idx, self.word_order)?),
                FieldType::I32 => FieldValue::I32(data.get_i32(idx, self.word_order)?),
                FieldType::F32 => FieldValue::F32(data.get_f32(idx, self.word_order)?),
                FieldType::F64 => FieldValue::F64(data.get_f64(idx, self.word_order)?),
                FieldType::String(len) => FieldValue::String(data.get_string(idx, *len)?),
                FieldType::Bool => {
                    let raw = data.get_u16(idx)?;
                    if let Some(b) = sf.bit {
                        FieldValue::Bool(((raw >> b) & 1) != 0)
                    } else {
                        FieldValue::Bool(raw != 0)
                    }
                }
                FieldType::Bitmask(bits) => {
                    let raw = data.get_u16(idx)?;
                    let mut map = HashMap::new();
                    for (flag, &pos) in bits {
                        map.insert(flag.clone(), ((raw >> pos) & 1) != 0);
                    }
                    FieldValue::Bitmask(map)
                }
            };
            result.insert(sf.name.clone(), val);
        }
        Ok(result)
    }
}

/// Complete declarative specification of a device's registers and fields.
///
/// # Example
///
/// ```
/// use modbus_connect::schema::{DeviceSchema, FieldType, TableKind};
/// use modbus_connect::core::register_data::WordOrder;
///
/// let schema = DeviceSchema::new(WordOrder::BigEndian)
///     .add_field("temperature", 100, TableKind::Input, FieldType::F32)
///     .add_field("pump_enabled", 0, TableKind::Coil, FieldType::Bool);
///
/// assert!(schema.fields.contains_key("temperature"));
/// ```
#[derive(Debug, Clone, Default)]
pub struct DeviceSchema {
    /// Default word ordering applied to multi-register fields.
    pub default_word_order: WordOrder,
    /// Standalone field definitions keyed by name.
    pub fields: HashMap<String, SchemaField>,
    /// Contiguous group definitions keyed by name.
    pub groups: HashMap<String, SchemaGroup>,
}

impl DeviceSchema {
    /// Creates an empty schema with the specified default word order.
    pub fn new(default_word_order: WordOrder) -> Self {
        Self {
            default_word_order,
            fields: HashMap::new(),
            groups: HashMap::new(),
        }
    }

    /// Adds a single named field to the schema.
    pub fn add_field(
        mut self,
        name: impl Into<String>,
        address: u16,
        table: TableKind,
        field_type: FieldType,
    ) -> Self {
        let name_str = name.into();
        self.fields.insert(
            name_str.clone(),
            SchemaField {
                name: name_str,
                address,
                table,
                field_type,
                word_order: self.default_word_order,
                bit: None,
            },
        );
        self
    }

    /// Adds a boolean field targeting a specific bit (0-15) of a 16-bit register.
    pub fn add_bit_field(
        mut self,
        name: impl Into<String>,
        address: u16,
        table: TableKind,
        bit: u8,
    ) -> Self {
        let name_str = name.into();
        self.fields.insert(
            name_str.clone(),
            SchemaField {
                name: name_str,
                address,
                table,
                field_type: FieldType::Bool,
                word_order: self.default_word_order,
                bit: Some(bit),
            },
        );
        self
    }

    /// Adds a contiguous register group to the schema.
    pub fn add_group(mut self, group: SchemaGroup) -> Self {
        self.groups.insert(group.name.clone(), group);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_group_total_registers_and_decoding() {
        let group = SchemaGroup::new("power_metrics", 100, TableKind::Holding, WordOrder::BigEndian)
            .add_subfield("voltage", 0, FieldType::F32)
            .add_subfield("current", 2, FieldType::F32);
        assert_eq!(group.total_registers(), 4);
        let v_regs = RegisterData::f32_to_registers(220.5, WordOrder::BigEndian);
        let c_regs = RegisterData::f32_to_registers(5.25, WordOrder::BigEndian);
        let all_regs = vec![v_regs[0], v_regs[1], c_regs[0], c_regs[1]];
        let data = RegisterData::new(all_regs);
        let decoded = group.decode_group(&data).unwrap();
        let v = decoded.get("voltage").unwrap().as_f32().unwrap();
        let c = decoded.get("current").unwrap().as_f32().unwrap();
        assert!((v - 220.5).abs() < 1e-4);
        assert!((c - 5.25).abs() < 1e-4);
    }
}
