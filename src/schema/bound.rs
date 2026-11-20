//! High-level declarative schema bound to an active Modbus client.
//!
//! [`BoundSchema`] pairs a [`DeviceSchema`] with a [`ModbusClient`], enabling named reads
//! and writes without manual calculation of register addresses, offsets, or byte conversions.

use std::collections::HashMap;

use crate::core::client::ModbusClient;
use crate::core::register_data::RegisterData;
use crate::error::{ModbusError, Result};
use crate::schema::types::{DeviceSchema, FieldType, FieldValue, SchemaField, TableKind};

/// An active binding between a [`DeviceSchema`] and a [`ModbusClient`].
///
/// # Example
///
/// ```no_run
/// use modbus_connect::schema::{DeviceSchema, BoundSchema, FieldType, TableKind};
/// use modbus_connect::core::register_data::WordOrder;
/// use modbus_connect::core::ModbusClient;
///
/// # async fn doc(client: ModbusClient) -> Result<(), Box<dyn std::error::Error>> {
/// let schema = DeviceSchema::new(WordOrder::BigEndian)
///     .add_field("temperature", 100, TableKind::Holding, FieldType::F32);
///
/// let bound = BoundSchema::new(client, schema);
/// let temp_val = bound.read_field("temperature").await?;
/// println!("Temp: {}", temp_val.as_f32()?);
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct BoundSchema {
    client: ModbusClient,
    schema: DeviceSchema,
}

impl BoundSchema {
    /// Creates a new bound schema tying the client to the declarative device specification.
    pub fn new(client: ModbusClient, schema: DeviceSchema) -> Self {
        Self { client, schema }
    }

    /// Accesses the underlying [`ModbusClient`].
    pub fn client(&self) -> &ModbusClient {
        &self.client
    }

    /// Accesses the underlying [`DeviceSchema`].
    pub fn schema(&self) -> &DeviceSchema {
        &self.schema
    }

    /// Reads a single named field from the remote device, automatically executing
    /// the appropriate Modbus read function and decoding the bytes into a [`FieldValue`].
    ///
    /// Supports dot-notation paths to access group subfields directly (e.g. `"metrics.voltage"`).
    ///
    /// # Errors
    ///
    /// - Returns [`ModbusError::InvalidFrame`] if `name` is not declared in the schema.
    /// - Returns communication errors if the read request fails.
    pub async fn read_field(&self, name: &str) -> Result<FieldValue> {
        if let Some((group_name, subfield_name)) = name.split_once('.') {
            let group_values = self.read_group(group_name).await?;
            return group_values.get(subfield_name).cloned().ok_or_else(|| {
                ModbusError::InvalidFrame(format!(
                    "Subfield '{}' not found in group '{}'",
                    subfield_name, group_name
                ))
            });
        }

        let field = self.schema.fields.get(name).ok_or_else(|| {
            ModbusError::InvalidFrame(format!("Field '{}' not found in schema", name))
        })?;

        match field.table {
            TableKind::Holding => {
                let count = field.field_type.register_count();
                let data = self.client.read_holding_registers(field.address, count).await?;
                decode_single_field(field, &data)
            }
            TableKind::Input => {
                let count = field.field_type.register_count();
                let data = self.client.read_input_registers(field.address, count).await?;
                decode_single_field(field, &data)
            }
            TableKind::Coil => {
                let coils = self.client.read_coils(field.address, 1).await?;
                let val = coils.first().copied().unwrap_or(false);
                Ok(FieldValue::Bool(val))
            }
            TableKind::Discrete => {
                let inputs = self.client.read_discrete_inputs(field.address, 1).await?;
                let val = inputs.first().copied().unwrap_or(false);
                Ok(FieldValue::Bool(val))
            }
        }
    }

    /// Reads an entire contiguous group of fields in a single Modbus read transaction.
    ///
    /// # Errors
    ///
    /// - Returns [`ModbusError::InvalidFrame`] if `name` is not in the schema or targets a non-register table.
    /// - Returns communication errors if the transaction fails.
    pub async fn read_group(&self, name: &str) -> Result<HashMap<String, FieldValue>> {
        let group = self.schema.groups.get(name).ok_or_else(|| {
            ModbusError::InvalidFrame(format!("Group '{}' not found in schema", name))
        })?;
        let total_regs = group.total_registers();
        let data = match group.table {
            TableKind::Holding => {
                self.client
                    .read_holding_registers(group.address, total_regs)
                    .await?
            }
            TableKind::Input => {
                self.client
                    .read_input_registers(group.address, total_regs)
                    .await?
            }
            _ => {
                return Err(ModbusError::InvalidFrame(
                    "Groups only support Holding or Input tables".into(),
                ))
            }
        };
        group.decode_group(&data)
    }

    /// Encodes and atomically writes contiguous registers for an entire group.
    pub async fn write_group(&self, name: &str, values: &HashMap<String, FieldValue>) -> Result<()> {
        let group = self.schema.groups.get(name).ok_or_else(|| {
            ModbusError::InvalidFrame(format!("Group '{}' not found in schema", name))
        })?;
        if group.table != TableKind::Holding {
            return Err(ModbusError::InvalidFrame(
                "write_group only supports Holding register groups".into(),
            ));
        }

        let total_regs = group.total_registers();
        let mut regs = self
            .client
            .read_holding_registers(group.address, total_regs)
            .await
            .map(|d| d.to_vec())
            .unwrap_or_else(|_| vec![0u16; total_regs as usize]);

        for sf in &group.subfields {
            if let Some(val) = values.get(&sf.name) {
                let encoded = encode_subfield(&sf.field_type, sf.bit, val, group.word_order)?;
                for (i, reg) in encoded.iter().enumerate() {
                    let target_idx = sf.offset as usize + i;
                    if target_idx < regs.len() {
                        regs[target_idx] = *reg;
                    }
                }
            }
        }

        self.client.write_multiple_registers(group.address, &regs).await?;
        Ok(())
    }

    /// Encodes and writes a value to a named schema field on the device.
    ///
    /// Supports dot-notation paths (e.g. `"metrics.voltage"`).
    /// Dispatched with [`Priority::Immediate`](crate::transport::queue::Priority::Immediate).
    ///
    /// # Errors
    ///
    /// - Returns [`ModbusError::InvalidFrame`] if `name` is not found, does not match the field type,
    ///   or targets a read-only table (Input or Discrete).
    pub async fn write_field(&self, name: &str, value: FieldValue) -> Result<()> {
        if let Some((group_name, subfield_name)) = name.split_once('.') {
            let mut map = HashMap::new();
            map.insert(subfield_name.to_string(), value);
            return self.write_group(group_name, &map).await;
        }

        let field = self.schema.fields.get(name).ok_or_else(|| {
            ModbusError::InvalidFrame(format!("Field '{}' not found in schema", name))
        })?;
        match field.table {
            TableKind::Holding => {
                let regs = encode_single_field(field, &value)?;
                if regs.len() == 1 {
                    self.client
                        .write_single_register(field.address, regs[0])
                        .await?;
                } else {
                    self.client
                        .write_multiple_registers(field.address, &regs)
                        .await?;
                }
                Ok(())
            }
            TableKind::Coil => {
                let b = match value {
                    FieldValue::Bool(b) => b,
                    _ => {
                        return Err(ModbusError::InvalidFrame(
                            "Expected Bool value for Coil".into(),
                        ))
                    }
                };
                self.client.write_single_coil(field.address, b).await?;
                Ok(())
            }
            TableKind::Input | TableKind::Discrete => Err(ModbusError::InvalidFrame(
                "Cannot write to read-only table (Input/Discrete)".into(),
            )),
        }
    }
}

fn decode_single_field(field: &SchemaField, data: &RegisterData) -> Result<FieldValue> {
    match &field.field_type {
        FieldType::U16 => Ok(FieldValue::U16(data.get_u16(0)?)),
        FieldType::I16 => Ok(FieldValue::I16(data.get_i16(0)?)),
        FieldType::U32 => Ok(FieldValue::U32(data.get_u32(0, field.word_order)?)),
        FieldType::I32 => Ok(FieldValue::I32(data.get_i32(0, field.word_order)?)),
        FieldType::F32 => Ok(FieldValue::F32(data.get_f32(0, field.word_order)?)),
        FieldType::F64 => Ok(FieldValue::F64(data.get_f64(0, field.word_order)?)),
        FieldType::String(len) => Ok(FieldValue::String(data.get_string(0, *len)?)),
        FieldType::Bool => {
            let raw = data.get_u16(0)?;
            if let Some(b) = field.bit {
                Ok(FieldValue::Bool(((raw >> b) & 1) != 0))
            } else {
                Ok(FieldValue::Bool(raw != 0))
            }
        }
        FieldType::Bitmask(bits) => {
            let raw = data.get_u16(0)?;
            let mut map = HashMap::new();
            for (flag, &pos) in bits {
                map.insert(flag.clone(), ((raw >> pos) & 1) != 0);
            }
            Ok(FieldValue::Bitmask(map))
        }
    }
}

fn encode_subfield(
    field_type: &FieldType,
    bit: Option<u8>,
    value: &FieldValue,
    word_order: crate::core::register_data::WordOrder,
) -> Result<Vec<u16>> {
    match (field_type, value) {
        (FieldType::U16, FieldValue::U16(v)) => Ok(vec![*v]),
        (FieldType::I16, FieldValue::I16(v)) => Ok(vec![*v as u16]),
        (FieldType::U32, FieldValue::U32(v)) => {
            Ok(RegisterData::u32_to_registers(*v, word_order).to_vec())
        }
        (FieldType::I32, FieldValue::I32(v)) => {
            Ok(RegisterData::i32_to_registers(*v, word_order).to_vec())
        }
        (FieldType::F32, FieldValue::F32(v)) => {
            Ok(RegisterData::f32_to_registers(*v, word_order).to_vec())
        }
        (FieldType::F64, FieldValue::F64(v)) => {
            Ok(RegisterData::f64_to_registers(*v, word_order).to_vec())
        }
        (FieldType::String(_), FieldValue::String(s)) => {
            Ok(RegisterData::string_to_registers(s))
        }
        (FieldType::Bool, FieldValue::Bool(b)) => {
            if let Some(b_pos) = bit {
                Ok(vec![if *b { 1 << b_pos } else { 0 }])
            } else {
                Ok(vec![if *b { 1 } else { 0 }])
            }
        }
        (FieldType::Bitmask(bits), FieldValue::Bitmask(map)) => {
            let mut raw = 0u16;
            for (flag, &pos) in bits {
                if let Some(&true) = map.get(flag) {
                    raw |= 1 << pos;
                }
            }
            Ok(vec![raw])
        }
        _ => Err(ModbusError::InvalidFrame(
            "FieldValue type does not match SchemaField type".into(),
        )),
    }
}

fn encode_single_field(field: &SchemaField, value: &FieldValue) -> Result<Vec<u16>> {
    encode_subfield(&field.field_type, field.bit, value, field.word_order)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::time::Duration;
    use crate::core::client::ClientOptions;
    use crate::core::crc::crc16;
    use crate::core::framing::FramingKind;
    use crate::core::register_data::WordOrder;
    use crate::schema::types::SchemaGroup;
    use crate::transport::queue::PortQueueHandle;
    use crate::transport::traits::Transport;

    struct MockSchemaTransport;

    #[async_trait]
    impl Transport for MockSchemaTransport {
        async fn exchange(&mut self, _request: &[u8], _timeout: Duration) -> Result<Vec<u8>> {
            let v_regs = RegisterData::f32_to_registers(220.0, WordOrder::BigEndian);
            let c_regs = RegisterData::f32_to_registers(10.0, WordOrder::BigEndian);
            let mut resp = vec![0x01, 0x03, 0x08]; // Slave: 1, FC: 3, 8 bytes data
            for r in [v_regs[0], v_regs[1], c_regs[0], c_regs[1]] {
                resp.extend_from_slice(&r.to_be_bytes());
            }
            let crc = crc16(&resp);
            resp.push((crc & 0xFF) as u8);
            resp.push((crc >> 8) as u8);
            Ok(resp)
        }
        async fn flush(&mut self) -> Result<()> { Ok(()) }
        async fn close(&mut self) -> Result<()> { Ok(()) }
        fn is_open(&self) -> bool { true }
    }

    #[tokio::test]
    async fn test_bound_schema_group_read() {
        let queue = PortQueueHandle::new(MockSchemaTransport, 10);
        let client = ModbusClient::new(1, FramingKind::Rtu, queue, ClientOptions::default()).unwrap();
        let schema = DeviceSchema::new(WordOrder::BigEndian).add_group(
            SchemaGroup::new("power", 100, TableKind::Holding, WordOrder::BigEndian)
                .add_subfield("voltage", 0, FieldType::F32)
                .add_subfield("current", 2, FieldType::F32),
        );
        let bound = BoundSchema::new(client, schema);
        let values = bound.read_group("power").await.unwrap();
        let v = values.get("voltage").unwrap().as_f32().unwrap();
        let c = values.get("current").unwrap().as_f32().unwrap();
        assert!((v - 220.0).abs() < 1e-4);
        assert!((c - 10.0).abs() < 1e-4);

        // Test dot notation access
        let v_dot = bound.read_field("power.voltage").await.unwrap();
        assert!((v_dot.as_f32().unwrap() - 220.0).abs() < 1e-4);
    }

    #[tokio::test]
    async fn test_schema_bitmask_and_bit_field() {
        let mut bits = HashMap::new();
        bits.insert("running".to_string(), 0);
        bits.insert("alarm".to_string(), 2);

        let field = SchemaField {
            name: "status".to_string(),
            address: 10,
            table: TableKind::Holding,
            field_type: FieldType::Bitmask(bits.clone()),
            word_order: WordOrder::BigEndian,
            bit: None,
        };

        // Raw register 0b0000_0101 = 5 (running=true, alarm=true)
        let data = RegisterData::new(vec![5]);
        let decoded = decode_single_field(&field, &data).unwrap();
        let mask = decoded.as_bitmask().unwrap();
        assert_eq!(mask.get("running"), Some(&true));
        assert_eq!(mask.get("alarm"), Some(&true));

        let encoded = encode_single_field(&field, &decoded).unwrap();
        assert_eq!(encoded, vec![5]);
    }
}
