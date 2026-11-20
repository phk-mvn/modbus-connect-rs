//! Declarative device schema and strongly-typed field mapping subsystem.
//!
//! Maps Modbus addresses, table types, byte orders, and data types to named fields
//! and contiguous groups, enabling high-level symbolic reads and writes.
//!
//! - [`types::DeviceSchema`]: Declarative model containing field and group specifications.
//! - [`bound::BoundSchema`]: Schema instance bound to a [`ModbusClient`](crate::core::ModbusClient).

pub mod bound;
pub mod types;

pub use bound::BoundSchema;
pub use types::{
    DeviceSchema, FieldType, FieldValue, GroupSubfield, SchemaField, SchemaGroup, TableKind,
};
