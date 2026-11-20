//! Client roster and inventory management for Modbus devices

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::core::ModbusClient;
use crate::error::{ModbusError, Result};

/// Manager client entry in the controller registry.
#[derive(Clone)]
pub struct RegisteredClient {
    /// Unique client identifier (e.g., "flow-meter-1").
    pub client_id: String,
    /// Target Modbus slave ID.
    pub slave_id: u8,
    /// Identifier of the transport this client is assigned to.
    pub transport_id: String,
    /// Active client instance.
    pub client: ModbusClient,
}

/// Thread-safe registry coordinating clients across all controller transports.
#[derive(Clone, Default)]
pub struct ClientRegistry {
    clients: Arc<RwLock<HashMap<String, RegisteredClient>>>,
}

impl ClientRegistry {
    /// Creates an empty client registry.
    pub fn new() -> Self {
        Self {
            clients: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Registers a client, verifying uniqueness of `client_id` and checking for duplicate `slave_id`.
    pub async fn register(
        &self,
        client_id: impl Into<String>,
        slave_id: u8,
        transport_id: impl Into<String>,
        client: ModbusClient,
        allow_duplicate_slave: bool,
    ) -> Result<()> {
        let client_id = client_id.into();
        let transport_id = transport_id.into();
        let mut map = self.clients.write().await;

        if map.contains_key(&client_id) {
            return Err(ModbusError::ClientAlreadyExists(client_id));
        }

        if !allow_duplicate_slave {
            for existing in map.values() {
                if existing.transport_id == transport_id && existing.slave_id == slave_id {
                    return Err(ModbusError::DuplicateSlaveId(slave_id, transport_id));
                }
            }
        }

        map.insert(
            client_id.clone(),
            RegisteredClient {
                client_id,
                slave_id,
                transport_id,
                client,
            },
        );

        Ok(())
    }

    /// Retrieves a client by its unique client ID.
    pub async fn get(&self, client_id: &str) -> Option<ModbusClient> {
        let map = self.clients.read().await;
        map.get(client_id).map(|r| r.client.clone())
    }

    /// Retrieves a registered client record by its unique client ID.
    pub async fn get_entry(&self, client_id: &str) -> Option<RegisteredClient> {
        let map = self.clients.read().await;
        map.get(client_id).cloned()
    }

    /// Lists all registered clients, optionally filtered by `transport_id`
    pub async fn list(&self, transport_id: Option<&str>) -> Vec<RegisteredClient> {
        let map = self.clients.read().await;
        map.values()
            .filter(|r| match transport_id {
                Some(tid) => r.transport_id == tid,
                None => true,
            })
            .cloned()
            .collect()
    }

    /// Removes a client from the registry by its client ID.
    pub async fn remove(&self, client_id: &str) -> Option<RegisteredClient> {
        let mut map = self.clients.write().await;
        map.remove(client_id)
    }

    /// Removes all clients belonging to a specific transport.
    pub async fn remove_by_transport(&self, transport_id: &str) -> Vec<RegisteredClient> {
        let mut map = self.clients.write().await;
        let mut removed = Vec::new();
        let keys_to_remove: Vec<String> = map
            .values()
            .filter(|r| r.transport_id == transport_id)
            .map(|r| r.client_id.clone())
            .collect();

        for key in keys_to_remove {
            if let Some(r) = map.remove(&key) {
                removed.push(r);
            }
        }
        removed
    }

    /// Returns the total count of registered clients.
    pub async fn count(&self) -> usize {
        let map = self.clients.read().await;
        map.len()
    }
}