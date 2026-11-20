//! Plugin system for extending Modbus client with proprietary function codes.

use std::collections::HashMap;
use std::sync::Arc;
use crate::error::Result;

/// Type alias for dynamic response length resolver.
/// 
/// Returns:
/// - `Some(usize)`: Exact expected response PDU length in bytes.
/// - `None`: If more bytes are required to determine the length.
pub type ExpectedLengthResolver = Arc<dyn Fn(&[u8], &[u8]) -> Option<usize> + Send + Sync>;

/// Handler for building requests and parsing responses of a custom Modbus function code.
pub trait CustomFunctionHandler: Send + Sync {
    /// Builds the raw request PDU bytes (Function Code + Payload).
    fn build_request(&self, args: &[u8]) -> Vec<u8>;

    /// Parses the raw response PDU bytes returned by the slave.
    fn parse_response(&self, response_pdu: &[u8]) -> Result<Vec<u8>>;

    /// Optional dynamic response length predictor.
    /// 
    /// # Arguments
    /// * `partial_response_pdu` - Bytes received so far (starting with Function code).
    /// * `request_pdu` - The request PDU that was sent.
    fn get_expected_response_length(&self, _partial_response_pdu: &[u8], _request_pdu: &[u8]) -> Option<usize> {
        None
    }
}

pub trait ModbusPlugin: Send + Sync {
    /// Name of the plugin.
    fn name(&self) -> &str;

    /// Mapping of custom function name to their handlers.
    fn custom_functions(&self) -> HashMap<String, Arc<dyn CustomFunctionHandler>>;
}