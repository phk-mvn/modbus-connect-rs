//! Network diagnostic trackers, connection monitoring, and traffic inspection.
//!
//! - [`DeviceConnectionTracker`]: Tracks online/offline status per slave device with debouncing.
//! - [`PortConnectionTracker`]: Tracks connection status of the physical line/port.
//! - [`TrafficSniffer`]: Non-invasive raw frame capture and inspection.
pub mod device_tracker;
pub mod port_tracker;
pub mod sniffer;
pub mod types;
pub use device_tracker::{DeviceConnectionTracker, DeviceStateHandler};
pub use port_tracker::{PortConnectionTracker, PortStateHandler};
pub use sniffer::{Direction, SniffedFrame, TrafficSniffer};
pub use types::{
    ConnectionErrorInfo, ConnectionErrorType, DeviceConnectionState, PortConnectionState,
};