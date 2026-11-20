//! Physical communication mode standarts (RS-485, RS-232, TCP/IP)

use crate::core::framing::FramingKind;

/// Physical bus standard or network mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RsMode {
    /// RS-485 multidrop bug (suppors up to 247 slave devices, RTU framing).
    #[default]
    Rs485,
    /// RS-232 point-to-point serial line (strictly limited to 1 slave device, RTU framing).
    Rs232,
    /// Modbus TCP/IP Ethernet connection (MBAP framing).
    Tcp,
}

impl RsMode {
    /// Returns default framing associated with this physical mode.
    pub fn default_framing(&self) -> FramingKind {
        match self {
            Self::Rs485 | Self::Rs232 => FramingKind::Rtu,
            Self::Tcp => FramingKind::Tcp,
        }
    }

    /// Maximum number of devices allowed on a single port for this mode.
    /// `Rs232` allows at most 1; `Rs485` and `Tcp` allow multiple.
    pub fn max_devices(&self) -> Option<usize> {
        match self {
            Self::Rs232 => Some(1),
            Self::Rs485 | Self::Tcp => None,
        }
    }
}