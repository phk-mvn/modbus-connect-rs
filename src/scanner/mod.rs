//! Device discovery and network scanning for Modbus RTU lines and TCP subnets.
//!
//! [`ModbusScanner`] probes target slave addresses or IP hosts with standard diagnostic queries
//! (e.g., reading holding register 0) to detect responding devices and measure roundtrip latencies.

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

use crate::core::framing::{RtuFramer, TcpFramer};
use crate::core::pdu::{Request, Response};
use crate::error::ModbusError;
use crate::transport::queue::{PortQueueHandle, Priority};

#[cfg(feature = "tcp")]
use crate::transport::tcp::TcpTransport;
#[cfg(feature = "tcp")]
use crate::transport::traits::Transport;

/// Predefined scanning parameter presets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ScanProfile {
    /// Standard baud rates (9600..115200), shorter padding (5ms).
    #[default]
    Quick,
    /// Broad sweep covering low baud rates down to 1200, padding (10ms).
    Deep,
    /// Custom configuration.
    Custom,
}

impl ScanProfile {
    /// Returns default baud rates for this profile.
    pub fn default_baud_rates(&self) -> Vec<u32> {
        match self {
            ScanProfile::Quick => vec![115200, 57600, 38400, 19200, 9600],
            ScanProfile::Deep => vec![115200, 57600, 38400, 19200, 9600, 4800, 2400, 1200],
            ScanProfile::Custom => vec![9600],
        }
    }

    /// Returns recommended padding in milliseconds.
    pub fn default_padding_ms(&self) -> u32 {
        match self {
            ScanProfile::Quick => 5,
            ScanProfile::Deep => 10,
            ScanProfile::Custom => 5,
        }
    }
}

/// Calculates the optimal serial RTU response timeout using the standard formula:
/// `timeout = ceil(264000 / baud + padding_ms)`.
pub fn calculate_adaptive_timeout(baud_rate: u32, padding_ms: u32) -> Duration {
    if baud_rate == 0 {
        return Duration::from_millis(100);
    }
    let ms = ((264_000.0 / baud_rate as f64) + padding_ms as f64).ceil() as u64;
    Duration::from_millis(ms.max(5))
}

/// Scan sweep statistics summary.
#[derive(Debug, Clone, Default)]
pub struct ScanStats {
    /// Number of total addresses/hosts probed.
    pub total_probed: usize,
    /// Number of responsive devices discovered.
    pub discovered_count: usize,
    /// Total duration elapsed during the scan.
    pub duration: Duration,
}

/// Information about a Modbus device discovered during a bus or network scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredDevice {
    /// Human-readable target description (e.g., `"RTU Slave 5"` or `"192.168.1.50:502 (Unit 1)"`).
    pub target: String,
    /// Responding Modbus slave/unit ID.
    pub slave_id: u8,
    /// Measured communication response latency.
    pub latency: Duration,
}

/// Aggregate report produced by a scan sweep.
#[derive(Debug, Clone, Default)]
pub struct ScanReport {
    /// List of all discovered devices that answered probe requests.
    pub devices: Vec<DiscoveredDevice>,
    /// Total duration elapsed during the scan.
    pub duration: Duration,
    /// Comprehensive statistics for the scan run.
    pub stats: ScanStats,
}

/// Scanner utility providing RTU address sweeps and concurrent TCP subnet discovery.
pub struct ModbusScanner;

impl ModbusScanner {
    /// Scans a range of slave IDs on an RTU transport line.
    ///
    /// Probes each slave ID with a benign read holding registers request (FC 03, address 0, quantity 1).
    /// Both valid register data responses and standard Modbus exception responses
    /// (e.g. `IllegalDataAddress`) confirm the presence of a responsive device on that address.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use std::time::Duration;
    /// use modbus_connect::scanner::ModbusScanner;
    /// use modbus_connect::transport::queue::PortQueueHandle;
    ///
    /// # async fn doc(queue: PortQueueHandle) {
    /// let report = ModbusScanner::scan_rtu(&queue, 1..=10, Duration::from_millis(50)).await;
    /// for dev in report.devices {
    ///     println!("Found device at slave ID {} (latency: {:?})", dev.slave_id, dev.latency);
    /// }
    /// # }
    /// ```
    pub async fn scan_rtu(
        queue: &PortQueueHandle,
        slave_ids: std::ops::RangeInclusive<u8>,
        timeout_per_device: Duration,
    ) -> ScanReport {
        let start_time = Instant::now();
        let mut devices = Vec::new();

        let probe_request = Request::ReadHoldingRegisters {
            address: 0,
            quantity: 1,
        };

        let mut probed = 0;
        for slave_id in slave_ids {
            probed += 1;
            let frame = RtuFramer::build_frame(slave_id, &probe_request);
            let send_start = Instant::now();

            let res = queue
                .send_frame(frame, Priority::Immediate, timeout_per_device)
                .await;

            if let Ok(response_bytes) = res {
                let parsed = RtuFramer::parse_response(slave_id, &probe_request, &response_bytes);
                let latency = send_start.elapsed();

                match parsed {
                    Ok(Response::ReadHoldingRegisters(_)) => {
                        devices.push(DiscoveredDevice {
                            target: format!("RTU Slave {}", slave_id),
                            slave_id,
                            latency,
                        });
                    }
                    Err(ModbusError::Exception(_)) => {
                        devices.push(DiscoveredDevice {
                            target: format!("RTU Slave {}", slave_id),
                            slave_id,
                            latency,
                        });
                    }
                    _ => {}
                }
            }
        }

        let duration = start_time.elapsed();
        let discovered_count = devices.len();
        ScanReport {
            devices,
            duration,
            stats: ScanStats {
                total_probed: probed,
                discovered_count,
                duration,
            },
        }
    }

    /// Performs an adaptive scan across slave IDs using a [`ScanProfile`] and computed timeouts.
    pub async fn scan_rtu_profile(
        queue: &PortQueueHandle,
        slave_ids: std::ops::RangeInclusive<u8>,
        baud_rate: u32,
        profile: ScanProfile,
        on_progress: Option<Arc<dyn Fn(u8, usize, usize) + Send + Sync>>,
    ) -> ScanReport {
        let timeout = calculate_adaptive_timeout(baud_rate, profile.default_padding_ms());
        let total = slave_ids.clone().count();
        let start_time = Instant::now();
        let mut devices = Vec::new();
        let mut probed = 0;

        let probe_request = Request::ReadHoldingRegisters {
            address: 0,
            quantity: 1,
        };

        for slave_id in slave_ids {
            probed += 1;
            if let Some(ref cb) = on_progress {
                cb(slave_id, probed, total);
            }
            let frame = RtuFramer::build_frame(slave_id, &probe_request);
            let send_start = Instant::now();

            let res = queue.send_frame(frame, Priority::Immediate, timeout).await;

            if let Ok(response_bytes) = res {
                let parsed = RtuFramer::parse_response(slave_id, &probe_request, &response_bytes);
                let latency = send_start.elapsed();

                match parsed {
                    Ok(Response::ReadHoldingRegisters(_)) | Err(ModbusError::Exception(_)) => {
                        devices.push(DiscoveredDevice {
                            target: format!("RTU Slave {}", slave_id),
                            slave_id,
                            latency,
                        });
                    }
                    _ => {}
                }
            }
        }

        let duration = start_time.elapsed();
        let discovered_count = devices.len();
        ScanReport {
            devices,
            duration,
            stats: ScanStats {
                total_probed: probed,
                discovered_count,
                duration,
            },
        }
    }

    /// Concurrently scans a list of IP hosts on Modbus TCP port 502 (or custom port).
    ///
    /// Limits concurrent connections using an asynchronous [`Semaphore`].
    ///
    /// Requires the `tcp` crate feature.
    #[cfg(feature = "tcp")]
    pub async fn scan_tcp(
        hosts: Vec<String>,
        port: u16,
        unit_id: u8,
        timeout: Duration,
        max_concurrency: usize,
    ) -> ScanReport {
        let start_time = Instant::now();
        let semaphore = Arc::new(Semaphore::new(max_concurrency));
        let mut tasks = Vec::new();

        for host in hosts {
            let sem = semaphore.clone();
            let target_addr = format!("{}:{}", host, port);

            tasks.push(tokio::spawn(async move {
                let _permit = sem.acquire().await.unwrap();
                let send_start = Instant::now();

                if let Ok(mut transport) = TcpTransport::connect(&target_addr).await {
                    let probe = Request::ReadHoldingRegisters {
                        address: 0,
                        quantity: 1,
                    };
                    let frame = TcpFramer::build_frame(1, unit_id, &probe);

                    if let Ok(raw_res) = transport.exchange(&frame, timeout).await {
                        if TcpFramer::parse_response(1, unit_id, &probe, &raw_res).is_ok() {
                            return Some(DiscoveredDevice {
                                target: format!("{} (Unit {})", target_addr, unit_id),
                                slave_id: unit_id,
                                latency: send_start.elapsed(),
                            });
                        }
                    }
                }
                None
            }));
        }

        let total_probed = tasks.len();
        let mut devices = Vec::new();
        for task in tasks {
            if let Ok(Some(device)) = task.await {
                devices.push(device);
            }
        }

        let duration = start_time.elapsed();
        let discovered_count = devices.len();
        ScanReport {
            devices,
            duration,
            stats: ScanStats {
                total_probed,
                discovered_count,
                duration,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use crate::core::crc::crc16;
    use crate::error::Result;

    struct MockScanTransport;

    #[async_trait]
    impl crate::transport::traits::Transport for MockScanTransport {
        async fn exchange(&mut self, request: &[u8], _timeout: Duration) -> Result<Vec<u8>> {
            if request.is_empty() {
                return Err(ModbusError::BufferUnderrun { expected: 1, actual: 0 });
            }
            let slave_id = request[0];
            if slave_id == 5 {
                let mut resp = vec![0x05, 0x03, 0x02, 0x00, 0x2A];
                let crc = crc16(&resp);
                resp.push((crc & 0xFF) as u8);
                resp.push((crc >> 8) as u8);
                Ok(resp)
            } else {
                Err(ModbusError::Timeout(Duration::from_millis(10)))
            }
        }
        async fn flush(&mut self) -> Result<()> { Ok(()) }
        async fn close(&mut self) -> Result<()> { Ok(()) }
        fn is_open(&self) -> bool { true }
    }

    #[tokio::test]
    async fn test_rtu_scanner_finds_slave_5() {
        let queue = PortQueueHandle::new(MockScanTransport, 10);
        let report = ModbusScanner::scan_rtu(&queue, 1..=7, Duration::from_millis(50)).await;
        assert_eq!(report.devices.len(), 1);
        assert_eq!(report.devices[0].slave_id, 5);
        assert_eq!(report.devices[0].target, "RTU Slave 5");
        assert_eq!(report.stats.discovered_count, 1);
        assert_eq!(report.stats.total_probed, 7);

        // Test adaptive timeout formula: 264000 / 115200 = 2.29ms + 5ms = 8ms
        let timeout = calculate_adaptive_timeout(115200, 5);
        assert_eq!(timeout.as_millis(), 8);

        // Test scan_rtu_profile
        let report_profile = ModbusScanner::scan_rtu_profile(
            &queue,
            4..=6,
            115200,
            ScanProfile::Quick,
            None,
        ).await;
        assert_eq!(report_profile.devices.len(), 1);
        assert_eq!(report_profile.devices[0].slave_id, 5);
    }
}
