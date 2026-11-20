//! Priority-based actor queue for serialized Modbus port access.
//!
//! In RS-485 serial multidrop networks (and single-socket TCP connections), only one
//! request-response transaction can take place at any given instant.
//!
//! [`PortQueueHandle`] manages an asynchronous actor that serializes all requests,
//! scheduling them according to their [`Priority`]:
//! - [`Priority::Immediate`]: High-priority operations like critical single/multiple coil and register writes.
//! - [`Priority::Normal`]: Standard user reads and transactions.
//! - [`Priority::Polling`]: Periodic background telemetry updates.
//!
//! Additionally, the queue can be paused (e.g., during scanner address sweeps) and resumed,
//! and automatically taps traffic frames into a [`TrafficSniffer`] if attached.

use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use crate::error::{ModbusError, Result};
use crate::transport::traits::Transport;
use crate::trackers::sniffer::{Direction, TrafficSniffer};

/// Execution priority for requests dispatched to a [`PortQueueHandle`].
///
/// Lower numerical discriminant represents higher execution priority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
    /// Immediate priority. Handled before Normal and Polling requests (e.g., emergency stops, urgent control writes).
    Immediate = 0,
    /// Normal priority. Standard synchronous read/write requests from applications.
    Normal = 1,
    /// Polling priority. Background periodic telemetry requests that yield to user operations.
    Polling = 2,
}

/// Internal container representing a scheduled frame exchange job.
struct PortJob {
    priority: Priority,
    frame: Vec<u8>,
    timeout: Duration,
    ignore_no_response: bool,
    respond_to: oneshot::Sender<Result<Vec<u8>>>,
}

/// Actor control commands dispatched through the queue channel.
enum PortCommand {
    Execute(PortJob),
    Pause(oneshot::Sender<()>),
    Resume,
    Close,
}

/// Thread-safe cloneable handle to the background port queue actor.
///
/// All clones dispatch to the same underlying communication channel and actor task.
#[derive(Clone)]
pub struct PortQueueHandle {
    sender: mpsc::Sender<PortCommand>,
}

impl PortQueueHandle {
    /// Spawns a new queue actor wrapping the provided [`Transport`] with the specified buffer capacity.
    ///
    /// # Example
    ///
    /// ```
    /// use modbus_connect::transport::queue::PortQueueHandle;
    /// use modbus_connect::transport::emulator::SlaveEmulator;
    /// use modbus_connect::core::framing::FramingKind;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let emulator = SlaveEmulator::new(1, FramingKind::Rtu);
    /// let queue = PortQueueHandle::new(emulator, 50);
    /// # }
    /// ```
    pub fn new<T: Transport + 'static>(transport: T, queue_capacity: usize) -> Self {
        Self::with_sniffer(transport, queue_capacity, None)
    }

    /// Spawns a new queue actor with an optional [`TrafficSniffer`] attached to record all inbound and outbound frames.
    pub fn with_sniffer<T: Transport + 'static>(
        transport: T,
        queue_capacity: usize,
        sniffer: Option<TrafficSniffer>,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(queue_capacity);
        let actor = PortQueueActor::new(Box::new(transport), receiver, sniffer);
        tokio::spawn(actor.run());
        Self { sender }
    }

    /// Submits a raw frame for transmission, returning the raw response frame upon completion.
    ///
    /// Jobs are sorted and executed according to their [`Priority`].
    ///
    /// # Errors
    ///
    /// - Returns [`ModbusError::ScanActive`] if the queue is currently paused (e.g., due to an active bus scan).
    /// - Returns [`ModbusError::Timeout`] if the slave does not respond within `timeout`.
    /// - Returns [`ModbusError::NotConnected`] if the background actor has terminated.
    pub async fn send_frame(&self, frame: Vec<u8>, priority: Priority, timeout: Duration) -> Result<Vec<u8>> {
        self.send_frame_opt(frame, priority, timeout, false).await
    }

    /// Submits a raw frame for transmission, with optional `ignore_no_response` (fire-and-forget).
    pub async fn send_frame_opt(
        &self,
        frame: Vec<u8>,
        priority: Priority,
        timeout: Duration,
        ignore_no_response: bool,
    ) -> Result<Vec<u8>> {
        let (respond_to, response_rx) = oneshot::channel();
        let job = PortJob {
            priority,
            frame,
            timeout,
            ignore_no_response,
            respond_to,
        };

        self.sender
            .send(PortCommand::Execute(job))
            .await
            .map_err(|_| ModbusError::NotConnected("PortQueue actor is terminated".into()))?;

        response_rx
            .await
            .map_err(|_| ModbusError::NotConnected("Response channel dropped by actor".into()))?
    }

    /// Pauses the port queue.
    ///
    /// When paused, all new normal transactions fail immediately with [`ModbusError::ScanActive`].
    /// This is typically used by network scanners to take exclusive temporary ownership of the line.
    pub async fn pause(&self) -> Result<()> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.sender
            .send(PortCommand::Pause(ack_tx))
            .await
            .map_err(|_| ModbusError::NotConnected("PortQueue actor is terminated".into()))?;
        let _ = ack_rx.await;
        Ok(())
    }

    /// Resumes normal queue processing after a previous [`pause`](Self::pause) call.
    pub async fn resume(&self) -> Result<()> {
        self.sender
            .send(PortCommand::Resume)
            .await
            .map_err(|_| ModbusError::NotConnected("PortQueue actor is terminated".into()))?;
        Ok(())
    }

    /// Signals the queue actor to close the transport and terminate the background task.
    pub async fn close(&self) -> Result<()> {
        let _ = self.sender.send(PortCommand::Close).await;
        Ok(())
    }
}

/// Internal actor running on a spawned Tokio task managing the transport loop.
struct PortQueueActor {
    transport: Box<dyn Transport>,
    receiver: mpsc::Receiver<PortCommand>,
    pending_jobs: Vec<PortJob>,
    is_paused: bool,
    sniffer: Option<TrafficSniffer>,
}

impl PortQueueActor {
    fn new(
        transport: Box<dyn Transport>,
        receiver: mpsc::Receiver<PortCommand>,
        sniffer: Option<TrafficSniffer>,
    ) -> Self {
        Self {
            transport,
            receiver,
            pending_jobs: Vec::new(),
            is_paused: false,
            sniffer,
        }
    }

    async fn run(mut self) {
        loop {
            if !self.is_paused && !self.pending_jobs.is_empty() {
                self.pending_jobs.sort_by_key(|j| j.priority);
                let job = self.pending_jobs.remove(0);

                if let Some(sniffer) = &self.sniffer {
                    sniffer.record(Direction::Tx, &job.frame);
                }

                let result = if job.ignore_no_response {
                    self.transport.write_frame(&job.frame).await.map(|_| Vec::new())
                } else {
                    let res = self.transport.exchange(&job.frame, job.timeout).await;
                    if let Ok(response_bytes) = &res {
                        if let Some(sniffer) = &self.sniffer {
                            sniffer.record(Direction::Rx, response_bytes);
                        }
                    }
                    res
                };

                let _ = job.respond_to.send(result);
                continue;
            }

            tokio::select! {
                command = self.receiver.recv() => {
                    match command {
                        Some(PortCommand::Execute(job)) => {
                            if self.is_paused {
                                let _ = job.respond_to.send(Err(ModbusError::ScanActive));
                            } else {
                                self.pending_jobs.push(job);
                            }
                        }
                        Some(PortCommand::Pause(ack)) => {
                            self.is_paused = true;
                            let _ = ack.send(());
                        }
                        Some(PortCommand::Resume) => {
                            self.is_paused = false;
                        }
                        Some(PortCommand::Close) | None => {
                            let _ = self.transport.close().await;
                            break;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    struct MockEchoTransport {
        open: bool,
    }

    #[async_trait]
    impl Transport for MockEchoTransport {
        async fn exchange(&mut self, request_frame: &[u8], _timeout: Duration) -> Result<Vec<u8>> {
            Ok(request_frame.to_vec())
        }
        async fn flush(&mut self) -> Result<()> {
            Ok(())
        }
        async fn close(&mut self) -> Result<()> {
            self.open = false;
            Ok(())
        }
        fn is_open(&self) -> bool {
            self.open
        }
    }

    #[tokio::test]
    async fn test_port_queue_exchange() {
        let transport = MockEchoTransport { open: true };
        let queue = PortQueueHandle::new(transport, 100);
        let data = vec![1, 2, 3, 4];
        let res = queue
            .send_frame(data.clone(), Priority::Normal, Duration::from_millis(500))
            .await
            .unwrap();
        assert_eq!(res, data);
        queue.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_port_queue_pause() {
        let transport = MockEchoTransport { open: true };
        let queue = PortQueueHandle::new(transport, 100);
        queue.pause().await.unwrap();
        let res = queue
            .send_frame(vec![1, 2], Priority::Normal, Duration::from_millis(100))
            .await;
        assert!(matches!(res, Err(ModbusError::ScanActive)));
        queue.resume().await.unwrap();
        let res = queue
            .send_frame(vec![1, 2], Priority::Normal, Duration::from_millis(100))
            .await
            .unwrap();
        assert_eq!(res, vec![1, 2]);
        queue.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_port_queue_sniffer_integration() {
        let sniffer = TrafficSniffer::new(10);
        let mut sub = sniffer.subscribe();

        let transport = MockEchoTransport { open: true };
        let queue = PortQueueHandle::with_sniffer(transport, 10, Some(sniffer));

        let data = vec![0x05, 0x03, 0x00, 0x00];
        let _ = queue
            .send_frame(data.clone(), Priority::Normal, Duration::from_millis(500))
            .await
            .unwrap();

        let tx = sub.recv().await.unwrap();
        assert_eq!(tx.direction, Direction::Tx);
        assert_eq!(tx.data, data);

        let rx = sub.recv().await.unwrap();
        assert_eq!(rx.direction, Direction::Rx);
        assert_eq!(rx.data, data);
    }
}
