//! Background telemetry polling, cyclic reading routines, and event notification streams.
//!
//! Provides [`PollingManager`] and [`PollingTask`] mirroring the TypeScript `modbus-connect` polling subsystem:
//! - Periodic data fetching with configurable intervals.
//! - Automatic error backoff when communication fails.
//! - Per-task `.on_data` and `.on_error` callbacks.
//! - Central broadcast channel for [`PollingEvent`] streaming.

pub mod manager;

pub use manager::{PollAction, PollingEvent, PollingManager, PollingTask, RetryMode};
