//! Background telemetry polling manager and task definitions.
//!
//! Handles periodic execution of Modbus reads with automatic backoff, per-task callbacks,
//! dynamic interval changes, and multi-subscriber event broadcasting.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, oneshot, RwLock};
use tokio::time::MissedTickBehavior;

use crate::core::register_data::RegisterData;
use crate::error::{ModbusError, Result};

/// Type alias for an asynchronous polling action returning [`RegisterData`].
pub type TaskAction = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<RegisterData>> + Send>> + Send + Sync>;

/// Callback invoked whenever a polling cycle successfully produces [`RegisterData`].
pub type DataCallback = Arc<dyn Fn(RegisterData) + Send + Sync>;

/// Callback invoked whenever an error occurs during a polling cycle.
pub type ErrorCallback = Arc<dyn Fn(&ModbusError) + Send + Sync>;

/// Callback invoked when a polling retry occurs.
/// Arguments: `(&ModbusError, action_index, retry_attempt)`.
pub type RetryCallback = Arc<dyn Fn(&ModbusError, usize, usize) + Send + Sync>;

/// Callback invoked when a task transitions between healthy and unhealthy.
pub type StateChangeCallback = Arc<dyn Fn(bool) + Send + Sync>;

/// Predicate deciding whether the polling cycle should run.
pub type ShouldRunPredicate = Arc<dyn Fn() -> bool + Send + Sync>;

/// Backoff and retry mode for polling task failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RetryMode {
    /// Retry interval increases linearly: backoff * attempt.
    #[default]
    Linear,
    /// Retry interval increases exponentially: backoff * 2^(attempt - 1).
    Exponential,
    /// No backoff delay between retries.
    None,
}

/// Configurable definition of a periodic polling task.
///
/// Designed with a fluent builder pattern matching the TypeScript `PollingTask` API.
///
/// # Example
///
/// ```
/// use std::time::Duration;
/// use modbus_connect::polling::PollingTask;
/// use modbus_connect::core::register_data::RegisterData;
///
/// let task = PollingTask::new("temperature_sensors", Duration::from_millis(500), || async {
///     // Periodic action, e.g. client.read_holding_registers(0, 2).await
///     Ok(RegisterData::new(vec![250, 480]))
/// })
/// .on_data(|data| {
///     println!("Got telemetry: temp={}, humidity={}", data[0], data[1]);
/// })
/// .on_error(|err| {
///     eprintln!("Polling error: {}", err);
/// })
/// .priority(5);
///
/// assert_eq!(task.id, "temperature_sensors");
/// assert_eq!(task.priority, 5);
/// ```
#[derive(Clone)]
pub struct PollingTask {
    /// Unique identifier for this task.
    pub id: String,
    /// Base interval duration between successive executions.
    pub interval: Duration,
    /// Numerical priority of the polling task (lower values represent higher priority).
    pub priority: u32,
    /// Async action executed on each poll tick.
    pub action: TaskAction,
    /// List of multiple async actions executed sequentially per poll tick.
    pub actions: Vec<TaskAction>,
    /// Optional callback invoked on successful execution.
    pub on_data: Option<DataCallback>,
    /// Optional callback invoked on execution error.
    pub on_error: Option<ErrorCallback>,
    /// Optional callback invoked when a retry attempt occurs.
    pub on_retry: Option<RetryCallback>,
    /// Optional callback invoked when healthy/unhealthy state changes.
    pub on_state_change: Option<StateChangeCallback>,
    /// Optional predicate function determining if this tick should execute.
    pub should_run: Option<ShouldRunPredicate>,
    /// Retry backoff strategy.
    pub retry_mode: RetryMode,
    /// Maximum consecutive retry attempts before extended backoff.
    pub max_retries: usize,
    /// Initial backoff delay duration when consecutive errors occur.
    pub backoff_delay: Duration,
    /// Whether to fire immediately upon registration without waiting for the first interval tick.
    pub immediate: bool,
}

impl PollingTask {
    /// Creates a new polling task with default priority (10) and standard error backoff.
    pub fn new<F, Fut>(id: impl Into<String>, interval: Duration, action: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<RegisterData>> + Send + 'static,
    {
        let act: TaskAction = Arc::new(move || Box::pin(action()));
        Self {
            id: id.into(),
            interval,
            priority: 10,
            action: act.clone(),
            actions: vec![act],
            on_data: None,
            on_error: None,
            on_retry: None,
            on_state_change: None,
            should_run: None,
            retry_mode: RetryMode::Linear,
            max_retries: 3,
            backoff_delay: Duration::from_millis(1000),
            immediate: false,
        }
    }

    /// Appends an additional async action executed sequentially in the same tick.
    pub fn add_action<F, Fut>(mut self, action: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<RegisterData>> + Send + 'static,
    {
        self.actions.push(Arc::new(move || Box::pin(action())));
        self
    }

    /// Attaches a condition deciding whether the task should execute on a tick.
    pub fn should_run<F: Fn() -> bool + Send + Sync + 'static>(mut self, pred: F) -> Self {
        self.should_run = Some(Arc::new(pred));
        self
    }

    /// Sets the retry backoff mode.
    pub fn retry_mode(mut self, mode: RetryMode) -> Self {
        self.retry_mode = mode;
        self
    }

    /// Attaches a callback invoked on each retry attempt.
    pub fn on_retry<F: Fn(&ModbusError, usize, usize) + Send + Sync + 'static>(mut self, cb: F) -> Self {
        self.on_retry = Some(Arc::new(cb));
        self
    }

    /// Attaches a callback invoked when connection state flips between healthy and failed.
    pub fn on_state_change<F: Fn(bool) + Send + Sync + 'static>(mut self, cb: F) -> Self {
        self.on_state_change = Some(Arc::new(cb));
        self
    }

    /// Attaches a success callback invoked whenever new [`RegisterData`] is obtained.
    pub fn on_data<H>(mut self, handler: H) -> Self
    where
        H: Fn(RegisterData) + Send + Sync + 'static,
    {
        self.on_data = Some(Arc::new(handler));
        self
    }

    /// Attaches an error callback invoked whenever the polling action fails.
    pub fn on_error<H>(mut self, handler: H) -> Self
    where
        H: Fn(&ModbusError) + Send + Sync + 'static,
    {
        self.on_error = Some(Arc::new(handler));
        self
    }

    /// Sets the task execution priority.
    pub fn priority(mut self, priority: u32) -> Self {
        self.priority = priority;
        self
    }

    /// Sets the maximum retry count before backoff pauses.
    pub fn max_retries(mut self, retries: usize) -> Self {
        self.max_retries = retries;
        self
    }

    /// Sets the backoff duration applied upon communication failures.
    pub fn backoff_delay(mut self, delay: Duration) -> Self {
        self.backoff_delay = delay;
        self
    }

    /// Sets whether the task should execute immediately upon startup.
    pub fn immediate(mut self, immediate: bool) -> Self {
        self.immediate = immediate;
        self
    }
}

/// Broadcast event published on polling outcomes.
#[derive(Debug, Clone)]
pub enum PollingEvent {
    /// Telemetry data successfully received.
    Data {
        /// Identifier of the task that produced the data.
        task_id: String,
        /// The parsed register data.
        data: RegisterData,
    },
    /// Communication failure during a polling attempt.
    Error {
        /// Identifier of the task that failed.
        task_id: String,
        /// Human-readable description of the error.
        error: String,
    },
}

/// Trait representing an executable polling action.
pub trait PollAction: Send + Sync + 'static {
    /// Dispatches the asynchronous polling call.
    fn call(&self) -> Pin<Box<dyn Future<Output = Result<RegisterData>> + Send>>;
}

impl<F, Fut> PollAction for F
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<RegisterData>> + Send + 'static,
{
    fn call(&self) -> Pin<Box<dyn Future<Output = Result<RegisterData>> + Send>> {
        Box::pin((self)())
    }
}

/// Internal handle managing the running Tokio task and dynamic parameters.
struct TaskHandle {
    interval: Arc<RwLock<Duration>>,
    is_paused: Arc<AtomicBool>,
    stop_tx: Option<oneshot::Sender<()>>,
    task_def: PollingTask,
}

/// Thread-safe manager coordinating concurrent polling tasks and event broadcasting.
#[derive(Clone)]
pub struct PollingManager {
    tasks: Arc<RwLock<HashMap<String, TaskHandle>>>,
    event_tx: broadcast::Sender<PollingEvent>,
    inter_task_delay: Arc<RwLock<Duration>>,
}

impl PollingManager {
    /// Creates a new polling manager with the specified broadcast event capacity.
    pub fn new(event_capacity: usize) -> Self {
        let (event_tx, _) = broadcast::channel(event_capacity);
        Self {
            tasks: Arc::new(RwLock::new(HashMap::new())),
            event_tx,
            inter_task_delay: Arc::new(RwLock::new(Duration::ZERO)),
        }
    }

    /// Sets the minimum inter-task pause delay between successive poll iterations.
    pub async fn set_inter_task_delay(&self, delay: Duration) {
        let mut d = self.inter_task_delay.write().await;
        *d = delay;
    }

    /// Returns the currently configured inter-task pause delay.
    pub async fn inter_task_delay(&self) -> Duration {
        *self.inter_task_delay.read().await
    }

    /// Spawns and registers a [`PollingTask`].
    ///
    /// # Errors
    ///
    /// Returns [`ModbusError::InvalidFrame`] if a task with the same `id` already exists.
    pub async fn add_task(&self, task: PollingTask) -> Result<()> {
        let mut tasks = self.tasks.write().await;
        let id_str = task.id.clone();

        if tasks.contains_key(&id_str) {
            return Err(ModbusError::InvalidFrame(format!(
                "Polling task with id '{}' already exists",
                id_str
            )));
        }

        let is_paused = Arc::new(AtomicBool::new(false));
        let interval_holder = Arc::new(RwLock::new(task.interval));
        let (stop_tx, mut stop_rx) = oneshot::channel();

        let task_id = id_str.clone();
        let task_is_paused = is_paused.clone();
        let task_interval_holder = interval_holder.clone();
        let event_tx = self.event_tx.clone();
        let actions = if task.actions.is_empty() {
            vec![task.action.clone()]
        } else {
            task.actions.clone()
        };
        let should_run = task.should_run.clone();
        let retry_mode = task.retry_mode;
        let on_data = task.on_data.clone();
        let on_error = task.on_error.clone();
        let on_retry = task.on_retry.clone();
        let on_state_change = task.on_state_change.clone();
        let max_retries = task.max_retries;
        let base_backoff = task.backoff_delay;
        let inter_delay_holder = self.inter_task_delay.clone();
        let base_interval = task.interval;
        let max_backoff = base_interval * 8;

        tokio::spawn(async move {
            let mut current_interval = base_interval;
            let mut ticker = tokio::time::interval(current_interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let mut is_healthy = true;

            loop {
                tokio::select! {
                    _ = &mut stop_rx => {
                        break;
                    }
                    _ = ticker.tick() => {
                        if task_is_paused.load(Ordering::Relaxed) {
                            continue;
                        }

                        if let Some(ref sr) = should_run {
                            if !sr() {
                                continue;
                            }
                        }

                        let target_interval = *task_interval_holder.read().await;

                        let mut combined_data = Vec::new();
                        let mut task_failed = false;
                        let mut last_error = None;

                        for (action_idx, act) in actions.iter().enumerate() {
                            let mut attempt = 0;
                            loop {
                                match act().await {
                                    Ok(data) => {
                                        combined_data.extend_from_slice(data.as_slice());
                                        break;
                                    }
                                    Err(err) => {
                                        attempt += 1;
                                        if attempt <= max_retries {
                                            if let Some(ref cb) = on_retry {
                                                cb(&err, action_idx, attempt);
                                            }
                                            let delay = match retry_mode {
                                                RetryMode::Linear => base_backoff * (attempt as u32),
                                                RetryMode::Exponential => {
                                                    let shift = (attempt - 1).min(10) as u32;
                                                    base_backoff * (1 << shift)
                                                }
                                                RetryMode::None => Duration::ZERO,
                                            };
                                            if delay > Duration::ZERO {
                                                tokio::time::sleep(delay).await;
                                            }
                                        } else {
                                            task_failed = true;
                                            last_error = Some(err);
                                            break;
                                        }
                                    }
                                }
                            }
                            if task_failed {
                                break;
                            }
                        }

                        if !task_failed {
                            if current_interval != target_interval {
                                current_interval = target_interval;
                                ticker = tokio::time::interval(current_interval);
                                ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
                            }

                            if !is_healthy {
                                is_healthy = true;
                                if let Some(ref sc) = on_state_change {
                                    sc(true);
                                }
                            }

                            let data = RegisterData::new(combined_data);
                            if let Some(ref cb) = on_data {
                                cb(data.clone());
                            }

                            let _ = event_tx.send(PollingEvent::Data {
                                task_id: task_id.clone(),
                                data,
                            });
                        } else if let Some(err) = last_error {
                            current_interval = (current_interval * 2).min(max_backoff);
                            ticker = tokio::time::interval(current_interval);
                            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

                            if is_healthy {
                                is_healthy = false;
                                if let Some(ref sc) = on_state_change {
                                    sc(false);
                                }
                            }

                            if let Some(ref cb) = on_error {
                                cb(&err);
                            }

                            let _ = event_tx.send(PollingEvent::Error {
                                task_id: task_id.clone(),
                                error: err.to_string(),
                            });
                        }

                        let delay = *inter_delay_holder.read().await;
                        if delay > Duration::ZERO {
                            tokio::time::sleep(delay).await;
                        }
                    }
                }
            }
        });

        tasks.insert(
            id_str,
            TaskHandle {
                interval: interval_holder,
                is_paused,
                stop_tx: Some(stop_tx),
                task_def: task,
            },
        );

        Ok(())
    }

    /// Registers a simplified polling task from an asynchronous closure.
    pub async fn add_simple_task<A: PollAction>(
        &self,
        id: impl Into<String>,
        interval: Duration,
        action: A,
    ) -> Result<()> {
        let action = Arc::new(action);
        let task = PollingTask::new(id, interval, move || {
            let a = action.clone();
            async move { a.call().await }
        });
        self.add_task(task).await
    }

    /// Pauses an active polling task without terminating its background worker.
    pub async fn pause_task(&self, id: &str) -> Result<()> {
        let tasks = self.tasks.read().await;
        let task = tasks.get(id).ok_or_else(|| {
            ModbusError::InvalidFrame(format!("Task '{}' not found", id))
        })?;
        task.is_paused.store(true, Ordering::Relaxed);
        Ok(())
    }

    /// Resumes execution of a previously paused polling task.
    pub async fn resume_task(&self, id: &str) -> Result<()> {
        let tasks = self.tasks.read().await;
        let task = tasks.get(id).ok_or_else(|| {
            ModbusError::InvalidFrame(format!("Task '{}' not found", id))
        })?;
        task.is_paused.store(false, Ordering::Relaxed);
        Ok(())
    }

    /// Restarts a task, stopping its current worker and re-spawning it with base settings.
    pub async fn restart_task(&self, id: &str) -> Result<()> {
        let mut tasks = self.tasks.write().await;
        let mut handle = tasks.remove(id).ok_or_else(|| {
            ModbusError::InvalidFrame(format!("Task '{}' not found", id))
        })?;

        if let Some(stop) = handle.stop_tx.take() {
            let _ = stop.send(());
        }

        let task_def = handle.task_def.clone();
        drop(tasks);
        self.add_task(task_def).await
    }

    /// Dynamically updates the execution interval of an active polling task.
    pub async fn set_task_interval(&self, id: &str, interval: Duration) -> Result<()> {
        let tasks = self.tasks.read().await;
        let task = tasks.get(id).ok_or_else(|| {
            ModbusError::InvalidFrame(format!("Task '{}' not found", id))
        })?;
        let mut current = task.interval.write().await;
        *current = interval;
        Ok(())
    }

    /// Returns `true` if the specified task is registered and not currently paused.
    pub async fn is_task_running(&self, id: &str) -> bool {
        let tasks = self.tasks.read().await;
        if let Some(task) = tasks.get(id) {
            !task.is_paused.load(Ordering::Relaxed)
        } else {
            false
        }
    }

    /// Returns `true` if the specified task is currently paused.
    pub async fn is_task_paused(&self, id: &str) -> bool {
        let tasks = self.tasks.read().await;
        if let Some(task) = tasks.get(id) {
            task.is_paused.load(Ordering::Relaxed)
        } else {
            false
        }
    }

    /// Checks if a task with the given ID exists.
    pub async fn has_task(&self, id: &str) -> bool {
        let tasks = self.tasks.read().await;
        tasks.contains_key(id)
    }

    /// Returns a list of all registered polling task IDs.
    pub async fn get_task_ids(&self) -> Vec<String> {
        let tasks = self.tasks.read().await;
        tasks.keys().cloned().collect()
    }

    /// Pauses all currently registered polling tasks.
    pub async fn pause_all(&self) {
        let tasks = self.tasks.read().await;
        for task in tasks.values() {
            task.is_paused.store(true, Ordering::Relaxed);
        }
    }

    /// Resumes all paused polling tasks.
    pub async fn resume_all(&self) {
        let tasks = self.tasks.read().await;
        for task in tasks.values() {
            task.is_paused.store(false, Ordering::Relaxed);
        }
    }

    /// Alias for [`pause_all`](Self::pause_all).
    pub async fn pause_all_tasks(&self) {
        self.pause_all().await;
    }

    /// Alias for [`resume_all`](Self::resume_all).
    pub async fn resume_all_tasks(&self) {
        self.resume_all().await;
    }

    /// Stops and terminates all polling tasks.
    pub async fn stop_all_tasks(&self) {
        self.clear_all().await;
    }

    /// Clears and terminates all registered polling tasks.
    pub async fn clear_all(&self) {
        let mut tasks = self.tasks.write().await;
        for (_, mut handle) in tasks.drain() {
            if let Some(stop) = handle.stop_tx.take() {
                let _ = stop.send(());
            }
        }
    }

    /// Removes and terminates an individual polling task by ID.
    pub async fn remove_task(&self, id: &str) -> Result<()> {
        let mut tasks = self.tasks.write().await;
        if let Some(mut handle) = tasks.remove(id) {
            if let Some(stop) = handle.stop_tx.take() {
                let _ = stop.send(());
            }
            Ok(())
        } else {
            Err(ModbusError::InvalidFrame(format!("Task '{}' not found", id)))
        }
    }

    /// Subscribes to the broadcast channel of all polling events.
    pub fn subscribe(&self) -> broadcast::Receiver<PollingEvent> {
        self.event_tx.subscribe()
    }
}

impl Default for PollingManager {
    /// Creates a default polling manager with 256 event capacity.
    fn default() -> Self {
        Self::new(256)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_polling_manager_lifecycle() {
        let manager = PollingManager::new(10);
        let mut subscriber = manager.subscribe();

        let task = PollingTask::new("test_task", Duration::from_millis(50), || async {
            Ok(RegisterData::new(vec![123, 456]))
        });

        manager.add_task(task).await.unwrap();

        let event = subscriber.recv().await.unwrap();
        match event {
            PollingEvent::Data { task_id, data } => {
                assert_eq!(task_id, "test_task");
                assert_eq!(data.get_u16(0).unwrap(), 123);
                assert_eq!(data.get_u16(1).unwrap(), 456);
            }
            _ => panic!("Expected PollingEvent::Data"),
        }

        assert!(manager.is_task_running("test_task").await);
        manager.pause_task("test_task").await.unwrap();
        assert!(manager.is_task_paused("test_task").await);
        manager.resume_task("test_task").await.unwrap();
        assert!(manager.is_task_running("test_task").await);

        manager.remove_task("test_task").await.unwrap();
        assert!(!manager.has_task("test_task").await);
    }

    #[tokio::test]
    async fn test_polling_task_multi_action_and_should_run() {
        let manager = PollingManager::new(10);
        manager.set_inter_task_delay(Duration::from_millis(5)).await;
        assert_eq!(manager.inter_task_delay().await, Duration::from_millis(5));

        let mut subscriber = manager.subscribe();

        let should_execute = Arc::new(AtomicBool::new(false));
        let should_exec_clone = should_execute.clone();

        let task = PollingTask::new("multi_task", Duration::from_millis(20), || async {
            Ok(RegisterData::new(vec![10]))
        })
        .add_action(|| async {
            Ok(RegisterData::new(vec![20]))
        })
        .should_run(move || should_exec_clone.load(Ordering::SeqCst))
        .retry_mode(RetryMode::None);

        manager.add_task(task).await.unwrap();

        // Tick occurs while should_run is false -> nothing received
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(subscriber.try_recv().is_err());

        // Enable execution
        should_execute.store(true, Ordering::SeqCst);

        let event = subscriber.recv().await.unwrap();
        match event {
            PollingEvent::Data { task_id, data } => {
                assert_eq!(task_id, "multi_task");
                assert_eq!(data.len(), 2);
                assert_eq!(data.get_u16(0).unwrap(), 10);
                assert_eq!(data.get_u16(1).unwrap(), 20);
            }
            _ => panic!("Expected Data event with combined results"),
        }

        manager.clear_all().await;
    }
}
