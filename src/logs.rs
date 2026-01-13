use std::{
  collections::HashMap,
  sync::OnceLock,
  time::{Duration, SystemTime, UNIX_EPOCH},
};

use aws_sdk_cloudwatchlogs::{
  Client as CloudWatchLogsClient,
  types::{Entity, InputLogEvent},
};
use serde::Serialize;
use serde_json::Value;
use tokio::sync::mpsc;

/// Configuration for the CloudWatch logs handler.
#[derive(Debug, Clone)]
pub struct CloudWatchLogsConfig {
  /// The CloudWatch Log Group name.
  pub log_group: String,
  /// The CloudWatch Log Stream name.
  pub log_stream: String,
  /// The service name to attach as Entity metadata.
  pub service_name: String,
  /// How often to flush buffered logs to CloudWatch.
  pub flush_interval: Duration,
  /// Maximum number of logs to buffer before forcing a flush.
  pub buffer_size: usize,
}

impl CloudWatchLogsConfig {
  /// Create a new configuration.
  pub fn new(
    log_group: impl Into<String>,
    log_stream: impl Into<String>,
    service_name: impl Into<String>,
  ) -> Self {
    Self {
      log_group: log_group.into(),
      log_stream: log_stream.into(),
      service_name: service_name.into(),
      flush_interval: Duration::from_secs(60),
      buffer_size: 256,
    }
  }

  /// Set the flush interval.
  pub fn with_flush_interval(mut self, interval: Duration) -> Self {
    self.flush_interval = interval;
    self
  }

  /// Set the maximum buffer size.
  pub fn with_buffer_size(mut self, size: usize) -> Self {
    self.buffer_size = size;
    self
  }
}

#[derive(Debug, Serialize)]
struct Event {
  timestamp: u64,

  level: String,
  message: String,

  #[serde(flatten)]
  fields: HashMap<String, Value>,
}

enum WorkerMessage {
  Log(Event),
  Flush,
  Shutdown,
}

static LOG_SENDER: OnceLock<mpsc::UnboundedSender<WorkerMessage>> = OnceLock::new();

pub(crate) fn is_initialized() -> bool {
  LOG_SENDER.get().is_some()
}

/// Handle to the logs system.
#[derive(Clone)]
pub struct LogsHandle {
  sender: mpsc::UnboundedSender<WorkerMessage>,
}

impl LogsHandle {
  /// Trigger an immediate flush of buffered logs.
  pub fn flush(&self) {
    let _ = self.sender.send(WorkerMessage::Flush);
  }

  /// Gracefully shutdown the logs system.
  pub async fn shutdown(self) {
    let _ = self.sender.send(WorkerMessage::Shutdown);
    tokio::time::sleep(Duration::from_millis(100)).await;
  }
}

/// Initialize the CloudWatch logs handler.
pub fn init_logs(
  client: CloudWatchLogsClient,
  config: CloudWatchLogsConfig,
) -> Result<LogsHandle, Box<dyn std::error::Error>> {
  let (sender, receiver) = mpsc::unbounded_channel();

  if LOG_SENDER.set(sender.clone()).is_err() {
    return Err("Global logs sender already initialized".into());
  }

  tokio::spawn(logs_worker(client, config, receiver));

  Ok(LogsHandle { sender })
}

/// Internal function to emit a log entry.
/// Used by macros.
#[doc(hidden)]
pub fn emit_log(level: &str, message: String, fields: impl IntoIterator<Item = (String, Value)>) {
  if let Some(sender) = LOG_SENDER.get() {
    let timestamp = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap_or_default()
      .as_millis() as u64;

    let entry = Event {
      timestamp,
      level: level.to_string(),
      message,
      fields: fields.into_iter().collect(),
    };

    let _ = sender.send(WorkerMessage::Log(entry));
  }
}

async fn logs_worker(
  client: CloudWatchLogsClient,
  config: CloudWatchLogsConfig,
  mut receiver: mpsc::UnboundedReceiver<WorkerMessage>,
) {
  let mut buffer: Vec<Event> = Vec::with_capacity(config.buffer_size);
  let mut interval = tokio::time::interval(config.flush_interval);

  // Ensure log group and stream exist (best effort)
  let _ = ensure_log_stream(&client, &config.log_group, &config.log_stream).await;

  loop {
    tokio::select! {
        _ = interval.tick() => {
            if !buffer.is_empty() {
              flush_logs(&client, &config, &mut buffer).await;
            }
        }
        msg = receiver.recv() => {
            match msg {
                Some(WorkerMessage::Log(entry)) => {
                    buffer.push(entry);
                    if buffer.len() >= config.buffer_size {
                flush_logs(&client, &config, &mut buffer).await;
                    }
                }
                Some(WorkerMessage::Flush) => {
                    if !buffer.is_empty() {
                flush_logs(&client, &config, &mut buffer).await;
                    }
                }
                Some(WorkerMessage::Shutdown) | None => {
                    if !buffer.is_empty() {
                flush_logs(&client, &config, &mut buffer).await;
                    }
                    break;
                }
            }
        }
    }
  }
}

async fn ensure_log_stream(
  client: &CloudWatchLogsClient,
  group: &str,
  stream: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
  // Best-effort creation.

  if let Err(err) = client
    .create_log_stream()
    .log_group_name(group)
    .log_stream_name(stream)
    .send()
    .await
  {
    let is_already_exists = err.as_service_error().is_some_and(|service_error| {
      matches!(
        service_error,
        aws_sdk_cloudwatchlogs::operation::create_log_stream::CreateLogStreamError::ResourceAlreadyExistsException(_)
      )
    });

    if !is_already_exists {
      sentry::capture_error(&err);
      eprintln!("CloudWatchLogs CreateLogStream failed: {err:?}");
    }
  }

  Ok(())
}

async fn flush_logs(
  client: &CloudWatchLogsClient,
  config: &CloudWatchLogsConfig,
  buffer: &mut Vec<Event>,
) {
  if buffer.is_empty() {
    return;
  }

  // Sort by timestamp as required by CloudWatch Logs
  buffer.sort_by_key(|e| e.timestamp);

  let log_events: Vec<InputLogEvent> = buffer
    .iter()
    .filter_map(|entry| {
      let message_json = serde_json::to_string(entry).ok()?;

      InputLogEvent::builder()
        .timestamp(entry.timestamp as i64)
        .message(message_json)
        .build()
        .ok()
    })
    .collect();

  // CloudWatch Logs has a limit of 1MB or 10000 records per batch.
  // For simplicity, we'll just send what we have, assuming the buffer size isn't huge.
  // A production ready version should split batches.

  if log_events.is_empty() {
    buffer.clear();
    return;
  }

  let _ = client
    .put_log_events()
    .log_group_name(&config.log_group)
    .log_stream_name(&config.log_stream)
    .set_entity(Some(
      Entity::builder()
        .key_attributes("Type", "Service")
        .key_attributes("Name", &config.service_name)
        .build(),
    ))
    .set_log_events(Some(log_events))
    .send()
    .await
    .map_err(|err| {
      sentry::capture_error(&err);
      eprintln!("CloudWatchLogs PutLogEvents failed: {err:?}");
      err
    });

  buffer.clear();
}

#[macro_export]
macro_rules! info {
    ($msg:expr $(, $key:ident = $value:expr)* $(,)?) => {
        $crate::logs::emit_log("info", $msg.to_string(), vec![
            $( (stringify!($key).to_string(), serde_json::json!($value)) ),*
        ])
    };

    ($msg:expr, $fields:expr $(,)?) => {
      $crate::logs::emit_log("info", $msg.to_string(), $fields)
    };
}

#[macro_export]
macro_rules! error {
    ($msg:expr $(, $key:ident = $value:expr)* $(,)?) => {
        $crate::logs::emit_log("error", $msg.to_string(), vec![
            $( (stringify!($key).to_string(), serde_json::json!($value)) ),*
        ])
    };

    ($msg:expr, $fields:expr $(,)?) => {
      $crate::logs::emit_log("error", $msg.to_string(), $fields)
    };
}

#[macro_export]
macro_rules! warn {
    ($msg:expr $(, $key:ident = $value:expr)* $(,)?) => {
        $crate::logs::emit_log("warn", $msg.to_string(), vec![
            $( (stringify!($key).to_string(), serde_json::json!($value)) ),*
        ])
    };

    ($msg:expr, $fields:expr $(,)?) => {
      $crate::logs::emit_log("warn", $msg.to_string(), $fields)
    };
}

#[macro_export]
macro_rules! debug {
    ($msg:expr $(, $key:ident = $value:expr)* $(,)?) => {
        $crate::logs::emit_log("debug", $msg.to_string(), vec![
            $( (stringify!($key).to_string(), serde_json::json!($value)) ),*
        ])
    };

    ($msg:expr, $fields:expr $(,)?) => {
      $crate::logs::emit_log("debug", $msg.to_string(), $fields)
    };
}
