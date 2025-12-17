use std::{collections::HashMap, sync::Arc, time::Duration};

use aws_sdk_cloudwatch::{
  Client as CloudWatchClient,
  types::{Dimension, Entity, EntityMetricData, MetricDatum, StandardUnit},
};
pub use metrics::Unit;
use metrics::{
  Counter, CounterFn, Gauge, GaugeFn, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder,
  SharedString,
};
use parking_lot::Mutex;
use tokio::sync::mpsc;

/// Configuration for the CloudWatch metrics recorder.
#[derive(Debug, Clone)]
pub struct CloudWatchMetricsConfig {
  /// The CloudWatch namespace for all metrics.
  pub namespace: String,
  /// The service name for entity metrics.
  pub service_name: String,
  /// The environment name (e.g., "production", "staging").
  pub environment: String,
  /// How often to flush buffered metrics to CloudWatch.
  pub flush_interval: Duration,
  /// Maximum number of metrics to buffer before forcing a flush.
  pub buffer_size: usize,
  /// Default dimensions to add to all metrics.
  pub default_dimensions: Vec<(String, String)>,
}

impl CloudWatchMetricsConfig {
  /// Create a new configuration with the given namespace, service name, and environment.
  pub fn new(
    namespace: impl Into<String>,
    service_name: impl Into<String>,
    environment: impl Into<String>,
  ) -> Self {
    Self {
      namespace: namespace.into(),
      service_name: service_name.into(),
      environment: environment.into(),
      flush_interval: Duration::from_secs(60),
      buffer_size: 1000,
      default_dimensions: Vec::new(),
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

  /// Add a default dimension that will be attached to all metrics.
  pub fn with_dimension(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
    self.default_dimensions.push((name.into(), value.into()));
    self
  }
}

/// A metric value that can be sent to CloudWatch.
#[derive(Debug, Clone)]
enum MetricValue {
  Counter(f64),
  Gauge(f64),
  Histogram(f64),
}

/// A buffered metric ready to be sent to CloudWatch.
#[derive(Debug, Clone)]
struct BufferedMetric {
  name: String,
  value: MetricValue,
  dimensions: Vec<(String, String)>,
  unit: Option<StandardUnit>,
}

/// Message sent to the background worker.
enum WorkerMessage {
  Metric(BufferedMetric),
  Flush,
  Shutdown,
}

/// Handle to the metrics system.
///
/// Use this to gracefully shutdown the metrics background worker.
#[derive(Clone)]
pub struct MetricsHandle {
  sender: mpsc::UnboundedSender<WorkerMessage>,
}

impl MetricsHandle {
  /// Trigger an immediate flush of buffered metrics.
  pub fn flush(&self) {
    let _ = self.sender.send(WorkerMessage::Flush);
  }

  /// Gracefully shutdown the metrics system.
  ///
  /// This will flush any remaining buffered metrics before shutting down.
  pub async fn shutdown(self) {
    let _ = self.sender.send(WorkerMessage::Shutdown);
    // Give the worker a moment to flush
    tokio::time::sleep(Duration::from_millis(100)).await;
  }
}

/// Internal counter implementation.
struct CloudWatchCounter {
  key: Key,
  sender: mpsc::UnboundedSender<WorkerMessage>,
  default_dimensions: Vec<(String, String)>,
}

impl CounterFn for CloudWatchCounter {
  fn increment(&self, value: u64) {
    let dimensions = self.build_dimensions();
    let metric = BufferedMetric {
      name: self.key.name().to_string(),
      value: MetricValue::Counter(value as f64),
      dimensions,
      unit: Some(StandardUnit::Count),
    };
    let _ = self.sender.send(WorkerMessage::Metric(metric));
  }

  fn absolute(&self, value: u64) {
    let dimensions = self.build_dimensions();
    let metric = BufferedMetric {
      name: self.key.name().to_string(),
      value: MetricValue::Counter(value as f64),
      dimensions,
      unit: Some(StandardUnit::Count),
    };
    let _ = self.sender.send(WorkerMessage::Metric(metric));
  }
}

impl CloudWatchCounter {
  fn build_dimensions(&self) -> Vec<(String, String)> {
    let mut dimensions = self.default_dimensions.clone();
    for label in self.key.labels() {
      dimensions.push((label.key().to_string(), label.value().to_string()));
    }
    dimensions
  }
}

/// Internal gauge implementation.
struct CloudWatchGauge {
  key: Key,
  sender: mpsc::UnboundedSender<WorkerMessage>,
  default_dimensions: Vec<(String, String)>,
}

impl GaugeFn for CloudWatchGauge {
  fn increment(&self, value: f64) {
    // For CloudWatch, we just send the increment as a value
    // In a more sophisticated implementation, we'd track the current value
    let dimensions = self.build_dimensions();
    let metric = BufferedMetric {
      name: self.key.name().to_string(),
      value: MetricValue::Gauge(value),
      dimensions,
      unit: None,
    };
    let _ = self.sender.send(WorkerMessage::Metric(metric));
  }

  fn decrement(&self, value: f64) {
    let dimensions = self.build_dimensions();
    let metric = BufferedMetric {
      name: self.key.name().to_string(),
      value: MetricValue::Gauge(-value),
      dimensions,
      unit: None,
    };
    let _ = self.sender.send(WorkerMessage::Metric(metric));
  }

  fn set(&self, value: f64) {
    let dimensions = self.build_dimensions();
    let metric = BufferedMetric {
      name: self.key.name().to_string(),
      value: MetricValue::Gauge(value),
      dimensions,
      unit: None,
    };
    let _ = self.sender.send(WorkerMessage::Metric(metric));
  }
}

impl CloudWatchGauge {
  fn build_dimensions(&self) -> Vec<(String, String)> {
    let mut dimensions = self.default_dimensions.clone();
    for label in self.key.labels() {
      dimensions.push((label.key().to_string(), label.value().to_string()));
    }
    dimensions
  }
}

/// Internal histogram implementation.
struct CloudWatchHistogram {
  key: Key,
  sender: mpsc::UnboundedSender<WorkerMessage>,
  default_dimensions: Vec<(String, String)>,
  unit: Option<StandardUnit>,
}

impl HistogramFn for CloudWatchHistogram {
  fn record(&self, value: f64) {
    let dimensions = self.build_dimensions();
    let metric = BufferedMetric {
      name: self.key.name().to_string(),
      value: MetricValue::Histogram(value),
      dimensions,
      unit: self.unit.clone(),
    };
    let _ = self.sender.send(WorkerMessage::Metric(metric));
  }
}

impl CloudWatchHistogram {
  fn build_dimensions(&self) -> Vec<(String, String)> {
    let mut dimensions = self.default_dimensions.clone();
    for label in self.key.labels() {
      dimensions.push((label.key().to_string(), label.value().to_string()));
    }
    dimensions
  }
}

/// The CloudWatch metrics recorder.
struct CloudWatchRecorder {
  sender: mpsc::UnboundedSender<WorkerMessage>,
  default_dimensions: Vec<(String, String)>,
  units: Mutex<HashMap<String, StandardUnit>>,
}

impl Recorder for CloudWatchRecorder {
  fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {
    // CloudWatch doesn't have a concept of describing metrics ahead of time
  }

  fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {
    // CloudWatch doesn't have a concept of describing metrics ahead of time
  }

  fn describe_histogram(&self, key: KeyName, unit: Option<Unit>, _description: SharedString) {
    // Store the unit for later use when recording histogram values
    if let Some(unit) = unit {
      let cw_unit = convert_unit(unit);
      self.units.lock().insert(key.as_str().to_string(), cw_unit);
    }
  }

  fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
    Counter::from_arc(Arc::new(CloudWatchCounter {
      key: key.clone(),
      sender: self.sender.clone(),
      default_dimensions: self.default_dimensions.clone(),
    }))
  }

  fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
    Gauge::from_arc(Arc::new(CloudWatchGauge {
      key: key.clone(),
      sender: self.sender.clone(),
      default_dimensions: self.default_dimensions.clone(),
    }))
  }

  fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
    let unit = self
      .units
      .lock()
      .get(key.name().to_string().as_str())
      .cloned();
    Histogram::from_arc(Arc::new(CloudWatchHistogram {
      key: key.clone(),
      sender: self.sender.clone(),
      default_dimensions: self.default_dimensions.clone(),
      unit,
    }))
  }
}

/// Convert a `metrics` unit to a CloudWatch StandardUnit.
fn convert_unit(unit: Unit) -> StandardUnit {
  match unit {
    Unit::Count => StandardUnit::Count,
    Unit::Percent => StandardUnit::Percent,
    Unit::Seconds => StandardUnit::Seconds,
    Unit::Milliseconds => StandardUnit::Milliseconds,
    Unit::Microseconds => StandardUnit::Microseconds,
    Unit::Nanoseconds => StandardUnit::None, // CloudWatch doesn't have nanoseconds
    Unit::Bytes => StandardUnit::Bytes,
    Unit::Kibibytes => StandardUnit::Kilobytes,
    Unit::Mebibytes => StandardUnit::Megabytes,
    Unit::Gibibytes => StandardUnit::Gigabytes,
    Unit::Tebibytes => StandardUnit::Terabytes,
    Unit::BitsPerSecond => StandardUnit::BitsSecond,
    Unit::KilobitsPerSecond => StandardUnit::KilobitsSecond,
    Unit::MegabitsPerSecond => StandardUnit::MegabitsSecond,
    Unit::GigabitsPerSecond => StandardUnit::GigabitsSecond,
    Unit::TerabitsPerSecond => StandardUnit::TerabitsSecond,
    Unit::CountPerSecond => StandardUnit::CountSecond,
  }
}

/// Background worker that buffers and sends metrics to CloudWatch.
async fn metrics_worker(
  client: CloudWatchClient,
  namespace: String,
  service_name: String,
  mut receiver: mpsc::UnboundedReceiver<WorkerMessage>,
  flush_interval: Duration,
  buffer_size: usize,
) {
  let mut buffer: Vec<BufferedMetric> = Vec::with_capacity(buffer_size);
  let mut interval = tokio::time::interval(flush_interval);

  loop {
    tokio::select! {
      _ = interval.tick() => {
        if !buffer.is_empty() {
          flush_metrics(&client, &namespace, &service_name, &mut buffer).await;
        }
      }
      msg = receiver.recv() => {
        match msg {
          Some(WorkerMessage::Metric(metric)) => {
            buffer.push(metric);
            if buffer.len() >= buffer_size {
              flush_metrics(&client, &namespace, &service_name, &mut buffer).await;
            }
          }
          Some(WorkerMessage::Flush) => {
            if !buffer.is_empty() {
              flush_metrics(&client, &namespace, &service_name, &mut buffer).await;
            }
          }
          Some(WorkerMessage::Shutdown) | None => {
            // Final flush before shutdown
            if !buffer.is_empty() {
              flush_metrics(&client, &namespace, &service_name, &mut buffer).await;
            }
            break;
          }
        }
      }
    }
  }
}

/// Flush buffered metrics to CloudWatch.
async fn flush_metrics(
  client: &CloudWatchClient,
  namespace: &str,
  service_name: &str,
  buffer: &mut Vec<BufferedMetric>,
) {
  if buffer.is_empty() {
    return;
  }

  // CloudWatch allows max 1000 metrics per PutMetricData call
  const MAX_METRICS_PER_CALL: usize = 1000;

  // Build the service entity
  let entity = Entity::builder()
    .key_attributes("Type", "Service")
    .key_attributes("Name", service_name)
    .build();

  for chunk in buffer.chunks(MAX_METRICS_PER_CALL) {
    let metric_data: Vec<MetricDatum> = chunk
      .iter()
      .map(|m| {
        let dimensions: Vec<Dimension> = m
          .dimensions
          .iter()
          .map(|(name, value)| Dimension::builder().name(name).value(value).build())
          .collect();

        let value = match m.value {
          MetricValue::Counter(v) | MetricValue::Gauge(v) | MetricValue::Histogram(v) => v,
        };

        let mut builder = MetricDatum::builder()
          .metric_name(&m.name)
          .value(value)
          .set_dimensions(if dimensions.is_empty() {
            None
          } else {
            Some(dimensions)
          });

        if let Some(ref unit) = m.unit {
          builder = builder.unit(unit.clone());
        }

        builder.build()
      })
      .collect();

    let entity_metric_data = EntityMetricData::builder()
      .entity(entity.clone())
      .set_metric_data(Some(metric_data))
      .build();

    match client
      .put_metric_data()
      .namespace(namespace)
      .entity_metric_data(entity_metric_data)
      .strict_entity_validation(true)
      .send()
      .await
    {
      Ok(_) => {}
      Err(err) => {
        sentry::capture_error(&err);
        eprintln!("CloudWatch PutMetricData failed: {err:?}");
      }
    }
  }

  buffer.clear();
}

pub(crate) fn init_impl(
  client: CloudWatchClient,
  config: CloudWatchMetricsConfig,
) -> Result<MetricsHandle, Box<dyn std::error::Error>> {
  let (sender, receiver) = mpsc::unbounded_channel();

  let mut default_dimensions = config.default_dimensions.clone();
  default_dimensions.push(("Environment".to_string(), config.environment.clone()));

  let recorder = CloudWatchRecorder {
    sender: sender.clone(),
    default_dimensions,
    units: Mutex::new(HashMap::new()),
  };

  // Spawn the background worker
  tokio::spawn(metrics_worker(
    client,
    config.namespace,
    config.service_name,
    receiver,
    config.flush_interval,
    config.buffer_size,
  ));

  // Set the global recorder
  metrics::set_global_recorder(recorder)?;

  Ok(MetricsHandle { sender })
}

// Re-export metrics crate macros and types for convenience
pub use metrics::{
  counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram,
};

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_config_builder() {
    let config = CloudWatchMetricsConfig::new("TestNamespace", "TestService", "test")
      .with_flush_interval(Duration::from_secs(30))
      .with_buffer_size(500)
      .with_dimension("environment", "test");

    assert_eq!(config.namespace, "TestNamespace");
    assert_eq!(config.service_name, "TestService");
    assert_eq!(config.environment, "test");
    assert_eq!(config.flush_interval, Duration::from_secs(30));
    assert_eq!(config.buffer_size, 500);
    assert_eq!(config.default_dimensions.len(), 1);
    assert_eq!(
      config.default_dimensions[0],
      ("environment".to_string(), "test".to_string())
    );
  }

  #[test]
  fn test_unit_conversion() {
    assert!(matches!(convert_unit(Unit::Count), StandardUnit::Count));
    assert!(matches!(convert_unit(Unit::Seconds), StandardUnit::Seconds));
    assert!(matches!(
      convert_unit(Unit::Milliseconds),
      StandardUnit::Milliseconds
    ));
    assert!(matches!(convert_unit(Unit::Bytes), StandardUnit::Bytes));
    assert!(matches!(convert_unit(Unit::Percent), StandardUnit::Percent));
  }
}
