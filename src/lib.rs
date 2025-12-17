//! CloudWatch Metrics Crate
//!
//! This crate provides a metrics backend that emits metrics to AWS CloudWatch.
//! It uses the `metrics` crate facade for the user interface and buffers metrics
//! to send them in configurable intervals via a background task.
//!
//! Metrics are associated with a Service entity in CloudWatch for better
//! organization and correlation.
//!
//! # Example
//!
//! ```ignore
//! use indaq_metrics::{init, CloudWatchMetricsConfig};
//! use metrics::{counter, gauge, histogram};
//!
//! #[tokio::main]
//! async fn main() {
//!     let config = CloudWatchMetricsConfig::new("MyNamespace", "MyService", "production")
//!         .with_flush_interval(Duration::from_secs(60))
//!         .with_buffer_size(1000);
//!
//!     let handle = init(cloudwatch_client, config)?;
//!
//!     // Use metrics as normal
//!     counter!("requests_total").increment(1);
//!     gauge!("active_connections").set(42.0);
//!     histogram!("request_duration_ms").record(150.0);
//!
//!     // Gracefully shutdown
//!     handle.shutdown().await;
//! }
//! ```

pub mod metrics;

pub mod logs;

pub use metrics::{
  CloudWatchMetricsConfig, MetricsHandle, Unit, counter, describe_counter, describe_gauge,
  describe_histogram, gauge, histogram,
};

pub use logs::{CloudWatchLogsConfig, LogsHandle, init_logs};

use aws_sdk_cloudwatch::Client as CloudWatchClient;
use aws_sdk_cloudwatchlogs::Client as CloudWatchLogsClient;

/// Handle to both metrics and logs systems.
#[derive(Clone)]
pub struct CloudWatchHandle {
  pub metrics: MetricsHandle,
  pub logs: LogsHandle,
}

impl CloudWatchHandle {
  /// Trigger an immediate flush for both metrics and logs.
  pub fn flush(&self) {
    self.metrics.flush();
    self.logs.flush();
  }

  /// Gracefully shutdown both metrics and logs.
  pub async fn shutdown(self) {
    self.metrics.shutdown().await;
    self.logs.shutdown().await;
  }
}

/// Initialize both CloudWatch metrics and CloudWatch logs.
///
/// This sets up a global metrics recorder and a global logs sender, and spawns
/// background tasks to buffer and send metrics/logs to AWS.
///
/// # Arguments
///
/// * `metrics_client` - The AWS CloudWatch client to use for sending metrics.
/// * `metrics_config` - Configuration for the metrics system.
/// * `logs_client` - The AWS CloudWatch Logs client to use for sending logs.
/// * `logs_config` - Configuration for the logs system.
///
/// # Returns
///
/// A `CloudWatchHandle` that can be used to flush or shutdown both systems.
///
/// # Errors
///
/// Returns an error if metrics or logs have already been initialized.
pub fn init(
  metrics_client: CloudWatchClient,
  metrics_config: CloudWatchMetricsConfig,
  logs_client: CloudWatchLogsClient,
  logs_config: CloudWatchLogsConfig,
) -> Result<CloudWatchHandle, Box<dyn std::error::Error>> {
  // Fail fast if logs are already initialized, so we don't partially
  // initialize metrics and then fail.
  if crate::logs::is_initialized() {
    return Err("Global logs sender already initialized".into());
  }

  let metrics = crate::metrics::init_impl(metrics_client, metrics_config)?;
  let logs = crate::logs::init_logs(logs_client, logs_config)?;

  Ok(CloudWatchHandle { metrics, logs })
}

/// Initialize both CloudWatch metrics and logs, panicking on failure.
///
/// This is a convenience wrapper around [`init`] that panics if the
/// recorders cannot be set.
///
/// # Panics
///
/// Panics if metrics or logs have already been initialized.
pub fn init_or_panic(
  metrics_client: CloudWatchClient,
  metrics_config: CloudWatchMetricsConfig,
  logs_client: CloudWatchLogsClient,
  logs_config: CloudWatchLogsConfig,
) -> CloudWatchHandle {
  init(metrics_client, metrics_config, logs_client, logs_config)
    .expect("Failed to initialize CloudWatch metrics/logs")
}

/// Initialize only CloudWatch metrics.
pub fn init_metrics(
  client: CloudWatchClient,
  config: CloudWatchMetricsConfig,
) -> Result<MetricsHandle, Box<dyn std::error::Error>> {
  crate::metrics::init_impl(client, config)
}

/// Initialize only CloudWatch metrics, panicking on failure.
pub fn init_metrics_or_panic(
  client: CloudWatchClient,
  config: CloudWatchMetricsConfig,
) -> MetricsHandle {
  init_metrics(client, config).expect("Failed to set global metrics recorder")
}
