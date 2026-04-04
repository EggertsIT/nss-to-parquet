use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(default)]
pub struct AppConfig {
    pub listener: ListenerConfig,
    pub schema: SchemaConfig,
    pub writer: WriterConfig,
    pub dlq: DlqConfig,
    pub retention: RetentionConfig,
    pub metrics: MetricsConfig,
    pub durability: DurabilityConfig,
}

impl AppConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config file {}", path.display()))?;
        let mut cfg: AppConfig = toml::from_str(&raw)
            .with_context(|| format!("failed to parse config file {}", path.display()))?;

        if cfg.schema.path.is_relative() {
            let parent = path.parent().unwrap_or_else(|| Path::new("."));
            cfg.schema.path = parent.join(&cfg.schema.path);
        }
        if cfg.writer.output_dir.is_relative() {
            let parent = path.parent().unwrap_or_else(|| Path::new("."));
            cfg.writer.output_dir = parent.join(&cfg.writer.output_dir);
        }
        if cfg.dlq.path.is_relative() {
            let parent = path.parent().unwrap_or_else(|| Path::new("."));
            cfg.dlq.path = parent.join(&cfg.dlq.path);
        }
        if cfg.durability.path.is_relative() {
            let parent = path.parent().unwrap_or_else(|| Path::new("."));
            cfg.durability.path = parent.join(&cfg.durability.path);
        }

        Ok(cfg)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ListenerConfig {
    pub bind_addr: String,
    pub max_line_bytes: usize,
    pub read_timeout_secs: u64,
    pub max_connections: usize,
    pub ingress_channel_capacity: usize,
    pub parsed_channel_capacity: usize,
}

impl Default for ListenerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:514".to_string(),
            max_line_bytes: 256 * 1024,
            read_timeout_secs: 30,
            max_connections: 1024,
            ingress_channel_capacity: 50_000,
            parsed_channel_capacity: 50_000,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct SchemaConfig {
    pub profile: String,
    pub custom_schema_mode: bool,
    pub path: PathBuf,
    pub time_field: String,
    pub time_format: String,
    pub timezone: String,
    pub strict_type_validation: bool,
}

impl Default for SchemaConfig {
    fn default() -> Self {
        Self {
            profile: "zscaler_web_v1".to_string(),
            custom_schema_mode: false,
            path: PathBuf::from("schema.yaml"),
            time_field: "time".to_string(),
            time_format: "%a %b %d %H:%M:%S %Y".to_string(),
            timezone: "UTC".to_string(),
            strict_type_validation: true,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct WriterConfig {
    pub output_dir: PathBuf,
    pub batch_rows: usize,
    pub flush_interval_secs: u64,
    pub target_file_rows: usize,
    pub compression: String,
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            output_dir: PathBuf::from("data"),
            batch_rows: 10_000,
            flush_interval_secs: 5,
            target_file_rows: 250_000,
            compression: "zstd".to_string(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct DlqConfig {
    pub path: PathBuf,
    pub channel_capacity: usize,
    pub local_days: i64,
    pub sweep_interval_secs: u64,
}

impl Default for DlqConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("dlq"),
            channel_capacity: 20_000,
            local_days: 14,
            sweep_interval_secs: 3600,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct RetentionConfig {
    pub enabled: bool,
    pub local_days: i64,
    pub sweep_interval_secs: u64,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            local_days: 14,
            sweep_interval_secs: 3600,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub bind_addr: String,
    pub dashboard_enabled: bool,
    pub stats_window_hours: u32,
    pub degraded_error_ratio: f64,
    pub critical_error_ratio: f64,
    pub degraded_stale_seconds: u64,
    pub critical_stale_seconds: u64,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            bind_addr: "127.0.0.1:9090".to_string(),
            dashboard_enabled: false,
            stats_window_hours: 24,
            degraded_error_ratio: 0.02,
            critical_error_ratio: 0.10,
            degraded_stale_seconds: 300,
            critical_stale_seconds: 900,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct DurabilityConfig {
    pub enabled: bool,
    pub path: PathBuf,
    pub fsync_every: u64,
    pub max_log_bytes: u64,
}

impl Default for DurabilityConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            path: PathBuf::from("spool"),
            fsync_every: 100,
            max_log_bytes: 512 * 1024 * 1024,
        }
    }
}
