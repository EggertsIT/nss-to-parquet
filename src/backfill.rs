use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use tokio::sync::{mpsc, watch};
use tracing::info;

use crate::config::AppConfig;
use crate::metrics::Metrics;
use crate::schema::{LogicalType, SchemaDef};
use crate::types::ParsedRecord;
use crate::writer::run_parquet_writer;

pub async fn run_direct_backfill(
    cfg: AppConfig,
    schema: Arc<SchemaDef>,
    total_rows: u64,
    days: u32,
    workers: usize,
    seed: u64,
    progress_every: u64,
) -> Result<()> {
    let workers = workers.max(1);
    let days = days.max(1);
    let progress_every = progress_every.max(1);
    let total_rows = total_rows.max(1);

    std::fs::create_dir_all(&cfg.writer.output_dir).with_context(|| {
        format!(
            "failed to create writer output dir {}",
            cfg.writer.output_dir.display()
        )
    })?;

    schema.field_index(&cfg.schema.time_field).ok_or_else(|| {
        anyhow::anyhow!(
            "time_field '{}' does not exist in schema",
            cfg.schema.time_field
        )
    })?;

    let metrics_state_path = cfg.writer.output_dir.join(".metrics-state.json");
    let metrics = Arc::new(Metrics::new_with_persistence(
        cfg.metrics.stats_window_hours.max(1),
        Some(metrics_state_path),
    ));

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let channel_capacity = cfg.listener.parsed_channel_capacity.max(50_000);
    let (parsed_tx, parsed_rx) = mpsc::channel::<ParsedRecord>(channel_capacity);

    let writer_cfg = cfg.clone();
    let writer_schema = Arc::clone(&schema);
    let writer_metrics = Arc::clone(&metrics);
    let mut writer_shutdown = shutdown_rx.clone();
    let writer_task = tokio::spawn(async move {
        run_parquet_writer(
            writer_cfg,
            writer_schema,
            parsed_rx,
            writer_metrics,
            None,
            &mut writer_shutdown,
        )
        .await
    });

    let window_end = Utc::now();
    let window_start = window_end - Duration::days(days as i64);
    let window_secs = (window_end - window_start).num_seconds().max(1) as u64;

    info!(
        total_rows,
        workers,
        days,
        output = %cfg.writer.output_dir.display(),
        "starting direct parquet backfill"
    );

    let sent_total = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::with_capacity(workers);
    let per_worker = total_rows / workers as u64;
    let remainder = total_rows % workers as u64;

    for worker in 0..workers {
        let tx = parsed_tx.clone();
        let schema = Arc::clone(&schema);
        let time_field = cfg.schema.time_field.clone();
        let time_format = cfg.schema.time_format.clone();
        let worker_rows = per_worker + u64::from(worker + 1 == workers) * remainder;
        let worker_seed = seed.wrapping_add((worker as u64 + 1) * 97_421);
        let sent_total = Arc::clone(&sent_total);

        handles.push(tokio::spawn(async move {
            for idx in 0..worker_rows {
                let entropy = splitmix64(worker_seed ^ idx.wrapping_mul(0x9E37_79B9_7F4A_7C15));
                let event_time = synth_event_time(window_start, window_secs, entropy);
                let values = schema
                    .fields
                    .iter()
                    .enumerate()
                    .map(|(field_idx, field)| {
                        synth_value(
                            &field.name,
                            &field.logical_type,
                            &time_field,
                            &time_format,
                            event_time,
                            splitmix64(
                                entropy ^ (field_idx as u64).wrapping_mul(0xD6E8_FD90_8A5D_35A7),
                            ),
                        )
                    })
                    .collect::<Vec<_>>();

                let parsed = ParsedRecord {
                    values,
                    event_time,
                    spool_seq: None,
                };
                if tx.send(parsed).await.is_err() {
                    break;
                }

                let sent = sent_total.fetch_add(1, Ordering::Relaxed) + 1;
                if sent.is_multiple_of(progress_every) {
                    info!(sent, "direct backfill progress");
                }
            }
            Ok::<(), anyhow::Error>(())
        }));
    }
    drop(parsed_tx);

    for handle in handles {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => return Err(err),
            Err(err) => anyhow::bail!("backfill worker join error: {err}"),
        }
    }

    match writer_task.await {
        Ok(Ok(())) => {}
        Ok(Err(err)) => return Err(err).context("parquet writer failed during direct backfill"),
        Err(err) => anyhow::bail!("parquet writer join error: {err}"),
    }

    let _ = shutdown_tx.send(true);
    let sent = sent_total.load(Ordering::Relaxed);
    if let Err(err) = metrics.persist_now() {
        info!(error = %err, "failed to persist metrics state after direct backfill");
    }
    info!(sent, "direct parquet backfill finished");
    Ok(())
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

fn synth_event_time(start: DateTime<Utc>, window_secs: u64, entropy: u64) -> DateTime<Utc> {
    start + Duration::seconds((entropy % window_secs) as i64)
}

fn synth_value(
    field_name: &str,
    logical_type: &LogicalType,
    time_field: &str,
    time_format: &str,
    event_time: DateTime<Utc>,
    entropy: u64,
) -> Option<String> {
    let key = field_name.to_ascii_lowercase();
    if field_name == time_field {
        return Some(event_time.format(time_format).to_string());
    }

    if key.contains("action") {
        return Some(if entropy % 100 < 94 {
            "Allowed".to_string()
        } else {
            "Blocked".to_string()
        });
    }
    if key.contains("respcode") {
        return Some(if entropy % 100 < 94 {
            "200".to_string()
        } else {
            "403".to_string()
        });
    }
    if key.contains("reqmethod") {
        return Some(match entropy % 3 {
            0 => "GET".to_string(),
            1 => "POST".to_string(),
            _ => "CONNECT".to_string(),
        });
    }
    if key == "sip" || key.contains("server_ip") || key.contains("destination_ip") {
        return Some(format!(
            "{}.{}.{}.{}",
            20 + (entropy % 180),
            (entropy >> 8) & 0xff,
            (entropy >> 16) & 0xff,
            1 + ((entropy >> 24) % 254)
        ));
    }
    if key == "cip" || key.contains("client_ip") || key.contains("source_ip") {
        return Some(format!(
            "10.{}.{}.{}",
            (entropy >> 8) & 0xff,
            (entropy >> 16) & 0xff,
            1 + ((entropy >> 24) % 254)
        ));
    }
    if key.contains("appname") {
        return Some(
            [
                "M365",
                "Salesforce",
                "GitHub",
                "ServiceNow",
                "Workday",
                "Zoom",
                "Slack",
                "AWSConsole",
            ][(entropy % 8) as usize]
                .to_string(),
        );
    }
    if key.contains("appclass") {
        return Some(
            ["Office Apps", "Business", "Development", "Communication"][(entropy % 4) as usize]
                .to_string(),
        );
    }
    if key.contains("app_status") {
        return Some(["Sanctioned", "Unsanctioned", "N/A"][(entropy % 3) as usize].to_string());
    }
    if key.contains("url") || key.contains("host") || key.contains("referer") {
        let domain = [
            "example.com",
            "github.com",
            "microsoft.com",
            "aws.amazon.com",
        ][(entropy % 4) as usize];
        return Some(format!(
            "{}/v{}/{}/{}",
            domain,
            1 + (entropy % 5),
            (entropy >> 8) % 10_000,
            (entropy >> 16) % 10_000
        ));
    }
    if key.contains("dept") {
        return Some(
            [
                "IT",
                "HR",
                "Finance",
                "Sales",
                "Security",
                "Engineering",
                "Operations",
            ][(entropy % 7) as usize]
                .to_string(),
        );
    }
    if key.contains("location") {
        return Some(
            ["HQ-Berlin", "HQ-NYC", "DC-1", "DC-2", "Remote"][(entropy % 5) as usize].to_string(),
        );
    }
    if key.contains("deviceowner") {
        return Some(format!("user{:05}", entropy % 42_000));
    }
    if key.contains("devicehostname") {
        return Some(format!("dev-{:08x}", entropy as u32));
    }
    if key.contains("ua") {
        return Some(
            [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4)",
                "Mozilla/5.0 (X11; Linux x86_64)",
                "curl/8.6.0",
            ][(entropy % 4) as usize]
                .to_string(),
        );
    }
    if key.contains("threatseverity") {
        return Some(
            ["None", "Low", "Medium", "High", "Critical"][(entropy % 5) as usize].to_string(),
        );
    }
    if (key.contains("threatname")
        || key.contains("malwarecat")
        || key.contains("dlp")
        || key.contains("rulelabel")
        || key.contains("ruletype"))
        && entropy % 100 < 92
    {
        return Some("None".to_string());
    }

    match logical_type {
        LogicalType::String => Some(format!("{}-{:x}", field_name, entropy & 0xffff)),
        LogicalType::Int64 => Some((entropy % 500_000).to_string()),
        LogicalType::Float64 => {
            let v = ((entropy % 100_000) as f64) / 100.0;
            Some(format!("{v:.2}"))
        }
        LogicalType::Boolean => Some(
            if entropy.is_multiple_of(2) {
                "true"
            } else {
                "false"
            }
            .to_string(),
        ),
        LogicalType::Timestamp => Some(event_time.to_rfc3339()),
        LogicalType::Ip => Some(format!(
            "{}.{}.{}.{}",
            1 + (entropy % 223),
            (entropy >> 8) & 0xff,
            (entropy >> 16) & 0xff,
            1 + ((entropy >> 24) % 254)
        )),
    }
}
