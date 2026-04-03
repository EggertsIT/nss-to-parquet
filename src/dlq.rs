use anyhow::{Context, Result};
use chrono::{Datelike, Days, NaiveDate, Utc};
use std::path::Path;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, watch};
use tracing::{info, warn};

use crate::config::DlqConfig;
use crate::metrics::Metrics;
use crate::types::DlqRecord;
use std::sync::Arc;

pub async fn run_dlq_writer(
    cfg: DlqConfig,
    mut rx: mpsc::Receiver<DlqRecord>,
    metrics: Arc<Metrics>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<()> {
    tokio::fs::create_dir_all(&cfg.path)
        .await
        .with_context(|| format!("failed to create dlq dir {}", cfg.path.display()))?;
    let mut sweep_tick =
        tokio::time::interval(Duration::from_secs(cfg.sweep_interval_secs.max(60)));

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    break;
                }
            }
            _ = sweep_tick.tick() => {
                if let Err(err) = sweep_old_dlq_files(&cfg.path, cfg.local_days) {
                    warn!(error = %err, "dlq retention sweep failed");
                }
            }
            maybe = rx.recv() => {
                let Some(record) = maybe else { break; };
                write_dlq_record(&cfg, &record).await?;
                metrics.observe_dlq_rows(1);
            }
        }
    }

    info!("dlq writer stopped");
    Ok(())
}

async fn write_dlq_record(cfg: &DlqConfig, record: &DlqRecord) -> Result<()> {
    let dt = record.received_at.with_timezone(&Utc);
    let file_name = format!("dlq-{:04}-{:02}-{:02}.log", dt.year(), dt.month(), dt.day());
    let path = cfg.path.join(file_name);

    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await
        .with_context(|| format!("failed opening dlq file {}", path.display()))?;

    let safe_raw = sanitize(&record.raw_line);
    let safe_reason = sanitize(&record.reason);
    let safe_peer = record
        .peer_addr
        .as_deref()
        .map(sanitize)
        .unwrap_or_default();
    let line = format!(
        "{}\t{}\t{}\t{}\n",
        record.received_at.to_rfc3339(),
        safe_peer,
        safe_reason,
        safe_raw
    );
    file.write_all(line.as_bytes()).await?;
    Ok(())
}

fn sanitize(input: &str) -> String {
    input
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', " ")
}

fn sweep_old_dlq_files(path: &Path, local_days: i64) -> Result<()> {
    if local_days <= 0 || !path.exists() {
        return Ok(());
    }

    let now = Utc::now().date_naive();
    let cutoff = now
        .checked_sub_days(Days::new(local_days as u64))
        .unwrap_or(now);

    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Some(date_str) = name
            .strip_prefix("dlq-")
            .and_then(|v| v.strip_suffix(".log"))
        else {
            continue;
        };
        let Ok(date) = NaiveDate::parse_from_str(date_str, "%Y-%m-%d") else {
            continue;
        };
        if date < cutoff {
            std::fs::remove_file(entry.path())?;
            info!(file = %name, "dlq retention removed file");
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn sweep_removes_old_dlq_files() {
        let dir = tempdir().unwrap();
        let old = dir.path().join("dlq-2000-01-01.log");
        let new = dir.path().join(format!(
            "dlq-{}.log",
            Utc::now().date_naive().format("%Y-%m-%d")
        ));
        fs::write(&old, b"old").unwrap();
        fs::write(&new, b"new").unwrap();

        sweep_old_dlq_files(dir.path(), 14).unwrap();
        assert!(!old.exists());
        assert!(new.exists());
    }
}
