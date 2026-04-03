use anyhow::{Context, Result};
use chrono::{Datelike, Utc};
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, watch};
use tracing::info;

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

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    break;
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
