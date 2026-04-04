use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use arrow_array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use chrono::{Datelike, Timelike, Utc};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, GzipLevel, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use serde::Serialize;
use tokio::sync::{mpsc, oneshot, watch};
use tracing::info;

use crate::config::AppConfig;
use crate::durability::Durability;
use crate::metrics::Metrics;
use crate::schema::{LogicalType, SchemaDef};
use crate::types::ParsedRecord;

struct ActiveWriter {
    writer: ArrowWriter<std::fs::File>,
    rows_in_file: usize,
    tmp_path: PathBuf,
    final_path: PathBuf,
    max_spool_seq: Option<u64>,
    opened_at: Instant,
}

#[derive(Debug)]
pub enum WriterControlMessage {
    ForceFinalizeOpenFiles {
        respond_to: oneshot::Sender<std::result::Result<ForceFinalizeOpenFilesResult, String>>,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct ForceFinalizeOpenFilesResult {
    pub finalized_files: u64,
    pub finalized_rows: u64,
    pub skipped_empty_writers: u64,
}

#[derive(Default)]
struct PartitionState {
    active: Option<ActiveWriter>,
    next_file_seq: u64,
}

pub async fn run_parquet_writer(
    cfg: AppConfig,
    schema: Arc<SchemaDef>,
    mut parsed_rx: mpsc::Receiver<ParsedRecord>,
    mut control_rx: mpsc::Receiver<WriterControlMessage>,
    metrics: Arc<Metrics>,
    durability: Option<Arc<Durability>>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<()> {
    std::fs::create_dir_all(&cfg.writer.output_dir).with_context(|| {
        format!(
            "failed to create output dir {}",
            cfg.writer.output_dir.display()
        )
    })?;
    let removed_tmp = cleanup_orphan_parquet_tmp_files(&cfg.writer.output_dir)?;
    if removed_tmp > 0 {
        info!(removed_tmp, "cleaned orphan parquet temp files at startup");
    }

    let event_time_idx = schema
        .field_index(&cfg.schema.time_field)
        .ok_or_else(|| anyhow::anyhow!("time_field '{}' not in schema", cfg.schema.time_field))?;
    let arrow_schema = schema.arrow_schema();
    let compression = compression_from_str(&cfg.writer.compression)?;
    let ctx = FlushCtx {
        cfg: &cfg,
        schema: &schema,
        event_time_idx,
        arrow_schema: &arrow_schema,
        compression: &compression,
        metrics: &metrics,
        durability: durability.as_deref(),
    };

    let mut pending: HashMap<String, Vec<ParsedRecord>> = HashMap::new();
    let mut states: HashMap<String, PartitionState> = HashMap::new();
    let mut flush_tick =
        tokio::time::interval(Duration::from_secs(cfg.writer.flush_interval_secs.max(1)));
    let mut control_channel_open = true;

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    break;
                }
            }
            _ = flush_tick.tick() => {
                flush_all_partitions(&ctx, &mut pending, &mut states)?;
            }
            maybe_control = control_rx.recv(), if control_channel_open => {
                match maybe_control {
                    Some(control) => {
                        handle_writer_control(control, &ctx, &mut pending, &mut states);
                    }
                    None => {
                        control_channel_open = false;
                    }
                }
            }
            maybe = parsed_rx.recv() => {
                let Some(record) = maybe else { break; };
                let key = partition_key(record.event_time);
                let rows = pending.entry(key.clone()).or_default();
                rows.push(record);

                if rows.len() >= cfg.writer.batch_rows {
                    flush_partition(&ctx, &key, &mut pending, &mut states)?;
                }
            }
        }
    }

    flush_all_partitions(&ctx, &mut pending, &mut states)?;
    close_all_writers(&mut states, &metrics, durability.as_deref())?;
    info!("parquet writer stopped");
    Ok(())
}

fn handle_writer_control(
    control: WriterControlMessage,
    ctx: &FlushCtx<'_>,
    pending: &mut HashMap<String, Vec<ParsedRecord>>,
    states: &mut HashMap<String, PartitionState>,
) {
    match control {
        WriterControlMessage::ForceFinalizeOpenFiles { respond_to } => {
            let result = (|| -> Result<ForceFinalizeOpenFilesResult> {
                flush_all_partitions(ctx, pending, states)?;
                force_finalize_open_files(states, ctx.metrics, ctx.durability)
            })();

            let response = result.map_err(|err| err.to_string());
            let _ = respond_to.send(response);
        }
    }
}

struct FlushCtx<'a> {
    cfg: &'a AppConfig,
    schema: &'a SchemaDef,
    event_time_idx: usize,
    arrow_schema: &'a Arc<arrow_schema::Schema>,
    compression: &'a Compression,
    metrics: &'a Metrics,
    durability: Option<&'a Durability>,
}

fn flush_all_partitions(
    ctx: &FlushCtx<'_>,
    pending: &mut HashMap<String, Vec<ParsedRecord>>,
    states: &mut HashMap<String, PartitionState>,
) -> Result<()> {
    let keys = pending.keys().cloned().collect::<Vec<_>>();
    for key in keys {
        flush_partition(ctx, &key, pending, states)?;
    }
    rotate_expired_partitions(ctx, states)?;
    Ok(())
}

fn rotate_expired_partitions(
    ctx: &FlushCtx<'_>,
    states: &mut HashMap<String, PartitionState>,
) -> Result<()> {
    let max_age_secs = ctx.cfg.writer.max_file_age_secs;
    if max_age_secs == 0 {
        return Ok(());
    }
    let max_age = Duration::from_secs(max_age_secs);
    for state in states.values_mut() {
        if should_rotate_by_age(state, max_age) {
            let _ = close_partition_writer(state, ctx.metrics, ctx.durability)?;
        }
    }
    Ok(())
}

fn flush_partition(
    ctx: &FlushCtx<'_>,
    key: &str,
    pending: &mut HashMap<String, Vec<ParsedRecord>>,
    states: &mut HashMap<String, PartitionState>,
) -> Result<()> {
    let Some(records) = pending.remove(key) else {
        return Ok(());
    };
    if records.is_empty() {
        return Ok(());
    }

    let batch = build_record_batch(ctx.schema, ctx.event_time_idx, ctx.arrow_schema, &records)?;
    let batch_rows = batch.num_rows();
    let max_spool_seq = records.iter().filter_map(|record| record.spool_seq).max();
    let state = states.entry(key.to_string()).or_default();

    if should_rotate(state, batch_rows, ctx.cfg.writer.target_file_rows) {
        let _ = close_partition_writer(state, ctx.metrics, ctx.durability)?;
    }
    if state.active.is_none() {
        let (writer, next_seq) = open_partition_writer(
            &ctx.cfg.writer.output_dir,
            key,
            state.next_file_seq,
            ctx.arrow_schema,
            ctx.compression,
            &ctx.cfg.schema.profile,
            ctx.cfg.schema.custom_schema_mode,
        )?;
        state.active = Some(writer);
        state.next_file_seq = next_seq;
    }

    if let Some(active) = state.active.as_mut() {
        active.writer.write(&batch)?;
        active.rows_in_file += batch_rows;
        ctx.metrics.observe_written_rows(batch_rows as u64);
        if let Some(max_seq) = max_spool_seq {
            active.max_spool_seq = Some(active.max_spool_seq.unwrap_or(0).max(max_seq));
        }
    }
    if should_rotate(state, 0, ctx.cfg.writer.target_file_rows) {
        let _ = close_partition_writer(state, ctx.metrics, ctx.durability)?;
    }
    Ok(())
}

fn force_finalize_open_files(
    states: &mut HashMap<String, PartitionState>,
    metrics: &Metrics,
    durability: Option<&Durability>,
) -> Result<ForceFinalizeOpenFilesResult> {
    let mut finalized_files = 0_u64;
    let mut finalized_rows = 0_u64;
    let mut skipped_empty_writers = 0_u64;

    for state in states.values_mut() {
        let should_finalize = state
            .active
            .as_ref()
            .map(|active| active.rows_in_file > 0)
            .unwrap_or(false);
        if !should_finalize {
            if state.active.is_some() {
                skipped_empty_writers += 1;
            }
            continue;
        }
        if let Some(closed) = close_partition_writer(state, metrics, durability)? {
            finalized_files += 1;
            finalized_rows += closed.rows_in_file as u64;
        }
    }

    Ok(ForceFinalizeOpenFilesResult {
        finalized_files,
        finalized_rows,
        skipped_empty_writers,
    })
}

fn should_rotate(state: &PartitionState, incoming_rows: usize, target_rows: usize) -> bool {
    if target_rows == 0 {
        return false;
    }
    let Some(active) = state.active.as_ref() else {
        return false;
    };
    active.rows_in_file > 0 && (active.rows_in_file + incoming_rows >= target_rows)
}

fn should_rotate_by_age(state: &PartitionState, max_age: Duration) -> bool {
    let Some(active) = state.active.as_ref() else {
        return false;
    };
    active.rows_in_file > 0 && active.opened_at.elapsed() >= max_age
}

fn open_partition_writer(
    output_dir: &Path,
    partition_key: &str,
    file_seq: u64,
    arrow_schema: &Arc<arrow_schema::Schema>,
    compression: &Compression,
    schema_profile: &str,
    custom_schema_mode: bool,
) -> Result<(ActiveWriter, u64)> {
    let partition_dir = output_dir.join(partition_key);
    std::fs::create_dir_all(&partition_dir)
        .with_context(|| format!("failed to create partition dir {}", partition_dir.display()))?;
    let mut seq = file_seq;
    let (final_path, tmp_path) = loop {
        let final_path = partition_dir.join(format!("part-{seq:06}.parquet"));
        let tmp_path = partition_dir.join(format!(".part-{seq:06}.parquet.tmp"));
        if !final_path.exists() && !tmp_path.exists() {
            break (final_path, tmp_path);
        }
        seq += 1;
    };
    let file = std::fs::File::create(&tmp_path)
        .with_context(|| format!("failed creating parquet temp file {}", tmp_path.display()))?;
    let props = WriterProperties::builder()
        .set_compression(*compression)
        .set_key_value_metadata(Some(vec![
            KeyValue::new(
                "nss_ingestor_schema_profile".to_string(),
                schema_profile.to_string(),
            ),
            KeyValue::new(
                "nss_ingestor_custom_schema_mode".to_string(),
                custom_schema_mode.to_string(),
            ),
        ]))
        .build();
    let writer =
        ArrowWriter::try_new(file, Arc::clone(arrow_schema), Some(props)).with_context(|| {
            format!(
                "failed initializing parquet writer for {}",
                tmp_path.display()
            )
        })?;
    Ok((
        ActiveWriter {
            writer,
            rows_in_file: 0,
            tmp_path,
            final_path,
            max_spool_seq: None,
            opened_at: Instant::now(),
        },
        seq + 1,
    ))
}

fn close_partition_writer(
    state: &mut PartitionState,
    metrics: &Metrics,
    durability: Option<&Durability>,
) -> Result<Option<ClosedFileStats>> {
    let Some(active) = state.active.take() else {
        return Ok(None);
    };
    let rows_in_file = active.rows_in_file;
    let _metadata = active.writer.close()?;
    std::fs::rename(&active.tmp_path, &active.final_path).with_context(|| {
        format!(
            "failed atomically finalizing parquet file {} -> {}",
            active.tmp_path.display(),
            active.final_path.display()
        )
    })?;
    if let Some(max_seq) = active.max_spool_seq
        && let Some(durability) = durability
    {
        if let Err(err) = durability.commit(max_seq) {
            metrics.observe_durability_error(1);
            return Err(err);
        }
        metrics.observe_durability_committed(1);
    }
    metrics.observe_written_files(1);
    Ok(Some(ClosedFileStats { rows_in_file }))
}

struct ClosedFileStats {
    rows_in_file: usize,
}

fn close_all_writers(
    states: &mut HashMap<String, PartitionState>,
    metrics: &Metrics,
    durability: Option<&Durability>,
) -> Result<()> {
    for state in states.values_mut() {
        let _ = close_partition_writer(state, metrics, durability)?;
    }
    Ok(())
}

fn build_record_batch(
    schema: &SchemaDef,
    event_time_idx: usize,
    arrow_schema: &Arc<arrow_schema::Schema>,
    records: &[ParsedRecord],
) -> Result<RecordBatch> {
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.fields.len());

    for (idx, field) in schema.fields.iter().enumerate() {
        let arr: ArrayRef = match field.logical_type {
            LogicalType::String | LogicalType::Ip => {
                let values = records
                    .iter()
                    .map(|r| r.values[idx].clone())
                    .collect::<Vec<Option<String>>>();
                Arc::new(StringArray::from(values))
            }
            LogicalType::Int64 => {
                let values = records
                    .iter()
                    .map(|r| parse_i64(r.values[idx].as_deref()))
                    .collect::<Vec<Option<i64>>>();
                Arc::new(Int64Array::from(values))
            }
            LogicalType::Float64 => {
                let values = records
                    .iter()
                    .map(|r| parse_f64(r.values[idx].as_deref()))
                    .collect::<Vec<Option<f64>>>();
                Arc::new(Float64Array::from(values))
            }
            LogicalType::Boolean => {
                let values = records
                    .iter()
                    .map(|r| parse_bool(r.values[idx].as_deref()))
                    .collect::<Vec<Option<bool>>>();
                Arc::new(BooleanArray::from(values))
            }
            LogicalType::Timestamp => {
                let values = records
                    .iter()
                    .map(|r| {
                        if idx == event_time_idx {
                            Some(r.event_time.timestamp_micros())
                        } else {
                            parse_timestamp_micros(r.values[idx].as_deref())
                        }
                    })
                    .collect::<Vec<Option<i64>>>();
                Arc::new(TimestampMicrosecondArray::from(values))
            }
        };
        arrays.push(arr);
    }

    RecordBatch::try_new(Arc::clone(arrow_schema), arrays).map_err(Into::into)
}

fn partition_key(ts: chrono::DateTime<Utc>) -> String {
    format!(
        "dt={:04}-{:02}-{:02}/hour={:02}",
        ts.year(),
        ts.month(),
        ts.day(),
        ts.hour()
    )
}

fn compression_from_str(input: &str) -> Result<Compression> {
    let value = input.trim().to_lowercase();
    match value.as_str() {
        "zstd" | "zst" => Ok(Compression::ZSTD(ZstdLevel::try_new(3)?)),
        "snappy" => Ok(Compression::SNAPPY),
        "gzip" | "gz" => Ok(Compression::GZIP(GzipLevel::default())),
        other => anyhow::bail!("unsupported compression '{}'", other),
    }
}

fn cleanup_orphan_parquet_tmp_files(output_dir: &Path) -> Result<u64> {
    if !output_dir.exists() {
        return Ok(0);
    }

    let mut removed = 0_u64;
    let mut stack = vec![output_dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir)
            .with_context(|| format!("failed to list directory {}", dir.display()))?
        {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                stack.push(entry.path());
                continue;
            }
            if !file_type.is_file() {
                continue;
            }
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.ends_with(".parquet.tmp") {
                std::fs::remove_file(entry.path()).with_context(|| {
                    format!(
                        "failed removing orphan parquet temp {}",
                        entry.path().display()
                    )
                })?;
                removed += 1;
            }
        }
    }

    Ok(removed)
}

fn parse_i64(value: Option<&str>) -> Option<i64> {
    value.and_then(|v| v.parse::<i64>().ok())
}

fn parse_f64(value: Option<&str>) -> Option<f64> {
    value.and_then(|v| v.parse::<f64>().ok())
}

fn parse_bool(value: Option<&str>) -> Option<bool> {
    value.and_then(|v| match v.to_ascii_lowercase().as_str() {
        "true" | "yes" | "1" => Some(true),
        "false" | "no" | "0" => Some(false),
        _ => None,
    })
}

fn parse_timestamp_micros(value: Option<&str>) -> Option<i64> {
    let value = value?;
    if let Ok(epoch) = value.parse::<i64>() {
        if epoch.abs() >= 1_000_000_000_000_000 {
            return Some(epoch);
        }
        if epoch.abs() >= 1_000_000_000_000 {
            return Some(epoch * 1_000);
        }
        return Some(epoch * 1_000_000);
    }
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(value) {
        return Some(dt.with_timezone(&Utc).timestamp_micros());
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn partition_path_format() {
        let dt = Utc
            .with_ymd_and_hms(2026, 4, 3, 12, 15, 0)
            .single()
            .unwrap();
        assert_eq!(partition_key(dt), "dt=2026-04-03/hour=12");
    }

    #[test]
    fn cleanup_orphan_tmp_files_removes_temp_files() {
        let dir = tempdir().unwrap();
        let partition = dir.path().join("dt=2026-04-03").join("hour=12");
        fs::create_dir_all(&partition).unwrap();
        let tmp = partition.join(".part-000001.parquet.tmp");
        let final_file = partition.join("part-000001.parquet");
        fs::write(&tmp, b"partial").unwrap();
        fs::write(&final_file, b"done").unwrap();

        let removed = cleanup_orphan_parquet_tmp_files(dir.path()).unwrap();
        assert_eq!(removed, 1);
        assert!(!tmp.exists());
        assert!(final_file.exists());
    }
}
