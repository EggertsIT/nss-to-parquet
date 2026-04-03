use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use arrow_array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use chrono::{Datelike, Timelike, Utc};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, GzipLevel, ZstdLevel};
use parquet::file::properties::WriterProperties;
use tokio::sync::{mpsc, watch};
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
        close_partition_writer(state, ctx.metrics, ctx.durability)?;
    }
    if state.active.is_none() {
        let (writer, next_seq) = open_partition_writer(
            &ctx.cfg.writer.output_dir,
            key,
            state.next_file_seq,
            ctx.arrow_schema,
            ctx.compression,
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
        close_partition_writer(state, ctx.metrics, ctx.durability)?;
    }
    Ok(())
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

fn open_partition_writer(
    output_dir: &Path,
    partition_key: &str,
    file_seq: u64,
    arrow_schema: &Arc<arrow_schema::Schema>,
    compression: &Compression,
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
        },
        seq + 1,
    ))
}

fn close_partition_writer(
    state: &mut PartitionState,
    metrics: &Metrics,
    durability: Option<&Durability>,
) -> Result<()> {
    let Some(active) = state.active.take() else {
        return Ok(());
    };
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
    Ok(())
}

fn close_all_writers(
    states: &mut HashMap<String, PartitionState>,
    metrics: &Metrics,
    durability: Option<&Durability>,
) -> Result<()> {
    for state in states.values_mut() {
        close_partition_writer(state, metrics, durability)?;
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

    #[test]
    fn partition_path_format() {
        let dt = Utc
            .with_ymd_and_hms(2026, 4, 3, 12, 15, 0)
            .single()
            .unwrap();
        assert_eq!(partition_key(dt), "dt=2026-04-03/hour=12");
    }
}
