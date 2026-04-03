use std::sync::Arc;
use std::{net::IpAddr, str::FromStr};

use chrono::{DateTime, FixedOffset, NaiveDateTime, TimeZone, Utc};
use tokio::sync::{mpsc, watch};
use tracing::warn;

use crate::config::AppConfig;
use crate::durability::Durability;
use crate::metrics::Metrics;
use crate::schema::{LogicalType, SchemaDef};
use crate::types::{DlqRecord, ParsedRecord, RawRecord};

pub struct ParserCtx {
    pub schema: Arc<SchemaDef>,
    pub cfg: AppConfig,
    pub metrics: Arc<Metrics>,
    pub durability: Option<Arc<Durability>>,
}

pub async fn run_parser_loop(
    mut raw_rx: mpsc::Receiver<RawRecord>,
    parsed_tx: mpsc::Sender<ParsedRecord>,
    dlq_tx: mpsc::Sender<DlqRecord>,
    ctx: ParserCtx,
    shutdown: &mut watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    break;
                }
            }
            raw = raw_rx.recv() => {
                let Some(raw) = raw else { break; };

                match parse_record(&raw, &ctx.schema, &ctx.cfg) {
                    Ok(parsed) => {
                        ctx.metrics.observe_parsed_ok(1);
                        if parsed_tx.send(parsed).await.is_err() {
                            warn!("parser channel to writer closed");
                            break;
                        }
                    }
                    Err(err) => {
                        ctx.metrics.observe_parsed_error(1);
                        let spool_seq = raw.spool_seq;
                        let dlq = DlqRecord {
                            raw_line: raw.line,
                            reason: err.to_string(),
                            peer_addr: raw.peer_addr,
                            received_at: raw.received_at,
                        };
                        let dlq_write_ok = dlq_tx.send(dlq).await.is_ok();
                        if !dlq_write_ok {
                            warn!("dlq channel closed; parse-failed record not persisted to dlq");
                        }
                        if let Some(seq) = spool_seq
                            && let Some(durability) = ctx.durability.as_ref()
                            && dlq_write_ok
                        {
                            if let Err(err) = durability.commit(seq) {
                                ctx.metrics.observe_durability_error(1);
                                warn!(error = %err, seq, "failed to commit durability on parse error");
                            } else {
                                ctx.metrics.observe_durability_committed(1);
                            }
                        }
                    }
                }
            }
        }
    }
}

pub fn parse_record(
    raw: &RawRecord,
    schema: &SchemaDef,
    cfg: &AppConfig,
) -> anyhow::Result<ParsedRecord> {
    let fields = parse_csv_fields(&raw.line)?;
    if fields.len() != schema.fields.len() {
        anyhow::bail!(
            "field count mismatch: got {}, expected {}",
            fields.len(),
            schema.fields.len()
        );
    }

    let event_idx = schema
        .field_index(&cfg.schema.time_field)
        .ok_or_else(|| anyhow::anyhow!("time_field '{}' not in schema", cfg.schema.time_field))?;

    let event_value = fields
        .get(event_idx)
        .ok_or_else(|| anyhow::anyhow!("time field index out of range"))?;

    let event_time = parse_event_time(event_value, &cfg.schema.time_format, &cfg.schema.timezone)?;

    let values = fields
        .into_iter()
        .map(|value| normalize_field(&value))
        .collect::<Vec<_>>();

    if cfg.schema.strict_type_validation {
        validate_typed_values(schema, &values, cfg)?;
    }

    Ok(ParsedRecord {
        values,
        event_time,
        spool_seq: raw.spool_seq,
    })
}

pub fn count_csv_fields(line: &str) -> anyhow::Result<usize> {
    Ok(parse_csv_fields(line)?.len())
}

fn parse_csv_fields(line: &str) -> anyhow::Result<Vec<String>> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(false)
        .from_reader(line.as_bytes());

    let mut records = reader.records();
    let record = records
        .next()
        .ok_or_else(|| anyhow::anyhow!("empty record"))??;

    if records.next().transpose()?.is_some() {
        anyhow::bail!("multiple records found in a single line");
    }

    Ok(record.iter().map(ToString::to_string).collect())
}

fn normalize_field(value: &str) -> Option<String> {
    if value.is_empty() || value.eq_ignore_ascii_case("none") || value == "N/A" || value == "NA" {
        None
    } else {
        Some(value.to_string())
    }
}

fn validate_typed_values(
    schema: &SchemaDef,
    values: &[Option<String>],
    cfg: &AppConfig,
) -> anyhow::Result<()> {
    for (idx, field) in schema.fields.iter().enumerate() {
        let Some(value) = values[idx].as_deref() else {
            continue;
        };
        match field.logical_type {
            LogicalType::String => {}
            LogicalType::Int64 => {
                value.parse::<i64>().map_err(|err| {
                    anyhow::anyhow!(
                        "field '{}' expected int64, got '{}': {}",
                        field.name,
                        value,
                        err
                    )
                })?;
            }
            LogicalType::Float64 => {
                value.parse::<f64>().map_err(|err| {
                    anyhow::anyhow!(
                        "field '{}' expected float64, got '{}': {}",
                        field.name,
                        value,
                        err
                    )
                })?;
            }
            LogicalType::Boolean => {
                let normalized = value.to_ascii_lowercase();
                if !matches!(
                    normalized.as_str(),
                    "true" | "yes" | "1" | "false" | "no" | "0"
                ) {
                    anyhow::bail!("field '{}' expected boolean, got '{}'", field.name, value);
                }
            }
            LogicalType::Timestamp => {
                parse_event_time(value, &cfg.schema.time_format, &cfg.schema.timezone).map_err(
                    |err| {
                        anyhow::anyhow!(
                            "field '{}' expected timestamp, got '{}': {}",
                            field.name,
                            value,
                            err
                        )
                    },
                )?;
            }
            LogicalType::Ip => {
                IpAddr::from_str(value).map_err(|err| {
                    anyhow::anyhow!(
                        "field '{}' expected ip, got '{}': {}",
                        field.name,
                        value,
                        err
                    )
                })?;
            }
        }
    }
    Ok(())
}

pub fn parse_event_time(
    value: &str,
    format: &str,
    timezone: &str,
) -> anyhow::Result<DateTime<Utc>> {
    if let Ok(epoch) = value.parse::<i64>() {
        return DateTime::<Utc>::from_timestamp(epoch, 0)
            .ok_or_else(|| anyhow::anyhow!("invalid epoch timestamp '{}'", value));
    }

    if let Ok(dt) = DateTime::parse_from_rfc3339(value) {
        return Ok(dt.with_timezone(&Utc));
    }

    let naive = NaiveDateTime::parse_from_str(value, format)
        .map_err(|err| anyhow::anyhow!("failed to parse time '{}': {}", value, err))?;

    let offset = parse_timezone_offset(timezone)
        .ok_or_else(|| anyhow::anyhow!("unsupported timezone '{}'", timezone))?;

    let dt = offset
        .from_local_datetime(&naive)
        .single()
        .ok_or_else(|| anyhow::anyhow!("ambiguous local datetime '{}'", value))?;
    Ok(dt.with_timezone(&Utc))
}

fn parse_timezone_offset(input: &str) -> Option<FixedOffset> {
    let normalized = input.trim().to_uppercase();
    if normalized == "UTC" || normalized == "GMT" || normalized == "Z" {
        return FixedOffset::east_opt(0);
    }

    if let Some((sign, hh, mm)) = parse_offset_hhmm(&normalized) {
        let minutes = hh * 60 + mm;
        let seconds = minutes * 60;
        if sign == '-' {
            return FixedOffset::west_opt(seconds as i32);
        }
        return FixedOffset::east_opt(seconds as i32);
    }

    None
}

fn parse_offset_hhmm(input: &str) -> Option<(char, u32, u32)> {
    let mut chars = input.chars();
    let sign = chars.next()?;
    if sign != '+' && sign != '-' {
        return None;
    }
    let rest = chars.collect::<String>();
    if let Some((hh, mm)) = rest.split_once(':') {
        let hh = hh.parse::<u32>().ok()?;
        let mm = mm.parse::<u32>().ok()?;
        if hh <= 23 && mm <= 59 {
            return Some((sign, hh, mm));
        }
        return None;
    }

    if rest.len() == 4 {
        let hh = rest[0..2].parse::<u32>().ok()?;
        let mm = rest[2..4].parse::<u32>().ok()?;
        if hh <= 23 && mm <= 59 {
            return Some((sign, hh, mm));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AppConfig;
    use crate::schema::{FieldDef, LogicalType, SchemaDef};
    use crate::types::RawRecord;
    use chrono::Utc;

    #[test]
    fn parses_csv_with_quoted_values() {
        let line = "\"Mon Jun 20 15:29:11 2022\",\"new-gre\",\"ebay.com/path,with,comma\",\"Mozilla \"\"Test\"\"\"";
        let fields = parse_csv_fields(line).expect("parse fields");
        assert_eq!(fields.len(), 4);
        assert_eq!(fields[2], "ebay.com/path,with,comma");
        assert_eq!(fields[3], "Mozilla \"Test\"");
    }

    #[test]
    fn parse_event_time_from_epoch() {
        let dt = parse_event_time("1710000000", "%a %b %d %H:%M:%S %Y", "UTC").expect("time");
        assert_eq!(dt.timestamp(), 1_710_000_000);
    }

    #[test]
    fn parse_record_maps_schema_order() {
        let schema = SchemaDef {
            fields: vec![
                FieldDef {
                    name: "time".to_string(),
                    logical_type: LogicalType::Timestamp,
                    nullable: false,
                },
                FieldDef {
                    name: "action".to_string(),
                    logical_type: LogicalType::String,
                    nullable: true,
                },
            ],
        };

        let cfg = AppConfig::default();
        let raw = RawRecord {
            line: "\"Mon Jun 20 15:29:11 2022\",\"Blocked\"".to_string(),
            peer_addr: None,
            received_at: Utc::now(),
            spool_seq: None,
        };
        let parsed = parse_record(&raw, &schema, &cfg).expect("parsed");
        assert_eq!(parsed.values.len(), 2);
        assert_eq!(parsed.values[1], Some("Blocked".to_string()));
    }
}
