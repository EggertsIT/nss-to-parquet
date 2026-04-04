mod backfill;
mod config;
mod dlq;
mod durability;
mod metrics;
mod parser;
mod retention;
mod schema;
mod schema_generator;
mod server;
mod types;
mod writer;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tokio::sync::{mpsc, watch};
use tracing::{error, info};

use crate::backfill::run_direct_backfill;
use crate::config::AppConfig;
use crate::dlq::run_dlq_writer;
use crate::durability::Durability;
use crate::metrics::{
    ConfigOverview, Metrics, SchemaFieldOverview, SchemaOverview, StatsSettings, run_metrics_server,
};
use crate::parser::{ParserCtx, run_parser_loop};
use crate::retention::run_retention_loop;
use crate::schema::SchemaDef;
use crate::schema_generator::generate_schema_from_feed_template;
use crate::server::run_tcp_listener;
use crate::types::{DlqRecord, ParsedRecord, RawRecord};
use crate::writer::run_parquet_writer;

#[derive(Parser, Debug)]
#[command(name = "nss-ingestor")]
#[command(about = "Ingest Zscaler NSS logs over TCP and write Parquet partitions")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Run {
        #[arg(long)]
        config: PathBuf,
    },
    ValidateConfig {
        #[arg(long)]
        config: PathBuf,
    },
    ValidateSchema {
        #[arg(long)]
        schema: PathBuf,
        #[arg(long)]
        sample: Option<PathBuf>,
    },
    GenerateSchema {
        #[arg(long, conflicts_with = "feed_template_file")]
        feed_template: Option<String>,
        #[arg(long, conflicts_with = "feed_template")]
        feed_template_file: Option<PathBuf>,
        #[arg(long)]
        output: PathBuf,
        #[arg(long, default_value_t = false)]
        force: bool,
    },
    BackfillDirect {
        #[arg(long)]
        config: PathBuf,
        #[arg(long, default_value_t = 100_000)]
        total_rows: u64,
        #[arg(long, default_value_t = 13)]
        days: u32,
        #[arg(long, default_value_t = 8)]
        workers: usize,
        #[arg(long, default_value_t = 20260404)]
        seed: u64,
        #[arg(long, default_value_t = 1_000_000)]
        progress_every: u64,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();

    match cli.command {
        Command::Run { config } => run(config).await,
        Command::ValidateConfig { config } => validate_config(config),
        Command::ValidateSchema { schema, sample } => validate_schema(schema, sample).await,
        Command::GenerateSchema {
            feed_template,
            feed_template_file,
            output,
            force,
        } => generate_schema(feed_template, feed_template_file, output, force),
        Command::BackfillDirect {
            config,
            total_rows,
            days,
            workers,
            seed,
            progress_every,
        } => run_backfill_direct(config, total_rows, days, workers, seed, progress_every).await,
    }
}

fn init_tracing() {
    let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .init();
}

async fn run(config_path: PathBuf) -> Result<()> {
    let cfg = AppConfig::load(&config_path)?;
    let schema = Arc::new(SchemaDef::load(&cfg.schema.path)?);
    schema.validate()?;
    let durability = Durability::initialize(&cfg.durability)?;

    if schema.field_index(&cfg.schema.time_field).is_none() {
        anyhow::bail!(
            "time_field '{}' does not exist in schema",
            cfg.schema.time_field
        );
    }

    std::fs::create_dir_all(&cfg.writer.output_dir).with_context(|| {
        format!(
            "failed to create writer output dir {}",
            cfg.writer.output_dir.display()
        )
    })?;
    let metrics_state_path = cfg.writer.output_dir.join(".metrics-state.json");
    let metrics = Arc::new(Metrics::new_with_persistence(
        cfg.metrics.stats_window_hours.max(1),
        Some(metrics_state_path),
    ));
    let config_overview = ConfigOverview {
        config_path: config_path.display().to_string(),
        resolved_config: serde_json::to_value(&cfg).context("failed to serialize config")?,
    };
    let schema_overview = SchemaOverview {
        schema_path: cfg.schema.path.display().to_string(),
        time_field: cfg.schema.time_field.clone(),
        time_format: cfg.schema.time_format.clone(),
        timezone: cfg.schema.timezone.clone(),
        strict_type_validation: cfg.schema.strict_type_validation,
        field_count: schema.fields.len(),
        fields: schema
            .fields
            .iter()
            .enumerate()
            .map(|(index, field)| SchemaFieldOverview {
                index,
                name: field.name.clone(),
                logical_type: field.logical_type.as_str().to_string(),
                nullable: field.nullable,
            })
            .collect(),
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let (raw_tx, raw_rx) = mpsc::channel::<RawRecord>(cfg.listener.ingress_channel_capacity);
    let (parsed_tx, parsed_rx) =
        mpsc::channel::<ParsedRecord>(cfg.listener.parsed_channel_capacity);
    let (dlq_tx, dlq_rx) = mpsc::channel::<DlqRecord>(cfg.dlq.channel_capacity);

    let mut metrics_task = if cfg.metrics.enabled {
        let addr = cfg.metrics.bind_addr.clone();
        let metrics = Arc::clone(&metrics);
        let settings = StatsSettings {
            dashboard_enabled: cfg.metrics.dashboard_enabled,
            degraded_error_ratio: cfg.metrics.degraded_error_ratio,
            critical_error_ratio: cfg.metrics.critical_error_ratio,
            degraded_stale_seconds: cfg.metrics.degraded_stale_seconds,
            critical_stale_seconds: cfg.metrics.critical_stale_seconds,
        };
        let schema_overview = schema_overview.clone();
        let config_overview = config_overview.clone();
        let shutdown = shutdown_rx.clone();
        Some(tokio::spawn(async move {
            run_metrics_server(
                addr,
                metrics,
                settings,
                schema_overview,
                config_overview,
                shutdown,
            )
            .await
        }))
    } else {
        None
    };

    let dlq_cfg = cfg.dlq.clone();
    let dlq_metrics = Arc::clone(&metrics);
    let mut dlq_shutdown = shutdown_rx.clone();
    let mut dlq_task = tokio::spawn(async move {
        run_dlq_writer(dlq_cfg, dlq_rx, dlq_metrics, &mut dlq_shutdown).await
    });

    let parser_schema = Arc::clone(&schema);
    let parser_cfg = cfg.clone();
    let parser_metrics = Arc::clone(&metrics);
    let parser_dlq = dlq_tx.clone();
    let parser_durability = durability.clone();
    let parser_ctx = ParserCtx {
        schema: parser_schema,
        cfg: parser_cfg,
        metrics: parser_metrics,
        durability: parser_durability,
    };
    let mut parser_shutdown = shutdown_rx.clone();
    let mut parser_task = tokio::spawn(async move {
        run_parser_loop(
            raw_rx,
            parsed_tx,
            parser_dlq,
            parser_ctx,
            &mut parser_shutdown,
        )
        .await;
        Ok::<(), anyhow::Error>(())
    });

    let writer_cfg = cfg.clone();
    let writer_schema = Arc::clone(&schema);
    let writer_metrics = Arc::clone(&metrics);
    let writer_durability = durability.clone();
    let mut writer_shutdown = shutdown_rx.clone();
    let mut writer_task = tokio::spawn(async move {
        run_parquet_writer(
            writer_cfg,
            writer_schema,
            parsed_rx,
            writer_metrics,
            writer_durability,
            &mut writer_shutdown,
        )
        .await
    });

    let retention_cfg = cfg.clone();
    let mut retention_shutdown = shutdown_rx.clone();
    let retention_task =
        tokio::spawn(
            async move { run_retention_loop(retention_cfg, &mut retention_shutdown).await },
        );

    if let Some(durability) = durability.as_ref() {
        let replay = durability.replay_uncommitted()?;
        if !replay.is_empty() {
            info!(
                count = replay.len(),
                "replaying uncommitted records from durability log"
            );
        }
        for record in replay {
            metrics.observe_durability_replayed(1);
            if raw_tx.send(record).await.is_err() {
                anyhow::bail!("failed to enqueue replayed record: raw channel closed");
            }
        }
    }

    info!(bind = %cfg.listener.bind_addr, "starting nss ingestor");

    let listener_cfg = cfg.clone();
    let listener_metrics = Arc::clone(&metrics);
    let listener_dlq = dlq_tx.clone();
    let listener_durability = durability.clone();
    let mut listener_shutdown = shutdown_rx.clone();
    let mut listener_task = tokio::spawn(async move {
        run_tcp_listener(
            listener_cfg,
            raw_tx,
            listener_dlq,
            listener_metrics,
            listener_durability,
            &mut listener_shutdown,
        )
        .await
    });
    let mut fatal_error: Option<anyhow::Error> = None;
    let mut listener_finished = false;
    let mut writer_finished = false;
    let mut dlq_finished = false;
    let mut parser_finished = false;
    let mut metrics_finished = false;

    #[cfg(unix)]
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("failed to register SIGTERM handler")?;

    #[cfg(unix)]
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!(signal = "SIGINT", "received shutdown signal");
        }
        _ = sigterm.recv() => {
            info!(signal = "SIGTERM", "received shutdown signal");
        }
        result = &mut listener_task => {
            listener_finished = true;
            match join_task("listener", result) {
                Ok(()) => set_first_error(&mut fatal_error, anyhow::anyhow!("listener stopped unexpectedly")),
                Err(err) => set_first_error(&mut fatal_error, err),
            }
        }
        result = &mut writer_task => {
            writer_finished = true;
            match join_task("parquet writer", result) {
                Ok(()) => set_first_error(&mut fatal_error, anyhow::anyhow!("parquet writer stopped unexpectedly")),
                Err(err) => set_first_error(&mut fatal_error, err),
            }
        }
        result = &mut dlq_task => {
            dlq_finished = true;
            match join_task("dlq writer", result) {
                Ok(()) => set_first_error(&mut fatal_error, anyhow::anyhow!("dlq writer stopped unexpectedly")),
                Err(err) => set_first_error(&mut fatal_error, err),
            }
        }
        result = &mut parser_task => {
            parser_finished = true;
            match join_task("parser", result) {
                Ok(()) => set_first_error(&mut fatal_error, anyhow::anyhow!("parser stopped unexpectedly")),
                Err(err) => set_first_error(&mut fatal_error, err),
            }
        }
        maybe_metrics_result = async {
            if let Some(task) = &mut metrics_task {
                Some(task.await)
            } else {
                None
            }
        }, if metrics_task.is_some() => {
            metrics_finished = true;
            if let Some(result) = maybe_metrics_result {
                match join_task("metrics server", result) {
                    Ok(()) => set_first_error(&mut fatal_error, anyhow::anyhow!("metrics server stopped unexpectedly")),
                    Err(err) => set_first_error(&mut fatal_error, err),
                }
            }
        }
    }

    #[cfg(not(unix))]
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!(signal = "SIGINT", "received shutdown signal");
        }
        result = &mut listener_task => {
            listener_finished = true;
            match join_task("listener", result) {
                Ok(()) => set_first_error(&mut fatal_error, anyhow::anyhow!("listener stopped unexpectedly")),
                Err(err) => set_first_error(&mut fatal_error, err),
            }
        }
        result = &mut writer_task => {
            writer_finished = true;
            match join_task("parquet writer", result) {
                Ok(()) => set_first_error(&mut fatal_error, anyhow::anyhow!("parquet writer stopped unexpectedly")),
                Err(err) => set_first_error(&mut fatal_error, err),
            }
        }
        result = &mut dlq_task => {
            dlq_finished = true;
            match join_task("dlq writer", result) {
                Ok(()) => set_first_error(&mut fatal_error, anyhow::anyhow!("dlq writer stopped unexpectedly")),
                Err(err) => set_first_error(&mut fatal_error, err),
            }
        }
        result = &mut parser_task => {
            parser_finished = true;
            match join_task("parser", result) {
                Ok(()) => set_first_error(&mut fatal_error, anyhow::anyhow!("parser stopped unexpectedly")),
                Err(err) => set_first_error(&mut fatal_error, err),
            }
        }
        maybe_metrics_result = async {
            if let Some(task) = &mut metrics_task {
                Some(task.await)
            } else {
                None
            }
        }, if metrics_task.is_some() => {
            metrics_finished = true;
            if let Some(result) = maybe_metrics_result {
                match join_task("metrics server", result) {
                    Ok(()) => set_first_error(&mut fatal_error, anyhow::anyhow!("metrics server stopped unexpectedly")),
                    Err(err) => set_first_error(&mut fatal_error, err),
                }
            }
        }
    }

    let _ = shutdown_tx.send(true);

    if !listener_finished && let Err(err) = join_task("listener", listener_task.await) {
        set_first_error(&mut fatal_error, err);
    }
    if !writer_finished && let Err(err) = join_task("parquet writer", writer_task.await) {
        set_first_error(&mut fatal_error, err);
    }
    if !dlq_finished && let Err(err) = join_task("dlq writer", dlq_task.await) {
        set_first_error(&mut fatal_error, err);
    }
    if !parser_finished && let Err(err) = join_task("parser", parser_task.await) {
        set_first_error(&mut fatal_error, err);
    }
    if !metrics_finished
        && let Some(task) = metrics_task
        && let Err(err) = join_task("metrics server", task.await)
    {
        set_first_error(&mut fatal_error, err);
    }
    if let Err(err) = join_task("retention", retention_task.await) {
        set_first_error(&mut fatal_error, err);
    }
    if let Err(err) = metrics.persist_now() {
        error!(error = %err, "failed to persist metrics state on shutdown");
    }

    if let Some(err) = fatal_error {
        error!(error = %err, "shutting down due to worker failure");
        return Err(err);
    }
    info!("nss ingestor stopped");
    Ok(())
}

fn join_task(
    name: &str,
    outcome: std::result::Result<Result<()>, tokio::task::JoinError>,
) -> Result<()> {
    match outcome {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => Err(err).with_context(|| format!("{name} failed")),
        Err(err) => anyhow::bail!("{name} join error: {err}"),
    }
}

fn set_first_error(slot: &mut Option<anyhow::Error>, err: anyhow::Error) {
    let incoming = err.to_string();
    let incoming_generic = incoming.contains("stopped unexpectedly");
    match slot {
        None => *slot = Some(err),
        Some(existing) => {
            let existing_generic = existing.to_string().contains("stopped unexpectedly");
            if existing_generic && !incoming_generic {
                *slot = Some(err);
            }
        }
    }
}

fn validate_config(config_path: PathBuf) -> Result<()> {
    let cfg = AppConfig::load(&config_path)?;
    let schema = SchemaDef::load(&cfg.schema.path)
        .with_context(|| format!("failed to load schema from {}", cfg.schema.path.display()))?;
    schema.validate()?;
    info!(
        listener = %cfg.listener.bind_addr,
        output = %cfg.writer.output_dir.display(),
        schema_fields = schema.fields.len(),
        "config is valid"
    );
    Ok(())
}

async fn validate_schema(schema_path: PathBuf, sample: Option<PathBuf>) -> Result<()> {
    let schema = SchemaDef::load(&schema_path)?;
    schema.validate()?;
    info!(fields = schema.fields.len(), "schema is valid");

    if let Some(sample_file) = sample {
        let content = tokio::fs::read_to_string(&sample_file)
            .await
            .with_context(|| format!("failed to read sample file {}", sample_file.display()))?;
        let mut checked = 0usize;
        for line in content
            .lines()
            .filter(|line| !line.trim().is_empty())
            .take(100)
        {
            let count = parser::count_csv_fields(line)?;
            if count != schema.fields.len() {
                anyhow::bail!(
                    "sample line {} has {} fields, schema has {}",
                    checked + 1,
                    count,
                    schema.fields.len()
                );
            }
            checked += 1;
        }
        info!(checked, "sample validation passed");
    }

    Ok(())
}

fn generate_schema(
    feed_template: Option<String>,
    feed_template_file: Option<PathBuf>,
    output: PathBuf,
    force: bool,
) -> Result<()> {
    let template = match (feed_template, feed_template_file) {
        (Some(s), None) => s,
        (None, Some(path)) => std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read feed template file {}", path.display()))?,
        (None, None) => anyhow::bail!("provide either --feed-template or --feed-template-file"),
        (Some(_), Some(_)) => {
            anyhow::bail!("provide only one of --feed-template or --feed-template-file")
        }
    };

    if output.exists() && !force {
        anyhow::bail!(
            "output file {} already exists (use --force to overwrite)",
            output.display()
        );
    }
    if let Some(parent) = output.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create output directory {}", parent.display()))?;
    }

    let schema = generate_schema_from_feed_template(&template)?;
    let yaml = serde_yaml::to_string(&schema)?;
    std::fs::write(&output, yaml)
        .with_context(|| format!("failed to write schema to {}", output.display()))?;
    info!(
        output = %output.display(),
        fields = schema.fields.len(),
        "schema generated from feed template"
    );
    Ok(())
}

async fn run_backfill_direct(
    config_path: PathBuf,
    total_rows: u64,
    days: u32,
    workers: usize,
    seed: u64,
    progress_every: u64,
) -> Result<()> {
    let cfg = AppConfig::load(&config_path)?;
    let schema = Arc::new(SchemaDef::load(&cfg.schema.path)?);
    schema.validate()?;
    run_direct_backfill(cfg, schema, total_rows, days, workers, seed, progress_every).await
}
