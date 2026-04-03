use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use anyhow::Result;
use axum::extract::State;
use axum::response::Html;
use axum::routing::get;
use axum::{Json, Router};
use chrono::Utc;
use serde::Serialize;
use tokio::sync::watch;

#[derive(Debug, Clone)]
pub struct StatsSettings {
    pub dashboard_enabled: bool,
    pub degraded_error_ratio: f64,
    pub critical_error_ratio: f64,
    pub degraded_stale_seconds: u64,
    pub critical_stale_seconds: u64,
}

impl Default for StatsSettings {
    fn default() -> Self {
        Self {
            dashboard_enabled: true,
            degraded_error_ratio: 0.02,
            critical_error_ratio: 0.10,
            degraded_stale_seconds: 300,
            critical_stale_seconds: 900,
        }
    }
}

#[derive(Debug)]
pub struct Metrics {
    raw_received: AtomicU64,
    parsed_ok: AtomicU64,
    parsed_error: AtomicU64,
    written_rows: AtomicU64,
    written_files: AtomicU64,
    dlq_rows: AtomicU64,
    durability_replayed: AtomicU64,
    durability_commits: AtomicU64,
    durability_errors: AtomicU64,
    connection_dropped: AtomicU64,
    connection_timeouts: AtomicU64,
    started_at_epoch: i64,
    last_ingest_epoch: AtomicI64,
    last_write_epoch: AtomicI64,
    timeline: Mutex<TimeSeriesWindow>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new(24)
    }
}

impl Metrics {
    pub fn new(window_hours: u32) -> Self {
        let now = Utc::now().timestamp();
        Self {
            raw_received: AtomicU64::new(0),
            parsed_ok: AtomicU64::new(0),
            parsed_error: AtomicU64::new(0),
            written_rows: AtomicU64::new(0),
            written_files: AtomicU64::new(0),
            dlq_rows: AtomicU64::new(0),
            durability_replayed: AtomicU64::new(0),
            durability_commits: AtomicU64::new(0),
            durability_errors: AtomicU64::new(0),
            connection_dropped: AtomicU64::new(0),
            connection_timeouts: AtomicU64::new(0),
            started_at_epoch: now,
            last_ingest_epoch: AtomicI64::new(0),
            last_write_epoch: AtomicI64::new(0),
            timeline: Mutex::new(TimeSeriesWindow::new(window_hours.max(1))),
        }
    }

    pub fn observe_raw_received(&self, n: u64) {
        self.raw_received.fetch_add(n, Ordering::Relaxed);
        let now = Utc::now().timestamp();
        self.last_ingest_epoch.store(now, Ordering::Relaxed);
        self.record_timeline(TimelineKind::Ingested, n, now);
    }

    pub fn observe_parsed_ok(&self, n: u64) {
        self.parsed_ok.fetch_add(n, Ordering::Relaxed);
    }

    pub fn observe_parsed_error(&self, n: u64) {
        self.parsed_error.fetch_add(n, Ordering::Relaxed);
        let now = Utc::now().timestamp();
        self.record_timeline(TimelineKind::ParseError, n, now);
    }

    pub fn observe_written_rows(&self, n: u64) {
        self.written_rows.fetch_add(n, Ordering::Relaxed);
        let now = Utc::now().timestamp();
        self.last_write_epoch.store(now, Ordering::Relaxed);
        self.record_timeline(TimelineKind::Written, n, now);
    }

    pub fn observe_written_files(&self, n: u64) {
        self.written_files.fetch_add(n, Ordering::Relaxed);
    }

    pub fn observe_dlq_rows(&self, n: u64) {
        self.dlq_rows.fetch_add(n, Ordering::Relaxed);
        let now = Utc::now().timestamp();
        self.record_timeline(TimelineKind::Dlq, n, now);
    }

    pub fn observe_durability_replayed(&self, n: u64) {
        self.durability_replayed.fetch_add(n, Ordering::Relaxed);
    }

    pub fn observe_durability_committed(&self, n: u64) {
        self.durability_commits.fetch_add(n, Ordering::Relaxed);
    }

    pub fn observe_durability_error(&self, n: u64) {
        self.durability_errors.fetch_add(n, Ordering::Relaxed);
    }

    pub fn observe_connection_dropped(&self, n: u64) {
        self.connection_dropped.fetch_add(n, Ordering::Relaxed);
    }

    pub fn observe_connection_timeout(&self, n: u64) {
        self.connection_timeouts.fetch_add(n, Ordering::Relaxed);
    }

    pub fn render_prometheus(&self) -> String {
        let mut out = String::new();
        push_counter(
            &mut out,
            "nss_ingestor_raw_received_total",
            "Raw records received from TCP listener",
            self.raw_received.load(Ordering::Relaxed),
        );
        push_counter(
            &mut out,
            "nss_ingestor_parsed_ok_total",
            "Records successfully parsed",
            self.parsed_ok.load(Ordering::Relaxed),
        );
        push_counter(
            &mut out,
            "nss_ingestor_parsed_error_total",
            "Records that failed parsing",
            self.parsed_error.load(Ordering::Relaxed),
        );
        push_counter(
            &mut out,
            "nss_ingestor_written_rows_total",
            "Rows written to Parquet",
            self.written_rows.load(Ordering::Relaxed),
        );
        push_counter(
            &mut out,
            "nss_ingestor_written_files_total",
            "Parquet files created",
            self.written_files.load(Ordering::Relaxed),
        );
        push_counter(
            &mut out,
            "nss_ingestor_dlq_rows_total",
            "Rows sent to dead-letter queue",
            self.dlq_rows.load(Ordering::Relaxed),
        );
        push_counter(
            &mut out,
            "nss_ingestor_durability_replayed_total",
            "Rows replayed from durability spool at startup",
            self.durability_replayed.load(Ordering::Relaxed),
        );
        push_counter(
            &mut out,
            "nss_ingestor_durability_commits_total",
            "Durability checkpoint commit operations",
            self.durability_commits.load(Ordering::Relaxed),
        );
        push_counter(
            &mut out,
            "nss_ingestor_durability_errors_total",
            "Durability operation errors",
            self.durability_errors.load(Ordering::Relaxed),
        );
        push_counter(
            &mut out,
            "nss_ingestor_connection_dropped_total",
            "Connections dropped due to listener limits",
            self.connection_dropped.load(Ordering::Relaxed),
        );
        push_counter(
            &mut out,
            "nss_ingestor_connection_timeouts_total",
            "Connections closed due to read timeout",
            self.connection_timeouts.load(Ordering::Relaxed),
        );
        out
    }

    pub fn snapshot(&self, settings: &StatsSettings) -> StatsResponse {
        let now = Utc::now().timestamp();
        let total_ingested = self.raw_received.load(Ordering::Relaxed);
        let total_parsed_ok = self.parsed_ok.load(Ordering::Relaxed);
        let total_parsed_error = self.parsed_error.load(Ordering::Relaxed);
        let total_written_rows = self.written_rows.load(Ordering::Relaxed);
        let total_written_files = self.written_files.load(Ordering::Relaxed);
        let total_dlq_rows = self.dlq_rows.load(Ordering::Relaxed);
        let parse_total = total_parsed_ok + total_parsed_error;
        let parse_success_ratio = if parse_total == 0 {
            1.0
        } else {
            total_parsed_ok as f64 / parse_total as f64
        };
        let parse_error_ratio = if parse_total == 0 {
            0.0
        } else {
            total_parsed_error as f64 / parse_total as f64
        };

        let ingest_last_seen_seconds_ago =
            epoch_age(now, self.last_ingest_epoch.load(Ordering::Relaxed));
        let write_last_seen_seconds_ago =
            epoch_age(now, self.last_write_epoch.load(Ordering::Relaxed));

        let (rates, trends) = {
            let mut guard = self
                .timeline
                .lock()
                .expect("metrics timeline lock poisoned");
            guard.trim_to_window(now);
            (guard.rates(now), guard.series(now))
        };

        let (status, reasons) = classify_health(
            parse_error_ratio,
            ingest_last_seen_seconds_ago,
            write_last_seen_seconds_ago,
            total_ingested,
            settings,
        );

        StatsResponse {
            status,
            reasons,
            generated_at: Utc::now().to_rfc3339(),
            uptime_seconds: now.saturating_sub(self.started_at_epoch) as u64,
            totals: Totals {
                ingested: total_ingested,
                parsed_ok: total_parsed_ok,
                parsed_error: total_parsed_error,
                written_rows: total_written_rows,
                written_files: total_written_files,
                dlq_rows: total_dlq_rows,
                parse_success_ratio,
                parse_error_ratio,
            },
            rates_per_sec: rates,
            freshness: Freshness {
                ingest_last_seen_seconds_ago,
                write_last_seen_seconds_ago,
            },
            trends,
        }
    }

    fn record_timeline(&self, kind: TimelineKind, n: u64, now: i64) {
        if n == 0 {
            return;
        }
        let mut guard = self
            .timeline
            .lock()
            .expect("metrics timeline lock poisoned");
        guard.record(kind, n, now);
    }
}

fn push_counter(buf: &mut String, name: &str, help: &str, value: u64) {
    buf.push_str("# HELP ");
    buf.push_str(name);
    buf.push(' ');
    buf.push_str(help);
    buf.push('\n');
    buf.push_str("# TYPE ");
    buf.push_str(name);
    buf.push_str(" counter\n");
    buf.push_str(name);
    buf.push(' ');
    buf.push_str(&value.to_string());
    buf.push('\n');
}

fn epoch_age(now: i64, epoch: i64) -> Option<u64> {
    if epoch <= 0 || epoch > now {
        None
    } else {
        Some((now - epoch) as u64)
    }
}

fn classify_health(
    parse_error_ratio: f64,
    ingest_stale_seconds: Option<u64>,
    write_stale_seconds: Option<u64>,
    total_ingested: u64,
    settings: &StatsSettings,
) -> (String, Vec<String>) {
    let mut reasons = Vec::new();
    let mut status = "ok";

    if parse_error_ratio >= settings.critical_error_ratio {
        status = "critical";
        reasons.push(format!(
            "parse error ratio {:.2}% >= critical {:.2}%",
            parse_error_ratio * 100.0,
            settings.critical_error_ratio * 100.0
        ));
    } else if parse_error_ratio >= settings.degraded_error_ratio {
        status = "degraded";
        reasons.push(format!(
            "parse error ratio {:.2}% >= degraded {:.2}%",
            parse_error_ratio * 100.0,
            settings.degraded_error_ratio * 100.0
        ));
    }

    if total_ingested > 0
        && let Some(stale) = ingest_stale_seconds
    {
        if stale >= settings.critical_stale_seconds {
            status = "critical";
            reasons.push(format!(
                "ingest stale for {}s (critical threshold {}s)",
                stale, settings.critical_stale_seconds
            ));
        } else if stale >= settings.degraded_stale_seconds && status == "ok" {
            status = "degraded";
            reasons.push(format!(
                "ingest stale for {}s (degraded threshold {}s)",
                stale, settings.degraded_stale_seconds
            ));
        }
    }

    if let Some(stale) = write_stale_seconds {
        if stale >= settings.critical_stale_seconds {
            status = "critical";
            reasons.push(format!(
                "writer stale for {}s (critical threshold {}s)",
                stale, settings.critical_stale_seconds
            ));
        } else if stale >= settings.degraded_stale_seconds && status == "ok" {
            status = "degraded";
            reasons.push(format!(
                "writer stale for {}s (degraded threshold {}s)",
                stale, settings.degraded_stale_seconds
            ));
        }
    }

    (status.to_string(), reasons)
}

#[derive(Debug, Clone, Copy)]
enum TimelineKind {
    Ingested,
    Written,
    ParseError,
    Dlq,
}

#[derive(Debug, Default)]
struct TimeSeriesWindow {
    window_minutes: u32,
    buckets: VecDeque<MinuteBucket>,
}

impl TimeSeriesWindow {
    fn new(window_hours: u32) -> Self {
        Self {
            window_minutes: window_hours.max(1) * 60,
            buckets: VecDeque::new(),
        }
    }

    fn record(&mut self, kind: TimelineKind, n: u64, now: i64) {
        let minute_epoch = floor_to_minute(now);
        self.trim_to_window(now);
        match self.buckets.back_mut() {
            Some(last) if last.minute_epoch == minute_epoch => last.inc(kind, n),
            _ => {
                let mut bucket = MinuteBucket {
                    minute_epoch,
                    ..MinuteBucket::default()
                };
                bucket.inc(kind, n);
                self.buckets.push_back(bucket);
            }
        }
    }

    fn trim_to_window(&mut self, now: i64) {
        let min_minute = floor_to_minute(now) - ((self.window_minutes as i64 - 1) * 60);
        while matches!(self.buckets.front(), Some(front) if front.minute_epoch < min_minute) {
            let _ = self.buckets.pop_front();
        }
    }

    fn rates(&self, now: i64) -> RatesPerSec {
        let one = self.sum_since(now, 1);
        let five = self.sum_since(now, 5);
        RatesPerSec {
            ingest_1m: one.ingested as f64 / 60.0,
            ingest_5m: five.ingested as f64 / 300.0,
            write_1m: one.written as f64 / 60.0,
            write_5m: five.written as f64 / 300.0,
            parse_error_1m: one.parse_errors as f64 / 60.0,
            parse_error_5m: five.parse_errors as f64 / 300.0,
            dlq_1m: one.dlq as f64 / 60.0,
            dlq_5m: five.dlq as f64 / 300.0,
        }
    }

    fn series(&self, now: i64) -> Vec<TrendPoint> {
        let start_minute = floor_to_minute(now) - ((self.window_minutes as i64 - 1) * 60);
        let mut out = Vec::with_capacity(self.window_minutes as usize);
        let mut idx = 0usize;
        let mut minute = start_minute;
        while minute <= floor_to_minute(now) {
            while idx < self.buckets.len() && self.buckets[idx].minute_epoch < minute {
                idx += 1;
            }
            if idx < self.buckets.len() && self.buckets[idx].minute_epoch == minute {
                let b = self.buckets[idx];
                out.push(TrendPoint {
                    minute_epoch: b.minute_epoch,
                    ingested: b.ingested,
                    written: b.written,
                    parse_errors: b.parse_errors,
                    dlq: b.dlq,
                });
            } else {
                out.push(TrendPoint {
                    minute_epoch: minute,
                    ingested: 0,
                    written: 0,
                    parse_errors: 0,
                    dlq: 0,
                });
            }
            minute += 60;
        }
        out
    }

    fn sum_since(&self, now: i64, minutes: i64) -> BucketSums {
        let cutoff = floor_to_minute(now) - ((minutes - 1) * 60);
        let mut sums = BucketSums::default();
        for b in &self.buckets {
            if b.minute_epoch >= cutoff {
                sums.ingested += b.ingested;
                sums.written += b.written;
                sums.parse_errors += b.parse_errors;
                sums.dlq += b.dlq;
            }
        }
        sums
    }
}

fn floor_to_minute(ts: i64) -> i64 {
    ts - (ts % 60)
}

#[derive(Debug, Default, Clone, Copy)]
struct BucketSums {
    ingested: u64,
    written: u64,
    parse_errors: u64,
    dlq: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct MinuteBucket {
    minute_epoch: i64,
    ingested: u64,
    written: u64,
    parse_errors: u64,
    dlq: u64,
}

impl MinuteBucket {
    fn inc(&mut self, kind: TimelineKind, n: u64) {
        match kind {
            TimelineKind::Ingested => self.ingested += n,
            TimelineKind::Written => self.written += n,
            TimelineKind::ParseError => self.parse_errors += n,
            TimelineKind::Dlq => self.dlq += n,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct StatsResponse {
    pub status: String,
    pub reasons: Vec<String>,
    pub generated_at: String,
    pub uptime_seconds: u64,
    pub totals: Totals,
    pub rates_per_sec: RatesPerSec,
    pub freshness: Freshness,
    pub trends: Vec<TrendPoint>,
}

#[derive(Debug, Serialize)]
pub struct Totals {
    pub ingested: u64,
    pub parsed_ok: u64,
    pub parsed_error: u64,
    pub written_rows: u64,
    pub written_files: u64,
    pub dlq_rows: u64,
    pub parse_success_ratio: f64,
    pub parse_error_ratio: f64,
}

#[derive(Debug, Serialize)]
pub struct RatesPerSec {
    pub ingest_1m: f64,
    pub ingest_5m: f64,
    pub write_1m: f64,
    pub write_5m: f64,
    pub parse_error_1m: f64,
    pub parse_error_5m: f64,
    pub dlq_1m: f64,
    pub dlq_5m: f64,
}

#[derive(Debug, Serialize)]
pub struct Freshness {
    pub ingest_last_seen_seconds_ago: Option<u64>,
    pub write_last_seen_seconds_ago: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct TrendPoint {
    pub minute_epoch: i64,
    pub ingested: u64,
    pub written: u64,
    pub parse_errors: u64,
    pub dlq: u64,
}

#[derive(Clone)]
struct MetricsState {
    metrics: Arc<Metrics>,
    settings: StatsSettings,
    schema: Arc<SchemaOverview>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SchemaOverview {
    pub schema_path: String,
    pub time_field: String,
    pub time_format: String,
    pub timezone: String,
    pub strict_type_validation: bool,
    pub field_count: usize,
    pub fields: Vec<SchemaFieldOverview>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SchemaFieldOverview {
    pub index: usize,
    pub name: String,
    #[serde(rename = "type")]
    pub logical_type: String,
    pub nullable: bool,
}

pub async fn run_metrics_server(
    bind_addr: String,
    metrics: Arc<Metrics>,
    settings: StatsSettings,
    schema: SchemaOverview,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let state = MetricsState {
        metrics,
        settings,
        schema: Arc::new(schema),
    };

    let mut app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/api/stats", get(stats_handler))
        .route("/api/schema", get(schema_handler))
        .with_state(state.clone());

    if state.settings.dashboard_enabled {
        app = app.route("/dashboard", get(dashboard_handler));
    }

    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            while shutdown.changed().await.is_ok() {
                if *shutdown.borrow() {
                    break;
                }
            }
        })
        .await?;
    Ok(())
}

async fn metrics_handler(State(state): State<MetricsState>) -> String {
    state.metrics.render_prometheus()
}

async fn stats_handler(State(state): State<MetricsState>) -> Json<StatsResponse> {
    Json(state.metrics.snapshot(&state.settings))
}

async fn schema_handler(State(state): State<MetricsState>) -> Json<SchemaOverview> {
    Json((*state.schema).clone())
}

async fn dashboard_handler() -> Html<&'static str> {
    Html(DASHBOARD_HTML)
}

const DASHBOARD_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>NSS Ingestor Dashboard</title>
  <style>
    :root { --bg:#f7fafc; --ink:#12263a; --card:#ffffff; --line:#d7e1ea; --ok:#0f9d58; --warn:#d97706; --crit:#c62828; }
    * { box-sizing: border-box; }
    body { margin:0; font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, sans-serif; background:var(--bg); color:var(--ink); }
    header { padding:16px 20px; border-bottom:1px solid var(--line); background:#fff; position:sticky; top:0; }
    .wrap { max-width:1200px; margin:0 auto; padding:18px; display:grid; gap:16px; }
    .grid { display:grid; gap:12px; grid-template-columns: repeat(auto-fit, minmax(220px,1fr)); }
    .card { background:var(--card); border:1px solid var(--line); border-radius:10px; padding:12px; }
    .label { font-size:12px; color:#4c6176; text-transform: uppercase; letter-spacing: .04em; }
    .value { font-size:24px; font-weight:700; margin-top:4px; }
    .muted { color:#5f7386; font-size:12px; }
    .status { padding:4px 10px; border-radius:999px; color:#fff; font-weight:700; font-size:12px; }
    .status.ok { background:var(--ok); } .status.degraded { background:var(--warn); } .status.critical { background:var(--crit); }
    canvas { width:100%; height:120px; }
    ul { margin:0; padding-left:16px; }
    table { width:100%; border-collapse: collapse; margin-top:8px; font-size:12px; }
    th, td { border-bottom: 1px solid var(--line); text-align:left; padding:6px 8px; vertical-align: top; }
    th { color:#4c6176; text-transform: uppercase; font-size:11px; letter-spacing: .03em; }
    td.type { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
  </style>
</head>
<body>
  <header>
    <strong>NSS Ingestor Dashboard</strong>
    <span id="status" class="status ok" style="margin-left:10px;">ok</span>
    <span class="muted" id="ts" style="margin-left:10px;"></span>
  </header>
  <main class="wrap">
    <section class="grid">
      <div class="card"><div class="label">Ingested</div><div id="ingested" class="value">0</div></div>
      <div class="card"><div class="label">Written Rows</div><div id="written" class="value">0</div></div>
      <div class="card"><div class="label">Parse Error Ratio</div><div id="perr" class="value">0.00%</div></div>
      <div class="card"><div class="label">DLQ Rows</div><div id="dlq" class="value">0</div></div>
      <div class="card"><div class="label">Ingest Rate (1m)</div><div id="ingr" class="value">0/s</div></div>
      <div class="card"><div class="label">Write Rate (1m)</div><div id="wrr" class="value">0/s</div></div>
    </section>
    <section class="grid">
      <div class="card"><div class="label">Ingest Trend (24h)</div><canvas id="c1" width="480" height="120"></canvas></div>
      <div class="card"><div class="label">Write Trend (24h)</div><canvas id="c2" width="480" height="120"></canvas></div>
      <div class="card"><div class="label">Parse Errors (24h)</div><canvas id="c3" width="480" height="120"></canvas></div>
      <div class="card"><div class="label">DLQ (24h)</div><canvas id="c4" width="480" height="120"></canvas></div>
    </section>
    <section class="card">
      <div class="label">Health Details</div>
      <ul id="reasons"></ul>
      <div class="muted" id="freshness"></div>
    </section>
    <section class="card">
      <div class="label">Schema Overview</div>
      <div class="muted" id="schemaMeta">loading...</div>
      <table>
        <thead>
          <tr><th>#</th><th>Field</th><th>Type</th><th>Nullable</th></tr>
        </thead>
        <tbody id="schemaRows"></tbody>
      </table>
    </section>
  </main>
  <script>
    const byId = (id) => document.getElementById(id);
    const fmtInt = (n) => Number(n || 0).toLocaleString();
    const fmtRate = (n) => `${Number(n || 0).toFixed(2)}/s`;
    const fmtPct = (n) => `${(Number(n || 0) * 100).toFixed(2)}%`;

    function drawSeries(canvasId, points, key, color) {
      const c = byId(canvasId);
      const ctx = c.getContext('2d');
      const w = c.width, h = c.height;
      ctx.clearRect(0, 0, w, h);
      ctx.fillStyle = '#fff'; ctx.fillRect(0, 0, w, h);
      if (!points || points.length === 0) return;
      const vals = points.map(p => Number(p[key] || 0));
      const max = Math.max(...vals, 1);
      ctx.strokeStyle = '#e6edf3'; ctx.lineWidth = 1;
      for (let i=1;i<4;i++){ const y=(h/4)*i; ctx.beginPath(); ctx.moveTo(0,y); ctx.lineTo(w,y); ctx.stroke(); }
      ctx.strokeStyle = color; ctx.lineWidth = 2; ctx.beginPath();
      vals.forEach((v, i) => {
        const x = (i / (vals.length - 1 || 1)) * w;
        const y = h - (v / max) * (h - 6) - 3;
        if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
      });
      ctx.stroke();
    }

    async function loadSchema() {
      const res = await fetch('/api/schema', { cache: 'no-store' });
      const s = await res.json();
      byId('schemaMeta').textContent =
        `${s.field_count} fields | path: ${s.schema_path} | time_field: ${s.time_field} | timezone: ${s.timezone} | strict_type_validation: ${s.strict_type_validation}`;

      const rows = byId('schemaRows');
      rows.innerHTML = '';
      (s.fields || []).forEach((f) => {
        const tr = document.createElement('tr');
        tr.innerHTML = `<td>${f.index}</td><td>${f.name}</td><td class="type">${f.type}</td><td>${f.nullable ? 'true' : 'false'}</td>`;
        rows.appendChild(tr);
      });
    }

    async function refresh() {
      const res = await fetch('/api/stats', { cache: 'no-store' });
      const s = await res.json();
      byId('ingested').textContent = fmtInt(s.totals.ingested);
      byId('written').textContent = fmtInt(s.totals.written_rows);
      byId('perr').textContent = fmtPct(s.totals.parse_error_ratio);
      byId('dlq').textContent = fmtInt(s.totals.dlq_rows);
      byId('ingr').textContent = fmtRate(s.rates_per_sec.ingest_1m);
      byId('wrr').textContent = fmtRate(s.rates_per_sec.write_1m);
      byId('ts').textContent = `updated ${new Date(s.generated_at).toLocaleString()}`;

      const badge = byId('status');
      badge.textContent = s.status;
      badge.className = `status ${s.status}`;

      const reasons = byId('reasons');
      reasons.innerHTML = '';
      if ((s.reasons || []).length === 0) {
        const li = document.createElement('li'); li.textContent = 'No active health alerts.'; reasons.appendChild(li);
      } else {
        s.reasons.forEach(r => { const li = document.createElement('li'); li.textContent = r; reasons.appendChild(li); });
      }
      byId('freshness').textContent = `ingest last seen: ${s.freshness.ingest_last_seen_seconds_ago ?? 'n/a'}s, write last seen: ${s.freshness.write_last_seen_seconds_ago ?? 'n/a'}s`;

      drawSeries('c1', s.trends, 'ingested', '#1d4ed8');
      drawSeries('c2', s.trends, 'written', '#0f9d58');
      drawSeries('c3', s.trends, 'parse_errors', '#d97706');
      drawSeries('c4', s.trends, 'dlq', '#c62828');
    }

    loadSchema().catch(console.error);
    refresh().catch(console.error);
    setInterval(() => refresh().catch(console.error), 5000);
  </script>
</body>
</html>
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeline_records_and_rates() {
        let mut window = TimeSeriesWindow::new(1);
        let now = 1_700_000_000_i64;

        window.record(TimelineKind::Ingested, 120, now);
        window.record(TimelineKind::Written, 60, now);
        window.record(TimelineKind::ParseError, 6, now);

        let rates = window.rates(now);
        assert!((rates.ingest_1m - 2.0).abs() < f64::EPSILON);
        assert!((rates.write_1m - 1.0).abs() < f64::EPSILON);
        assert!((rates.parse_error_1m - 0.1).abs() < f64::EPSILON);
    }

    #[test]
    fn classify_health_critical_by_error_ratio() {
        let settings = StatsSettings::default();
        let (status, reasons) = classify_health(0.30, None, None, 100, &settings);
        assert_eq!(status, "critical");
        assert!(!reasons.is_empty());
    }

    #[test]
    fn snapshot_contains_trends_window() {
        let metrics = Metrics::new(1);
        metrics.observe_raw_received(10);
        metrics.observe_parsed_ok(9);
        metrics.observe_parsed_error(1);
        metrics.observe_written_rows(9);

        let settings = StatsSettings::default();
        let snapshot = metrics.snapshot(&settings);
        assert!(!snapshot.trends.is_empty());
        assert_eq!(snapshot.totals.ingested, 10);
        assert_eq!(snapshot.totals.written_rows, 9);
    }
}
