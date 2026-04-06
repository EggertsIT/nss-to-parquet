use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use chrono::{DateTime, Datelike, Duration, NaiveDateTime, Utc};
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, sleep};
use tracing::info;

use crate::config::AppConfig;
use crate::metrics::Metrics;
use crate::schema::{LogicalType, SchemaDef};
use crate::types::ParsedRecord;
use crate::writer::{WriterControlMessage, run_parquet_writer};

pub const DEFAULT_USER_COUNT: u32 = 42_000;
pub const DEFAULT_MIN_DEVICES_PER_USER: u8 = 2;
pub const DEFAULT_MAX_DEVICES_PER_USER: u8 = 4;

const CORPORATE_DOMAIN: &str = "corp.example";
const HOUR_WEIGHTS: [u16; 24] = [
    1, 1, 1, 1, 1, 2, 4, 8, 12, 15, 16, 17, 17, 17, 16, 16, 15, 14, 11, 8, 6, 4, 2, 1,
];

const TLS13_CIPHERS: [&str; 2] = [
    "TLS1_3_CK_AES_256_GCM_SHA384",
    "TLS1_3_CK_AES_128_GCM_SHA256",
];
const TLS12_CIPHERS: [&str; 2] = [
    "ECDHE_RSA_AES_256_GCM_SHA384",
    "ECDHE_RSA_AES_128_GCM_SHA256",
];

const DEPARTMENTS: [&str; 10] = [
    "Engineering",
    "Operations",
    "Security",
    "Finance",
    "Sales",
    "Marketing",
    "HR",
    "Support",
    "Legal",
    "Procurement",
];

const OFFICE_LOCATIONS: [LocationProfile; 10] = [
    LocationProfile::new(
        "DE-BER",
        "Berlin HQ",
        "None",
        "Germany",
        "BER1",
        "DE",
        "ZscalerClientConnector",
        30,
        (141, 78),
    ),
    LocationProfile::new(
        "DE-MUC",
        "Munich HQ",
        "None",
        "Germany",
        "MUC1",
        "DE",
        "ZscalerClientConnector",
        31,
        (141, 79),
    ),
    LocationProfile::new(
        "DE-FRA",
        "Frankfurt Hub",
        "None",
        "Germany",
        "FRA1",
        "DE",
        "GRE Tunnel",
        32,
        (141, 80),
    ),
    LocationProfile::new(
        "UK-LON",
        "London Hub",
        "None",
        "United Kingdom",
        "LON1",
        "GB",
        "GRE Tunnel",
        40,
        (51, 140),
    ),
    LocationProfile::new(
        "NL-AMS",
        "Amsterdam Hub",
        "None",
        "The Netherlands",
        "AMS1",
        "NL",
        "GRE Tunnel",
        41,
        (20, 10),
    ),
    LocationProfile::new(
        "US-NYC",
        "New York HQ",
        "None",
        "United States",
        "NYC1",
        "US",
        "ZscalerClientConnector",
        50,
        (20, 42),
    ),
    LocationProfile::new(
        "US-DFW",
        "Dallas Hub",
        "None",
        "United States",
        "DFW1",
        "US",
        "GRE Tunnel",
        51,
        (40, 76),
    ),
    LocationProfile::new(
        "SG-SIN",
        "Singapore Hub",
        "None",
        "Singapore",
        "SIN1",
        "SG",
        "IPSEC",
        60,
        (52, 220),
    ),
    LocationProfile::new(
        "AU-SYD",
        "Sydney Hub",
        "None",
        "Australia",
        "SYD1",
        "AU",
        "IPSEC",
        61,
        (52, 63),
    ),
    LocationProfile::new(
        "RW-DE",
        "Road Warrior",
        "Germany Remote",
        "Germany",
        "MUC1",
        "DE",
        "ZscalerClientConnector",
        70,
        (93, 184),
    ),
];

const PRIMARY_APPS: [AppProfile; 14] = [
    AppProfile::new(
        "outlook.office.com",
        "M365",
        "Office Apps",
        "Sanctioned",
        "Business Use",
        "Business and Economy",
        "Corporate Email",
        "application/json",
        "Ireland",
        PathFamily::ApiJson,
    ),
    AppProfile::new(
        "graph.microsoft.com",
        "M365",
        "Office Apps",
        "Sanctioned",
        "Business Use",
        "Business and Economy",
        "Corporate Email",
        "application/json",
        "Ireland",
        PathFamily::GraphApi,
    ),
    AppProfile::new(
        "api.github.com",
        "GitHub",
        "Development",
        "Sanctioned",
        "Business Use",
        "Information Technology",
        "Other Information Technology",
        "application/json",
        "United States",
        PathFamily::ApiJson,
    ),
    AppProfile::new(
        "github.com",
        "GitHub",
        "Development",
        "Sanctioned",
        "Business Use",
        "Information Technology",
        "Other Information Technology",
        "text/html",
        "United States",
        PathFamily::WebPage,
    ),
    AppProfile::new(
        "instance.service-now.com",
        "ServiceNow",
        "Business",
        "Sanctioned",
        "Business Use",
        "Business and Economy",
        "Professional Services",
        "application/json",
        "United States",
        PathFamily::ApiJson,
    ),
    AppProfile::new(
        "my.salesforce.com",
        "Salesforce",
        "Business",
        "Sanctioned",
        "Business Use",
        "Business and Economy",
        "Corporate Marketing",
        "application/json",
        "United States",
        PathFamily::ApiJson,
    ),
    AppProfile::new(
        "workday.com",
        "Workday",
        "Business",
        "Sanctioned",
        "Business Use",
        "Business and Economy",
        "Professional Services",
        "application/json",
        "United States",
        PathFamily::AuthFlow,
    ),
    AppProfile::new(
        "slack.com",
        "Slack",
        "Communication",
        "Sanctioned",
        "Business Use",
        "Internet Communication",
        "Instant Messaging",
        "application/json",
        "United States",
        PathFamily::ApiJson,
    ),
    AppProfile::new(
        "zoom.us",
        "Zoom",
        "Communication",
        "Sanctioned",
        "Business Use",
        "Internet Communication",
        "Internet Services",
        "application/json",
        "United States",
        PathFamily::ApiJson,
    ),
    AppProfile::new(
        "edition.cnn.com",
        "General Browsing",
        "News",
        "N/A",
        "General Surfing",
        "News and Media",
        "News and Media",
        "text/html",
        "United States",
        PathFamily::WebPage,
    ),
    AppProfile::new(
        "www.google.com",
        "Google Search",
        "Search",
        "N/A",
        "General Surfing",
        "Information Technology",
        "Web Search",
        "text/html",
        "United States",
        PathFamily::Search,
    ),
    AppProfile::new(
        "ping.chartbeat.net",
        "General Browsing",
        "Analytics",
        "N/A",
        "General Surfing",
        "Information Technology",
        "Internet Services",
        "application/json",
        "United States",
        PathFamily::Telemetry,
    ),
    AppProfile::new(
        "bam.nr-data.net",
        "New Relic",
        "Sales and Marketing",
        "Unsanctioned",
        "Business Use",
        "Business and Economy",
        "Corporate Marketing",
        "application/json",
        "United States",
        PathFamily::Telemetry,
    ),
    AppProfile::new(
        "cdn.jsdelivr.net",
        "General Browsing",
        "Development",
        "N/A",
        "General Surfing",
        "Information Technology",
        "Internet Services",
        "application/javascript",
        "United States",
        PathFamily::StaticAsset,
    ),
];

const SOCIAL_APPS: [AppProfile; 4] = [
    AppProfile::new(
        "ok.ru",
        "Odnoklassniki",
        "Consumer Apps",
        "Unsanctioned",
        "Productivity Loss",
        "Social Networking",
        "Hobbies/Leisure",
        "application/json",
        "Russia",
        PathFamily::ApiJson,
    ),
    AppProfile::new(
        "www.tiktok.com",
        "TikTok",
        "Consumer Apps",
        "Unsanctioned",
        "Productivity Loss",
        "Social Networking",
        "Social Networking",
        "text/html",
        "United States",
        PathFamily::WebPage,
    ),
    AppProfile::new(
        "www.instagram.com",
        "Instagram",
        "Consumer Apps",
        "Unsanctioned",
        "Productivity Loss",
        "Social Networking",
        "Social Networking",
        "application/json",
        "United States",
        PathFamily::ApiJson,
    ),
    AppProfile::new(
        "www.reddit.com",
        "Reddit",
        "Consumer Apps",
        "Unsanctioned",
        "Productivity Loss",
        "Social Networking",
        "Social Networking",
        "text/html",
        "United States",
        PathFamily::WebPage,
    ),
];

const GEO_APPS: [AppProfile; 4] = [
    AppProfile::new(
        "files.badcdn.ru",
        "General Browsing",
        "Consumer Apps",
        "Unsanctioned",
        "Privacy Risk",
        "Information Technology",
        "Internet Services",
        "application/octet-stream",
        "Russia",
        PathFamily::Download,
    ),
    AppProfile::new(
        "sync.cn-market.example",
        "General Browsing",
        "Consumer Apps",
        "Unsanctioned",
        "Privacy Risk",
        "Business and Economy",
        "Online Shopping",
        "application/json",
        "China",
        PathFamily::ApiJson,
    ),
    AppProfile::new(
        "banking-ir.example",
        "General Browsing",
        "Consumer Apps",
        "Unsanctioned",
        "Privacy Risk",
        "Business and Economy",
        "Financial Services",
        "text/html",
        "Iran",
        PathFamily::WebPage,
    ),
    AppProfile::new(
        "telemetry.by-example.net",
        "General Browsing",
        "Consumer Apps",
        "Unsanctioned",
        "Privacy Risk",
        "Information Technology",
        "Internet Services",
        "application/json",
        "Belarus",
        PathFamily::Telemetry,
    ),
];

const THREAT_APPS: [AppProfile; 4] = [
    AppProfile::new(
        "login-secure-m365.example",
        "General Browsing",
        "Consumer Apps",
        "Unsanctioned",
        "Privacy Risk",
        "Security",
        "Phishing and Other Frauds",
        "text/html",
        "United States",
        PathFamily::AuthFlow,
    ),
    AppProfile::new(
        "payloads-drop.example",
        "General Browsing",
        "Consumer Apps",
        "Unsanctioned",
        "Privacy Risk",
        "Security",
        "Spyware Callback",
        "application/octet-stream",
        "The Netherlands",
        PathFamily::Download,
    ),
    AppProfile::new(
        "malvertising.example",
        "General Browsing",
        "Sales and Marketing",
        "Unsanctioned",
        "Bandwidth Loss",
        "Security",
        "Advertising",
        "application/javascript",
        "United States",
        PathFamily::StaticAsset,
    ),
    AppProfile::new(
        "cdn-phish.example",
        "General Browsing",
        "Consumer Apps",
        "Unsanctioned",
        "Privacy Risk",
        "Security",
        "Phishing and Other Frauds",
        "application/json",
        "United States",
        PathFamily::ApiJson,
    ),
];

const DOWNLOAD_APPS: [AppProfile; 4] = [
    AppProfile::new(
        "downloads.box.com",
        "Box",
        "Business",
        "Sanctioned",
        "Business Use",
        "Business and Economy",
        "Internet Services",
        "application/zip",
        "United States",
        PathFamily::Download,
    ),
    AppProfile::new(
        "files.sharepoint.com",
        "M365",
        "Office Apps",
        "Sanctioned",
        "Business Use",
        "Business and Economy",
        "Corporate Email",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "Ireland",
        PathFamily::Download,
    ),
    AppProfile::new(
        "dl.dropboxusercontent.com",
        "Dropbox",
        "File Sharing",
        "Unsanctioned",
        "Business Use",
        "Business and Economy",
        "Internet Services",
        "application/octet-stream",
        "United States",
        PathFamily::Download,
    ),
    AppProfile::new(
        "objects.githubusercontent.com",
        "GitHub",
        "Development",
        "Sanctioned",
        "Business Use",
        "Information Technology",
        "Other Information Technology",
        "application/octet-stream",
        "United States",
        PathFamily::Download,
    ),
];

#[derive(Debug, Clone, Copy)]
pub struct FleetConfig {
    pub user_count: u32,
    pub min_devices_per_user: u8,
    pub max_devices_per_user: u8,
}

impl FleetConfig {
    fn validated(self) -> Result<Self> {
        if self.user_count == 0 {
            anyhow::bail!("backfill user_count must be > 0");
        }
        if self.min_devices_per_user == 0 {
            anyhow::bail!("backfill min_devices_per_user must be > 0");
        }
        if self.min_devices_per_user > self.max_devices_per_user {
            anyhow::bail!("backfill min_devices_per_user must be <= max_devices_per_user");
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy)]
struct LocationProfile {
    code: &'static str,
    location: &'static str,
    userlocation: &'static str,
    country: &'static str,
    datacenter: &'static str,
    datacenter_country: &'static str,
    trafficredirectmethod: &'static str,
    src_subnet: u8,
    public_prefix: (u8, u8),
}

impl LocationProfile {
    #[allow(clippy::too_many_arguments)]
    const fn new(
        code: &'static str,
        location: &'static str,
        userlocation: &'static str,
        country: &'static str,
        datacenter: &'static str,
        datacenter_country: &'static str,
        trafficredirectmethod: &'static str,
        src_subnet: u8,
        public_prefix: (u8, u8),
    ) -> Self {
        Self {
            code,
            location,
            userlocation,
            country,
            datacenter,
            datacenter_country,
            trafficredirectmethod,
            src_subnet,
            public_prefix,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct AppProfile {
    host: &'static str,
    appname: &'static str,
    appclass: &'static str,
    app_status: &'static str,
    urlclass: &'static str,
    urlsupercat: &'static str,
    urlcat: &'static str,
    contenttype: &'static str,
    country: &'static str,
    path_family: PathFamily,
}

impl AppProfile {
    #[allow(clippy::too_many_arguments)]
    const fn new(
        host: &'static str,
        appname: &'static str,
        appclass: &'static str,
        app_status: &'static str,
        urlclass: &'static str,
        urlsupercat: &'static str,
        urlcat: &'static str,
        contenttype: &'static str,
        country: &'static str,
        path_family: PathFamily,
    ) -> Self {
        Self {
            host,
            appname,
            appclass,
            app_status,
            urlclass,
            urlsupercat,
            urlcat,
            contenttype,
            country,
            path_family,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum PathFamily {
    ApiJson,
    AuthFlow,
    Download,
    GraphApi,
    Search,
    StaticAsset,
    Telemetry,
    WebPage,
}

#[derive(Debug, Clone, Copy)]
enum DeviceKind {
    WindowsLaptop,
    MacBook,
    IPhone,
    AndroidPhone,
    WindowsVdi,
}

#[derive(Debug, Clone, Copy)]
enum EventKind {
    AllowedClean,
    AllowedBackground,
    BlockedCategory,
    BlockedSslBadCert,
    BlockedSslLowTls,
    BlockedGeo,
    ThreatBlock,
    ConnectionProbeFailure,
    FileControlBlock,
    DlpBlock,
    ClientHandshakeFailure,
}

#[derive(Debug, Clone)]
struct SyntheticContext {
    event_time: DateTime<Utc>,
    user_id: u32,
    device_slot: u8,
    dept: &'static str,
    location: &'static LocationProfile,
    app: &'static AppProfile,
    device_kind: DeviceKind,
    event_kind: EventKind,
    src_ip: [u8; 4],
    nat_ip: [u8; 4],
    public_ip: [u8; 4],
    dest_ip: [u8; 4],
    reqmethod: &'static str,
    respcode: &'static str,
    action: &'static str,
    reason: &'static str,
    ruletype: &'static str,
    rulelabel: &'static str,
    proto: &'static str,
    alpnprotocol: &'static str,
    reqsize: i64,
    respsize: i64,
    totalsize: i64,
    riskscore: i64,
    threatseverity: &'static str,
    threatname: &'static str,
    malwarecat: &'static str,
    malwareclass: &'static str,
    dlpeng: &'static str,
    dlpdict: &'static str,
    ssldecrypted: &'static str,
    ssl_rulename: &'static str,
    externalspr: &'static str,
    keyprotectiontype: &'static str,
    clienttlsversion: &'static str,
    srvtlsversion: &'static str,
    clientsslcipher: &'static str,
    srvsslcipher: &'static str,
    cltsslfailreason: &'static str,
    cltsslfailcount: i64,
    srvocspresult: &'static str,
    srvcertchainvalpass: &'static str,
    srvcertvalidationtype: &'static str,
    is_ssluntrustedca: &'static str,
    is_sslselfsigned: &'static str,
    is_sslexpiredca: &'static str,
    clt_sport: i64,
    srv_dport: i64,
}

#[allow(clippy::too_many_arguments)]
pub async fn run_direct_backfill(
    cfg: AppConfig,
    schema: Arc<SchemaDef>,
    total_rows: u64,
    days: u32,
    workers: usize,
    seed: u64,
    target_lps: u64,
    progress_every: u64,
    fleet: FleetConfig,
) -> Result<()> {
    let workers = workers.max(1);
    let days = days.max(1);
    let progress_every = progress_every.max(1);
    let total_rows = total_rows.max(1);
    let fleet = fleet.validated()?;

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
    let (_writer_control_tx, writer_control_rx) = mpsc::channel::<WriterControlMessage>(4);

    let writer_cfg = cfg.clone();
    let writer_schema = Arc::clone(&schema);
    let writer_metrics = Arc::clone(&metrics);
    let mut writer_shutdown = shutdown_rx.clone();
    let writer_task = tokio::spawn(async move {
        run_parquet_writer(
            writer_cfg,
            writer_schema,
            parsed_rx,
            writer_control_rx,
            writer_metrics,
            None,
            &mut writer_shutdown,
        )
        .await
    });

    let window_end = Utc::now();

    info!(
        total_rows,
        workers,
        days,
        target_lps,
        user_count = fleet.user_count,
        min_devices_per_user = fleet.min_devices_per_user,
        max_devices_per_user = fleet.max_devices_per_user,
        output = %cfg.writer.output_dir.display(),
        "starting direct parquet backfill"
    );

    let sent_total = Arc::new(AtomicU64::new(0));
    let pace_total = Arc::new(AtomicU64::new(0));
    let pace_start = Instant::now();
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
        let pace_total = Arc::clone(&pace_total);

        handles.push(tokio::spawn(async move {
            for idx in 0..worker_rows {
                let entropy = splitmix64(worker_seed ^ idx.wrapping_mul(0x9E37_79B9_7F4A_7C15));
                let event_time = synth_event_time(window_end, days, entropy);
                let ctx = synth_context(event_time, entropy, fleet);
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
                            &ctx,
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
                if target_lps > 0 {
                    let sequence = pace_total.fetch_add(1, Ordering::Relaxed) + 1;
                    wait_for_target_rate(pace_start, target_lps, sequence).await;
                }
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

async fn wait_for_target_rate(start: Instant, target_lps: u64, sequence: u64) {
    let expected_elapsed =
        std::time::Duration::from_secs_f64(sequence as f64 / target_lps.max(1) as f64);
    let elapsed = start.elapsed();
    if let Some(wait) = expected_elapsed.checked_sub(elapsed) {
        sleep(wait).await;
    }
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

fn synth_event_time(window_end: DateTime<Utc>, days: u32, entropy: u64) -> DateTime<Utc> {
    let day_offset = (entropy % days.max(1) as u64) as i64;
    let base_date = (window_end - Duration::days(day_offset)).date_naive();
    let hour = weighted_hour(splitmix64(entropy));
    let minute = ((entropy >> 8) % 60) as u32;
    let second = ((entropy >> 16) % 60) as u32;
    let naive = NaiveDateTime::new(
        base_date,
        chrono::NaiveTime::from_hms_opt(hour, minute, second).expect("valid hms"),
    );
    let mut event_time = DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc);
    while event_time > window_end {
        event_time -= Duration::days(1);
    }
    event_time
}

fn weighted_hour(entropy: u64) -> u32 {
    let total_weight = HOUR_WEIGHTS.iter().map(|v| *v as u64).sum::<u64>();
    let mut cursor = entropy % total_weight;
    for (hour, weight) in HOUR_WEIGHTS.iter().enumerate() {
        if cursor < *weight as u64 {
            return hour as u32;
        }
        cursor -= *weight as u64;
    }
    12
}

fn synth_context(event_time: DateTime<Utc>, entropy: u64, fleet: FleetConfig) -> SyntheticContext {
    let user_id = (entropy % fleet.user_count as u64) as u32;
    let user_entropy = splitmix64(user_id as u64);
    let device_count = device_count_for_user(user_entropy, fleet);
    let device_slot = ((entropy >> 11) % device_count as u64) as u8;
    let dept = pick_department(user_entropy);
    let location = pick_location(user_entropy);
    let device_kind = device_kind_for(user_entropy, device_slot);
    let event_kind = pick_event_kind(entropy);
    let app = pick_app(event_kind, entropy);
    let reqmethod = pick_reqmethod(event_kind, app, entropy);
    let proto = if matches!(event_kind, EventKind::ConnectionProbeFailure) {
        "HTTPS"
    } else if entropy.is_multiple_of(20) {
        "HTTP"
    } else {
        "HTTPS"
    };
    let srv_dport = if proto == "HTTP" { 80 } else { 443 };
    let alpnprotocol = if proto == "HTTP" || entropy.is_multiple_of(4) {
        "HTTP/1.1"
    } else {
        "HTTP/2"
    };
    let clt_sport = 49_152 + ((entropy >> 27) % 10_000) as i64;

    let src_ip = source_ip_for(location, user_id, device_slot);
    let nat_ip = nat_ip_for(location, user_id, device_slot);
    let public_ip = public_ip_for(location, user_id, device_slot);
    let dest_ip = destination_ip_for(app, entropy);

    let (action, respcode, reason, ruletype, rulelabel) = event_strings(event_kind);
    let (reqsize, respsize) = payload_sizes(event_kind, reqmethod, entropy);
    let totalsize = reqsize + respsize;
    let (riskscore, threatseverity, threatname, malwarecat, malwareclass) =
        threat_details(event_kind, entropy);
    let (dlpeng, dlpdict) = dlp_details(event_kind);
    let (
        ssldecrypted,
        ssl_rulename,
        externalspr,
        keyprotectiontype,
        clienttlsversion,
        srvtlsversion,
        clientsslcipher,
        srvsslcipher,
        cltsslfailreason,
        cltsslfailcount,
        srvocspresult,
        srvcertchainvalpass,
        srvcertvalidationtype,
        is_ssluntrustedca,
        is_sslselfsigned,
        is_sslexpiredca,
    ) = ssl_details(event_kind, entropy);

    SyntheticContext {
        event_time,
        user_id,
        device_slot,
        dept,
        location,
        app,
        device_kind,
        event_kind,
        src_ip,
        nat_ip,
        public_ip,
        dest_ip,
        reqmethod,
        respcode,
        action,
        reason,
        ruletype,
        rulelabel,
        proto,
        alpnprotocol,
        reqsize,
        respsize,
        totalsize,
        riskscore,
        threatseverity,
        threatname,
        malwarecat,
        malwareclass,
        dlpeng,
        dlpdict,
        ssldecrypted,
        ssl_rulename,
        externalspr,
        keyprotectiontype,
        clienttlsversion,
        srvtlsversion,
        clientsslcipher,
        srvsslcipher,
        cltsslfailreason,
        cltsslfailcount,
        srvocspresult,
        srvcertchainvalpass,
        srvcertvalidationtype,
        is_ssluntrustedca,
        is_sslselfsigned,
        is_sslexpiredca,
        clt_sport,
        srv_dport,
    }
}

fn device_count_for_user(user_entropy: u64, fleet: FleetConfig) -> u8 {
    let range = fleet
        .max_devices_per_user
        .saturating_sub(fleet.min_devices_per_user)
        .saturating_add(1);
    fleet.min_devices_per_user + (user_entropy % range as u64) as u8
}

fn pick_department(user_entropy: u64) -> &'static str {
    DEPARTMENTS[(user_entropy % DEPARTMENTS.len() as u64) as usize]
}

fn pick_location(user_entropy: u64) -> &'static LocationProfile {
    let roll = (user_entropy % 100) as u8;
    let idx = match roll {
        0..=13 => 0,
        14..=24 => 1,
        25..=32 => 2,
        33..=41 => 3,
        42..=47 => 4,
        48..=60 => 5,
        61..=68 => 6,
        69..=74 => 7,
        75..=79 => 8,
        _ => 9,
    };
    &OFFICE_LOCATIONS[idx]
}

fn device_kind_for(user_entropy: u64, device_slot: u8) -> DeviceKind {
    match device_slot {
        0 => DeviceKind::WindowsLaptop,
        1 => match user_entropy % 3 {
            0 => DeviceKind::IPhone,
            1 => DeviceKind::MacBook,
            _ => DeviceKind::AndroidPhone,
        },
        2 => {
            if user_entropy.is_multiple_of(2) {
                DeviceKind::WindowsVdi
            } else {
                DeviceKind::MacBook
            }
        }
        _ => DeviceKind::WindowsLaptop,
    }
}

fn pick_event_kind(entropy: u64) -> EventKind {
    match entropy % 1000 {
        0..=869 => EventKind::AllowedClean,
        870..=909 => EventKind::AllowedBackground,
        910..=939 => EventKind::BlockedCategory,
        940..=954 => EventKind::BlockedSslBadCert,
        955..=964 => EventKind::BlockedSslLowTls,
        965..=974 => EventKind::BlockedGeo,
        975..=984 => EventKind::ThreatBlock,
        985..=991 => EventKind::ConnectionProbeFailure,
        992..=995 => EventKind::FileControlBlock,
        996..=997 => EventKind::DlpBlock,
        _ => EventKind::ClientHandshakeFailure,
    }
}

fn pick_app(event_kind: EventKind, entropy: u64) -> &'static AppProfile {
    let idx = (splitmix64(entropy) % PRIMARY_APPS.len() as u64) as usize;
    match event_kind {
        EventKind::BlockedCategory => &SOCIAL_APPS[(entropy % SOCIAL_APPS.len() as u64) as usize],
        EventKind::BlockedGeo => &GEO_APPS[(entropy % GEO_APPS.len() as u64) as usize],
        EventKind::ThreatBlock => &THREAT_APPS[(entropy % THREAT_APPS.len() as u64) as usize],
        EventKind::FileControlBlock | EventKind::DlpBlock => {
            &DOWNLOAD_APPS[(entropy % DOWNLOAD_APPS.len() as u64) as usize]
        }
        _ => &PRIMARY_APPS[idx],
    }
}

fn pick_reqmethod(event_kind: EventKind, app: &AppProfile, entropy: u64) -> &'static str {
    match event_kind {
        EventKind::AllowedBackground => {
            if entropy.is_multiple_of(2) {
                "POST"
            } else {
                "GET"
            }
        }
        EventKind::FileControlBlock | EventKind::DlpBlock => "POST",
        EventKind::ClientHandshakeFailure => "CONNECT",
        EventKind::ConnectionProbeFailure => "GET",
        EventKind::BlockedCategory | EventKind::BlockedGeo | EventKind::ThreatBlock => {
            if entropy.is_multiple_of(5) {
                "POST"
            } else {
                "GET"
            }
        }
        _ => match app.path_family {
            PathFamily::ApiJson | PathFamily::GraphApi | PathFamily::Telemetry => {
                if entropy.is_multiple_of(3) {
                    "POST"
                } else {
                    "GET"
                }
            }
            PathFamily::AuthFlow => {
                if entropy.is_multiple_of(2) {
                    "POST"
                } else {
                    "GET"
                }
            }
            _ => "GET",
        },
    }
}

fn source_ip_for(location: &LocationProfile, user_id: u32, device_slot: u8) -> [u8; 4] {
    [
        10,
        location.src_subnet,
        ((user_id / 256) % 250) as u8,
        ((user_id % 200) as u8).saturating_add(20 + device_slot),
    ]
}

fn nat_ip_for(location: &LocationProfile, user_id: u32, device_slot: u8) -> [u8; 4] {
    [
        location.public_prefix.0,
        location.public_prefix.1,
        ((user_id / 256) % 250) as u8 + 1,
        ((user_id % 250) as u8).saturating_add(1 + device_slot),
    ]
}

fn public_ip_for(location: &LocationProfile, user_id: u32, device_slot: u8) -> [u8; 4] {
    [
        location.public_prefix.0,
        location.public_prefix.1.saturating_add(1),
        ((user_id / 256) % 250) as u8 + 1,
        ((user_id % 250) as u8).saturating_add(10 + device_slot),
    ]
}

fn destination_ip_for(app: &AppProfile, entropy: u64) -> [u8; 4] {
    let country_seed = splitmix64(hash_str(app.country) ^ entropy);
    let first = match app.country {
        "United States" => 34,
        "Ireland" => 52,
        "Germany" => 18,
        "United Kingdom" => 51,
        "The Netherlands" => 20,
        "Singapore" => 52,
        "Australia" => 13,
        "Russia" => 95,
        "China" => 110,
        "Iran" => 37,
        "Belarus" => 178,
        _ => 44,
    };
    [
        first,
        ((country_seed >> 8) % 240) as u8 + 1,
        ((country_seed >> 16) % 240) as u8 + 1,
        ((country_seed >> 24) % 240) as u8 + 1,
    ]
}

fn event_strings(
    event_kind: EventKind,
) -> (
    &'static str,
    &'static str,
    &'static str,
    &'static str,
    &'static str,
) {
    match event_kind {
        EventKind::AllowedClean => ("Allowed", "200", "Allowed", "None", "None"),
        EventKind::AllowedBackground => ("Allowed", "304", "Allowed", "None", "None"),
        EventKind::BlockedCategory => (
            "Blocked",
            "403",
            "Not allowed to browse this category",
            "URL Filtering",
            "URLF_STD_BLOCK",
        ),
        EventKind::BlockedSslBadCert => (
            "Blocked",
            "403",
            "Access denied due to bad server certificate",
            "SSL/TLS",
            "SSL_BAD_CERT",
        ),
        EventKind::BlockedSslLowTls => (
            "Blocked",
            "403",
            "Access denied due to low TLS version",
            "SSL/TLS",
            "SSL_MIN_TLS",
        ),
        EventKind::BlockedGeo => (
            "Blocked",
            "403",
            "Country block outbound request: not allowed to access sites in this country",
            "Advanced Threat Protection",
            "ATP_COUNTRY_BLOCK",
        ),
        EventKind::ThreatBlock => (
            "Blocked",
            "403",
            "Reputation block outbound request: phishing site",
            "Advanced Threat Protection",
            "ATP_REPUTATION_BLOCK",
        ),
        EventKind::ConnectionProbeFailure => (
            "Blocked",
            "0",
            "Blocked due to Server Probe Failure",
            "SSL/TLS",
            "SSL_PROBE_FAIL",
        ),
        EventKind::FileControlBlock => (
            "Blocked",
            "403",
            "Not allowed to upload/download files of this type",
            "File Type Control",
            "FTC_BLOCK",
        ),
        EventKind::DlpBlock => (
            "Blocked",
            "403",
            "Violates Compliance Category",
            "Data Loss Prevention",
            "DLP_BLOCK",
        ),
        EventKind::ClientHandshakeFailure => (
            "Blocked",
            "0",
            "Dropped due to failed client SSL/TLS handshake",
            "SSL/TLS",
            "SSL_CLT_HS_FAIL",
        ),
    }
}

fn payload_sizes(event_kind: EventKind, reqmethod: &str, entropy: u64) -> (i64, i64) {
    match event_kind {
        EventKind::AllowedClean => {
            let req = if reqmethod == "POST" {
                900 + ((entropy >> 8) % 7_000) as i64
            } else {
                350 + ((entropy >> 8) % 2_500) as i64
            };
            let resp = 5_000 + ((entropy >> 20) % 180_000) as i64;
            (req, resp)
        }
        EventKind::AllowedBackground => {
            let req = 300 + ((entropy >> 8) % 1_500) as i64;
            let resp = 200 + ((entropy >> 18) % 6_000) as i64;
            (req, resp)
        }
        EventKind::BlockedCategory | EventKind::BlockedGeo | EventKind::ThreatBlock => {
            let req = 400 + ((entropy >> 8) % 1_800) as i64;
            let resp = 256 + ((entropy >> 18) % 768) as i64;
            (req, resp)
        }
        EventKind::BlockedSslBadCert
        | EventKind::BlockedSslLowTls
        | EventKind::ConnectionProbeFailure
        | EventKind::ClientHandshakeFailure => {
            let req = 250 + ((entropy >> 8) % 1_600) as i64;
            (req, 0)
        }
        EventKind::FileControlBlock => {
            let req = 1_200 + ((entropy >> 8) % 4_000) as i64;
            (req, 0)
        }
        EventKind::DlpBlock => {
            let req = 3_000 + ((entropy >> 8) % 15_000) as i64;
            (req, 0)
        }
    }
}

fn threat_details(
    event_kind: EventKind,
    entropy: u64,
) -> (i64, &'static str, &'static str, &'static str, &'static str) {
    match event_kind {
        EventKind::ThreatBlock => {
            let names = [
                "Credential Phish",
                "C2 Beaconing",
                "Exploit Kit Landing Page",
                "Malvertising Payload",
            ];
            let severities = ["High", "Critical"];
            (
                80 + (entropy % 20) as i64,
                severities[(entropy % severities.len() as u64) as usize],
                names[(entropy % names.len() as u64) as usize],
                "Phishing",
                "Web Threats",
            )
        }
        EventKind::BlockedGeo => (55, "Medium", "None", "None", "None"),
        EventKind::BlockedCategory => (35, "Low", "None", "None", "None"),
        _ => (0, "None", "None", "None", "None"),
    }
}

fn dlp_details(event_kind: EventKind) -> (&'static str, &'static str) {
    match event_kind {
        EventKind::DlpBlock => ("PCI", "Credit Cards|Customer IDs"),
        _ => ("None", "None"),
    }
}

#[allow(clippy::type_complexity)]
fn ssl_details(
    event_kind: EventKind,
    entropy: u64,
) -> (
    &'static str,
    &'static str,
    &'static str,
    &'static str,
    &'static str,
    &'static str,
    &'static str,
    &'static str,
    &'static str,
    i64,
    &'static str,
    &'static str,
    &'static str,
    &'static str,
    &'static str,
    &'static str,
) {
    let keyprotection = if entropy.is_multiple_of(10) {
        "HSM Protection"
    } else {
        "Software Protection"
    };
    match event_kind {
        EventKind::BlockedSslBadCert => (
            "No",
            "Block Bad Cert",
            "Blocked",
            keyprotection,
            "TLS1_2",
            "TLS1_2",
            TLS12_CIPHERS[(entropy % TLS12_CIPHERS.len() as u64) as usize],
            TLS12_CIPHERS[((entropy >> 4) % TLS12_CIPHERS.len() as u64) as usize],
            "None",
            0,
            "Unknown",
            "Fail",
            "DV",
            "Fail",
            "Yes",
            if entropy.is_multiple_of(2) {
                "Yes"
            } else {
                "No"
            },
        ),
        EventKind::BlockedSslLowTls => (
            "No",
            "Minimum TLS",
            "Blocked",
            keyprotection,
            "TLS1_0",
            "TLS1_0",
            "RSA_WITH_AES_128_CBC_SHA",
            "RSA_WITH_AES_128_CBC_SHA",
            "None",
            0,
            "Unknown",
            "Fail",
            "DV",
            "None",
            "No",
            "No",
        ),
        EventKind::ConnectionProbeFailure => (
            "No",
            "Inspect All",
            "Blocked",
            keyprotection,
            "TLS1_2",
            "None",
            TLS12_CIPHERS[(entropy % TLS12_CIPHERS.len() as u64) as usize],
            "None",
            "None",
            0,
            "Unknown",
            "Unknown",
            "DV",
            "None",
            "None",
            "None",
        ),
        EventKind::ClientHandshakeFailure => (
            "No",
            "Inspect All",
            "Blocked",
            keyprotection,
            "TLS1_3",
            "None",
            TLS13_CIPHERS[(entropy % TLS13_CIPHERS.len() as u64) as usize],
            "None",
            "Handshake Failure",
            1,
            "Unknown",
            "Unknown",
            "DV",
            "None",
            "None",
            "None",
        ),
        _ => {
            let tls13 = !entropy.is_multiple_of(4);
            let client_tls = if tls13 { "TLS1_3" } else { "TLS1_2" };
            let server_tls = if tls13 { "TLS1_3" } else { "TLS1_2" };
            let client_cipher = if tls13 {
                TLS13_CIPHERS[(entropy % TLS13_CIPHERS.len() as u64) as usize]
            } else {
                TLS12_CIPHERS[(entropy % TLS12_CIPHERS.len() as u64) as usize]
            };
            let server_cipher = if tls13 {
                TLS13_CIPHERS[((entropy >> 4) % TLS13_CIPHERS.len() as u64) as usize]
            } else {
                TLS12_CIPHERS[((entropy >> 4) % TLS12_CIPHERS.len() as u64) as usize]
            };
            (
                "Yes",
                "Inspect All",
                "INSPECTED",
                keyprotection,
                client_tls,
                server_tls,
                client_cipher,
                server_cipher,
                "None",
                0,
                "GOOD",
                "PASS",
                if entropy.is_multiple_of(20) {
                    "OV"
                } else {
                    "DV"
                },
                "Pass",
                "No",
                "No",
            )
        }
    }
}

fn synth_value(
    field_name: &str,
    logical_type: &LogicalType,
    time_field: &str,
    time_format: &str,
    ctx: &SyntheticContext,
    entropy: u64,
) -> Option<String> {
    if field_name == time_field {
        return Some(ctx.event_time.format(time_format).to_string());
    }

    match field_name.to_ascii_lowercase().as_str() {
        "time" => Some(ctx.event_time.format(time_format).to_string()),
        "tz" => Some("GMT".to_string()),
        "epochtime" => Some(ctx.event_time.timestamp().to_string()),
        "login" => Some(user_login(ctx.user_id)),
        "ologin" => Some(user_alias(ctx.user_id)),
        "dept" => Some(ctx.dept.to_string()),
        "action" => Some(ctx.action.to_string()),
        "ruletype" => Some(ctx.ruletype.to_string()),
        "rulelabel" => Some(ctx.rulelabel.to_string()),
        "reason" => Some(ctx.reason.to_string()),
        "proto" => Some(ctx.proto.to_string()),
        "reqmethod" => Some(ctx.reqmethod.to_string()),
        "respcode" => Some(ctx.respcode.to_string()),
        "url" | "eurl" => Some(render_url(ctx, entropy)),
        "host" | "ehost" => Some(ctx.app.host.to_string()),
        "referer" | "ereferer" => Some(render_referer(ctx, entropy)),
        "ua" | "eua" => Some(render_user_agent(ctx.device_kind, entropy)),
        "appname" => Some(ctx.app.appname.to_string()),
        "appclass" => Some(ctx.app.appclass.to_string()),
        "app_status" => Some(ctx.app.app_status.to_string()),
        "urlclass" => Some(ctx.app.urlclass.to_string()),
        "urlsupercat" => Some(ctx.app.urlsupercat.to_string()),
        "urlcat" => Some(ctx.app.urlcat.to_string()),
        "contenttype" => Some(ctx.app.contenttype.to_string()),
        "unscannabletype" => Some(match ctx.event_kind {
            EventKind::FileControlBlock => "Encrypted File".to_string(),
            _ => "None".to_string(),
        }),
        "threatname" => Some(ctx.threatname.to_string()),
        "malwarecat" => Some(ctx.malwarecat.to_string()),
        "malwareclass" => Some(ctx.malwareclass.to_string()),
        "riskscore" => Some(ctx.riskscore.to_string()),
        "threatseverity" => Some(ctx.threatseverity.to_string()),
        "dlpeng" => Some(ctx.dlpeng.to_string()),
        "dlpdict" => Some(ctx.dlpdict.to_string()),
        "ssldecrypted" => Some(ctx.ssldecrypted.to_string()),
        "ssl_rulename" => Some(ctx.ssl_rulename.to_string()),
        "externalspr" => Some(ctx.externalspr.to_string()),
        "keyprotectiontype" => Some(ctx.keyprotectiontype.to_string()),
        "clienttlsversion" => Some(ctx.clienttlsversion.to_string()),
        "srvtlsversion" => Some(ctx.srvtlsversion.to_string()),
        "clientsslcipher" => Some(ctx.clientsslcipher.to_string()),
        "srvsslcipher" => Some(ctx.srvsslcipher.to_string()),
        "cltsslfailreason" => Some(ctx.cltsslfailreason.to_string()),
        "cltsslfailcount" => Some(ctx.cltsslfailcount.to_string()),
        "srvocspresult" => Some(ctx.srvocspresult.to_string()),
        "srvcertchainvalpass" => Some(ctx.srvcertchainvalpass.to_string()),
        "srvcertvalidationtype" => Some(ctx.srvcertvalidationtype.to_string()),
        "is_ssluntrustedca" => Some(ctx.is_ssluntrustedca.to_string()),
        "is_sslselfsigned" => Some(ctx.is_sslselfsigned.to_string()),
        "is_sslexpiredca" => Some(ctx.is_sslexpiredca.to_string()),
        "cip" => Some(format_ip(ctx.src_ip)),
        "cintip" => Some(format_ip(ctx.nat_ip)),
        "cpubip" => Some(format_ip(ctx.public_ip)),
        "sip" => Some(format_ip(ctx.dest_ip)),
        "clt_sport" => Some(ctx.clt_sport.to_string()),
        "srv_dport" => Some(ctx.srv_dport.to_string()),
        "srcip_country" => Some(ctx.location.country.to_string()),
        "dstip_country" => Some(ctx.app.country.to_string()),
        "is_src_cntry_risky" => Some("No".to_string()),
        "is_dst_cntry_risky" => Some(if is_risky_country(ctx.app.country) {
            "Yes".to_string()
        } else {
            "No".to_string()
        }),
        "location" => Some(ctx.location.location.to_string()),
        "userlocationname" => Some(ctx.location.userlocation.to_string()),
        "datacenter" => Some(ctx.location.datacenter.to_string()),
        "datacentercountry" => Some(ctx.location.datacenter_country.to_string()),
        "trafficredirectmethod" => Some(ctx.location.trafficredirectmethod.to_string()),
        "alpnprotocol" => Some(ctx.alpnprotocol.to_string()),
        "reqsize" => Some(ctx.reqsize.to_string()),
        "respsize" => Some(ctx.respsize.to_string()),
        "totalsize" => Some(ctx.totalsize.to_string()),
        "devicehostname" | "edevicehostname" => Some(device_hostname(
            ctx.location,
            ctx.user_id,
            ctx.device_slot,
            ctx.device_kind,
        )),
        "deviceowner" | "odeviceowner" => Some(user_alias(ctx.user_id)),
        _ => match logical_type {
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
            LogicalType::Timestamp => Some(ctx.event_time.to_rfc3339()),
            LogicalType::Ip => Some(format_ip([
                1 + (entropy % 223) as u8,
                ((entropy >> 8) % 255) as u8,
                ((entropy >> 16) % 255) as u8,
                1 + ((entropy >> 24) % 254) as u8,
            ])),
        },
    }
}

fn user_login(user_id: u32) -> String {
    format!("user{:05}@{}", user_id + 1, CORPORATE_DOMAIN)
}

fn user_alias(user_id: u32) -> String {
    format!("user{:05}", user_id + 1)
}

fn device_hostname(
    location: &LocationProfile,
    user_id: u32,
    device_slot: u8,
    device_kind: DeviceKind,
) -> String {
    let base = (user_id + 1) * 10 + device_slot as u32;
    match device_kind {
        DeviceKind::WindowsLaptop => format!("{}-W11-{base:06}", location.code),
        DeviceKind::MacBook => format!("{}-MAC-{base:06}", location.code),
        DeviceKind::IPhone => format!("{}-IPH-{base:06}", location.code),
        DeviceKind::AndroidPhone => format!("{}-AND-{base:06}", location.code),
        DeviceKind::WindowsVdi => format!("{}-VDI-{base:06}", location.code),
    }
}

fn render_user_agent(device_kind: DeviceKind, entropy: u64) -> String {
    match device_kind {
        DeviceKind::WindowsLaptop | DeviceKind::WindowsVdi => {
            let browser = if entropy.is_multiple_of(2) {
                "Chrome/146.0.0.0"
            } else {
                "Edg/146.0.0.0"
            };
            format!(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) {browser} Safari/537.36"
            )
        }
        DeviceKind::MacBook => "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15".to_string(),
        DeviceKind::IPhone => "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1".to_string(),
        DeviceKind::AndroidPhone => "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Mobile Safari/537.36".to_string(),
    }
}

fn render_url(ctx: &SyntheticContext, entropy: u64) -> String {
    let suffix = match ctx.app.path_family {
        PathFamily::ApiJson => format!(
            "/api/v1/resource/{}?tenant={}&trace={:08x}",
            entropy % 9_000,
            ctx.user_id % 250,
            entropy as u32
        ),
        PathFamily::AuthFlow => format!(
            "/oauth2/authorize?client_id=corp-app-{}&session={:08x}",
            ctx.user_id % 32,
            (entropy >> 12) as u32
        ),
        PathFamily::Download => format!(
            "/downloads/{:04}/{:04}/report-{:05}.zip",
            ctx.event_time.year(),
            ctx.event_time.ordinal(),
            entropy % 50_000
        ),
        PathFamily::GraphApi => format!(
            "/v1.0/users/{}/messages?$top={}",
            user_alias(ctx.user_id),
            10 + (entropy % 40)
        ),
        PathFamily::Search => format!(
            "/search?q=case+{}+{}&source=nssq",
            ctx.user_id % 8_000,
            entropy % 1_000
        ),
        PathFamily::StaticAsset => format!(
            "/assets/{:06x}/bundle-{}.js",
            (entropy >> 10) & 0x00ff_ffff,
            entropy % 12
        ),
        PathFamily::Telemetry => format!(
            "/ping?h={}&u={}&ts={}",
            ctx.app.host,
            user_alias(ctx.user_id),
            ctx.event_time.timestamp()
        ),
        PathFamily::WebPage => format!(
            "/news/{:04}/{:02}/{:02}/story-{:05}",
            ctx.event_time.year(),
            ctx.event_time.month(),
            ctx.event_time.day(),
            entropy % 100_000
        ),
    };
    format!("{}{}", ctx.app.host, suffix)
}

fn render_referer(ctx: &SyntheticContext, entropy: u64) -> String {
    let candidates = [
        "portal.corp.example/home",
        "intranet.corp.example/launchpad",
        "www.google.com/search",
        "edition.cnn.com/",
        "www.aol.de/",
        "www.uol.com.br/",
    ];
    let selected = candidates[(entropy % candidates.len() as u64) as usize];
    if selected.ends_with('/') || selected.contains('?') {
        selected.to_string()
    } else if matches!(ctx.app.path_family, PathFamily::Telemetry) {
        format!("{selected}?src={}", user_alias(ctx.user_id))
    } else {
        selected.to_string()
    }
}

fn format_ip(parts: [u8; 4]) -> String {
    format!("{}.{}.{}.{}", parts[0], parts[1], parts[2], parts[3])
}

fn hash_str(value: &str) -> u64 {
    value
        .bytes()
        .fold(0_u64, |acc, b| acc.wrapping_mul(131).wrapping_add(b as u64))
}

fn is_risky_country(country: &str) -> bool {
    matches!(country, "Russia" | "China" | "Iran" | "Belarus")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration as StdDuration;

    #[test]
    fn validated_fleet_requires_non_zero_users_and_devices() {
        let fleet = FleetConfig {
            user_count: DEFAULT_USER_COUNT,
            min_devices_per_user: 2,
            max_devices_per_user: 4,
        }
        .validated()
        .expect("valid");
        assert_eq!(fleet.user_count, DEFAULT_USER_COUNT);
    }

    #[test]
    fn device_count_stays_within_requested_bounds() {
        let fleet = FleetConfig {
            user_count: DEFAULT_USER_COUNT,
            min_devices_per_user: 2,
            max_devices_per_user: 4,
        };
        for sample in [0_u64, 1, 42, 9001, u32::MAX as u64] {
            let count = device_count_for_user(sample, fleet);
            assert!((2..=4).contains(&count));
        }
    }

    #[test]
    fn same_user_slot_resolves_to_stable_hostname() {
        let location = &OFFICE_LOCATIONS[0];
        let a = device_hostname(location, 1234, 1, DeviceKind::IPhone);
        let b = device_hostname(location, 1234, 1, DeviceKind::IPhone);
        let c = device_hostname(location, 1234, 0, DeviceKind::WindowsLaptop);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn weighted_hour_prefers_business_hours() {
        let morning = weighted_hour(0);
        let late = weighted_hour(u64::MAX);
        assert!(morning <= 23);
        assert!(late <= 23);
    }

    #[test]
    fn rate_wait_computes_expected_target_window() {
        let target_lps = 11_574_u64;
        let rows = 100_u64;
        let expected = StdDuration::from_secs_f64(rows as f64 / target_lps as f64);
        assert_eq!(expected.as_secs(), 0);
        assert!(expected.as_millis() > 0);
    }
}
