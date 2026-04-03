# nss-ingestor

`nss-ingestor` is a Rust service that ingests Zscaler NSS web logs over TCP and writes partitioned Parquet files for fast local analytics (DuckDB-first).

## What It Does

- Listens for newline-delimited NSS records on TCP (default `0.0.0.0:514`)
- Parses CSV-style records using an external, ordered schema file
- Optionally enforces strict type validation per schema field
- Converts rows into typed Arrow columns and writes Parquet with ZSTD compression
- Partitions output as `dt=YYYY-MM-DD/hour=HH` for efficient time-range queries
- Uses atomic Parquet finalize (`.tmp` -> rename) to avoid exposing partial files
- Writes malformed records to a dead-letter queue (DLQ)
- Exposes Prometheus metrics plus built-in stats API/dashboard
- Supports durability spool + replay with checkpointed commit (at-least-once)
- Cleans up old partitions using retention policy (default 14 days)

## Architecture

Pipeline:
1. `TCP Listener` receives lines from NSS feed.
   Before enqueue, records can be appended to local durability spool.
2. `Parser` validates field count and extracts event time.
   If strict type validation is enabled, invalid typed fields go to DLQ.
3. `Writer` batches rows and writes Parquet by day/hour partition.
   After successful write, durability checkpoint is committed.
4. `DLQ` stores malformed/unparseable lines.
5. `Retention` periodically deletes old local partitions.

### Flowchart

```mermaid
flowchart TD
    A[Zscaler NSS TCP Feed] --> B[TCP Listener]
    B -->|append line| C[Durability Spool Log]
    C -->|startup replay uncommitted| B
    B --> D[Raw Channel]
    D --> E[Parser]
    E -->|valid record| F[Parsed Channel]
    E -->|parse/type error| G[DLQ Writer]
    G --> H[DLQ Files]
    E -->|commit replayed bad seq| I[Durability Checkpoint]
    F --> J[Parquet Writer]
    J -->|write batch to .tmp| K[Temp Parquet]
    K -->|atomic rename| L[Partitioned Parquet dt=.../hour=...]
    J -->|commit max written seq| I
    M[Retention Worker] -->|delete old partitions| L
    N[Metrics/Stats] --> O[/metrics]
    N --> P[/api/stats]
    N --> Q[/dashboard]
    B --> N
    E --> N
    J --> N
    G --> N
    C --> N
```

## Prerequisites

- Rust toolchain (tested with `rustc 1.94.1`)
- Network access from NSS sender to this host on TCP port `514` (or custom port)
- Sufficient disk for Parquet + DLQ

For non-root processes binding to port `<1024` on Linux:

```bash
sudo setcap 'cap_net_bind_service=+ep' ./target/release/nss-ingestor
```

Or run on a high port (example `5514`) and use network forwarding.

## Configuration

Use [config.example.toml](./config.example.toml) and [schema.example.yaml](./schema.example.yaml) as templates.

### `config.toml` sections

- `[listener]`: bind address, max line size, channel capacities
- `[schema]`: schema path, time field name, time format, timezone
- `[writer]`: output directory, batch/flush/rotation settings, compression
- `[dlq]`: DLQ directory and queue capacity
- `[retention]`: enable/disable, local days, sweep interval
- `[metrics]`: metrics/admin bind address and dashboard/health thresholds
- `[durability]`: local spool and replay settings

Important `[listener]` keys:

- `max_connections` (drop new connections above this limit)
- `read_timeout_secs` (idle connection timeout)

Important `[metrics]` keys:

- `dashboard_enabled` (default `true`)
- `stats_window_hours` (default `24`)
- `degraded_error_ratio` / `critical_error_ratio`
- `degraded_stale_seconds` / `critical_stale_seconds`

Important `[schema]` key:

- `strict_type_validation` (default `true`)

Important `[durability]` keys:

- `enabled` (default `true`)
- `path` (local spool directory)
- `fsync_every` (durability/throughput tradeoff; lower is safer, higher is faster)
- `max_log_bytes` (trigger compaction when spool log exceeds this size)

### `schema.yaml` requirements

- Field order must exactly match NSS feed output order.
- Each field must declare:
  - `name`
  - `type` (`string`, `int64`, `float64`, `boolean`, `timestamp`, `ip`)
  - optional `nullable` (default `true`)
- The configured `time_field` must exist in schema and be parseable with `time_format` and `timezone`.

## Build and Run

Build:

```bash
cargo build --release
```

Run:

```bash
./target/release/nss-ingestor run --config ./config.toml
```

## Upload to GitHub

Create an empty repository in GitHub (no README/license/gitignore), then from this project directory:

```bash
git add .
git commit -m "Initial commit: NSS TCP to Parquet ingestor"
git remote add origin git@github.com:<org-or-user>/nss-ingestor.git
git push -u origin main
```

Verify what will be versioned:

```bash
git status
git ls-files
```

## RHEL 9 / Rocky 9 Installation and Setup

This section is a production-style setup for `RHEL 9` and `Rocky Linux 9`.

### 1. Install OS dependencies

```bash
sudo dnf -y install \
  git curl gcc gcc-c++ make cmake pkgconfig \
  openssl-devel clang llvm \
  policycoreutils-python-utils
```

### 2. Install Rust toolchain

```bash
curl https://sh.rustup.rs -sSf | sh -s -- -y
source "$HOME/.cargo/env"
rustc --version
cargo --version
```

### 3. Build the binary

From your project checkout:

```bash
cargo build --release
```

Install binary:

```bash
sudo install -m 0755 target/release/nss-ingestor /usr/local/bin/nss-ingestor
```

### 4. Create service user and directories

```bash
sudo useradd --system --create-home --home-dir /var/lib/nss-ingestor --shell /sbin/nologin nssingestor

sudo install -d -m 0750 -o root -g nssingestor /etc/nss-ingestor
sudo install -d -m 0750 -o nssingestor -g nssingestor /var/lib/nss-ingestor/data
sudo install -d -m 0750 -o nssingestor -g nssingestor /var/lib/nss-ingestor/dlq
sudo install -d -m 0750 -o nssingestor -g nssingestor /var/lib/nss-ingestor/spool
```

### 5. Install config and schema

```bash
sudo install -m 0640 -o root -g nssingestor config.example.toml /etc/nss-ingestor/config.toml
sudo install -m 0640 -o root -g nssingestor schema.example.yaml /etc/nss-ingestor/schema.yaml
```

Edit `/etc/nss-ingestor/config.toml` and set production paths:

```toml
[schema]
path = "/etc/nss-ingestor/schema.yaml"

[writer]
output_dir = "/var/lib/nss-ingestor/data"

[dlq]
path = "/var/lib/nss-ingestor/dlq"

[durability]
enabled = true
path = "/var/lib/nss-ingestor/spool"
fsync_every = 100
max_log_bytes = 536870912

[listener]
bind_addr = "0.0.0.0:514"
max_connections = 1024
read_timeout_secs = 30
```

### 6. Validate config before starting service

```bash
sudo -u nssingestor /usr/local/bin/nss-ingestor validate-config --config /etc/nss-ingestor/config.toml
```

### 7. Create systemd service

Create `/etc/systemd/system/nss-ingestor.service`:

```ini
[Unit]
Description=NSS Ingestor (TCP -> Parquet)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=nssingestor
Group=nssingestor
WorkingDirectory=/var/lib/nss-ingestor
ExecStart=/usr/local/bin/nss-ingestor run --config /etc/nss-ingestor/config.toml
Restart=always
RestartSec=5
LimitNOFILE=262144
NoNewPrivileges=true
AmbientCapabilities=CAP_NET_BIND_SERVICE
CapabilityBoundingSet=CAP_NET_BIND_SERVICE

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now nss-ingestor
sudo systemctl status nss-ingestor --no-pager
```

### 8. Configure firewall (if needed)

Allow NSS ingest port:

```bash
sudo firewall-cmd --permanent --add-port=514/tcp
sudo firewall-cmd --reload
```

If metrics/dashboard should be remotely reachable, also allow `9090/tcp`:

```bash
sudo firewall-cmd --permanent --add-port=9090/tcp
sudo firewall-cmd --reload
```

### 9. SELinux notes (Enforcing mode)

In most deployments, the service runs correctly with the unit above.
If SELinux blocks access, check denials first:

```bash
sudo ausearch -m AVC,USER_AVC -ts recent | audit2why
```

Then apply a targeted local policy only if required:

```bash
sudo ausearch -m AVC,USER_AVC -ts recent | audit2allow -M nss_ingestor_local
sudo semodule -i nss_ingestor_local.pp
```

### 10. Verify runtime health

```bash
sudo journalctl -u nss-ingestor -f
ss -ltn | grep ':514'
curl -s http://127.0.0.1:9090/metrics | head
curl -s http://127.0.0.1:9090/api/stats
```

Open dashboard locally:

```bash
xdg-open http://127.0.0.1:9090/dashboard
```

### 11. Point Zscaler NSS feed

Configure your NSS feed destination to this host IP on `TCP 514` (or your custom `listener.bind_addr` port).

## Validation Commands

Validate config + schema linkage:

```bash
cargo run -- validate-config --config ./config.toml
```

Validate schema and sample lines:

```bash
cargo run -- validate-schema --schema ./schema.yaml --sample ./sample.log
```

## Output Layout

Parquet files are written under `writer.output_dir`:

```text
data/
  dt=2026-04-03/
    hour=15/
      part-000000.parquet
      part-000001.parquet
```

DLQ files are written daily:

```text
dlq/
  dlq-2026-04-03.log
```

DLQ line format:

```text
<received_at_rfc3339>\t<peer_addr>\t<reason>\t<raw_line>
```

## Metrics

If enabled, scrape:

```text
GET /metrics
```

Structured stats API:

```text
GET /api/stats
```

Built-in dashboard (if `metrics.dashboard_enabled = true`):

```text
GET /dashboard
```

Exposed counters:

- `nss_ingestor_raw_received_total`
- `nss_ingestor_parsed_ok_total`
- `nss_ingestor_parsed_error_total`
- `nss_ingestor_written_rows_total`
- `nss_ingestor_written_files_total`
- `nss_ingestor_dlq_rows_total`
- `nss_ingestor_durability_replayed_total`
- `nss_ingestor_durability_commits_total`
- `nss_ingestor_durability_errors_total`
- `nss_ingestor_connection_dropped_total`
- `nss_ingestor_connection_timeouts_total`

`/api/stats` includes:

- totals (ingested, parsed ok/error, written rows/files, DLQ)
- rates (`1m` and `5m` per second for ingest/write/errors/DLQ)
- freshness (seconds since last ingest/write activity)
- health status (`ok`, `degraded`, `critical`) and reasons
- in-memory 24h minute trends

## DuckDB Query Examples

Top actions by hour:

```sql
SELECT
  date_trunc('hour', "time") AS hour_bucket,
  action,
  count(*) AS events
FROM read_parquet('data/dt=*/hour=*/*.parquet')
GROUP BY 1, 2
ORDER BY hour_bucket DESC, events DESC
LIMIT 100;
```

Top blocked domains:

```sql
SELECT
  host,
  count(*) AS blocked_events
FROM read_parquet('data/dt=*/hour=*/*.parquet')
WHERE action = 'Blocked'
GROUP BY host
ORDER BY blocked_events DESC
LIMIT 50;
```

## Operations Notes

- With `[durability].enabled = true`, delivery semantics are **at-least-once**.
- If durability is disabled, semantics are **best-effort**.
- Keep schema synchronized with NSS feed template changes.
- Tune `batch_rows` and `target_file_rows` to balance write throughput vs file size.
- Keep retention enabled to avoid unbounded local disk growth.

## Troubleshooting

- `Operation not permitted` on startup:
  - Port permission issue (common on `:514`) or sandbox/network policy.
- High parser errors:
  - NSS feed order drifted from `schema.yaml`
  - Different delimiter/quoting behavior than expected
  - Incorrect `time_field`/`time_format`/`timezone`
- No Parquet files:
  - Check listener bind, ingress traffic, parser metrics, and DLQ contents.
- High `nss_ingestor_connection_dropped_total`:
  - Increase `listener.max_connections` or scale out multiple ingestors.
- High `nss_ingestor_durability_errors_total`:
  - Check spool filesystem health/permissions and available disk.
