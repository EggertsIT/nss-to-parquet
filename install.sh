#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="nss-ingestor"
SERVICE_USER="nssingestor"
SERVICE_GROUP="nssingestor"
CONFIG_DIR="/etc/nss-ingestor"
STATE_DIR="/var/lib/nss-ingestor"
BIN_PATH="/usr/local/bin/nss-ingestor"
UNIT_PATH="/etc/systemd/system/nss-ingestor.service"

INSTALL_DEPS=0
INSTALL_RUST=0
SKIP_BUILD=0
NO_START=0
ALLOW_UNSUPPORTED_OS=0
ALLOW_ROOT=0
NO_PROMPT_FEED_TEMPLATE=0
FEED_TEMPLATE="${NSS_FEED_TEMPLATE:-}"
FEED_TEMPLATE_FILE=""
LISTENER_BIND_ADDR="${LISTENER_BIND_ADDR:-0.0.0.0:514}"
METRICS_BIND_ADDR="${METRICS_BIND_ADDR:-127.0.0.1:9090}"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="${SCRIPT_DIR}"

log() {
  printf '[install] %s\n' "$*"
}

warn() {
  printf '[install][warn] %s\n' "$*" >&2
}

die() {
  printf '[install][error] %s\n' "$*" >&2
  exit 1
}

usage() {
  cat <<'USAGE'
Usage: ./install.sh [options]

Options:
  --install-deps          Install OS dependencies via dnf.
  --install-rust          Install Rust via rustup if cargo is missing.
  --skip-build            Skip cargo build and install existing target/release binary.
  --no-start              Do not enable/start systemd service.
  --bind-addr ADDR        Listener bind address (default: 0.0.0.0:514).
  --metrics-addr ADDR     Metrics bind address (default: 127.0.0.1:9090).
  --feed-template TEXT    NSS feed output template CSV line to generate schema.
  --feed-template-file P  File containing NSS feed output template CSV line.
  --no-prompt-feed-template
                          Do not prompt interactively for NSS feed template.
  --allow-unsupported-os  Continue even if OS is not RHEL/Rocky family.
  --allow-root            Allow direct root execution (not recommended).
  -h, --help              Show this help.
USAGE
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

is_supported_os() {
  [[ -r /etc/os-release ]] || return 1
  # shellcheck disable=SC1091
  . /etc/os-release
  local id_l="${ID:-}"
  local like_l="${ID_LIKE:-}"
  id_l="${id_l,,}"
  like_l="${like_l,,}"
  [[ "$id_l" =~ ^(rhel|rocky|almalinux|ol|centos)$ ]] || [[ "$like_l" == *"rhel"* ]]
}

setup_sudo() {
  if [[ "${EUID}" -eq 0 ]]; then
    SUDO=()
    INVOKER_USER="${SUDO_USER:-root}"
    if [[ "$INVOKER_USER" == "root" ]]; then
      warn "running from a root account is discouraged; run as a privileged user and let sudo handle elevation."
    fi
  else
    require_cmd sudo
    SUDO=(sudo)
    INVOKER_USER="$(id -un)"
  fi

  local invoker_home
  invoker_home="$(getent passwd "$INVOKER_USER" | awk -F: '{print $6}')"
  if [[ -z "$invoker_home" ]]; then
    invoker_home="$HOME"
  fi
  INVOKER_HOME="$invoker_home"
}

run_as_invoker() {
  if [[ "$INVOKER_USER" == "$(id -un)" ]]; then
    "$@"
  else
    "${SUDO[@]}" -u "$INVOKER_USER" "$@"
  fi
}

ensure_deps() {
  require_cmd dnf
  log "Installing OS dependencies with dnf"
  "${SUDO[@]}" dnf -y install \
    git curl gcc gcc-c++ make cmake pkgconfig \
    openssl-devel clang llvm \
    policycoreutils-python-utils
}

ensure_cargo() {
  if command -v cargo >/dev/null 2>&1; then
    CARGO_BIN="$(command -v cargo)"
    return
  fi

  if [[ -x "${INVOKER_HOME}/.cargo/bin/cargo" ]]; then
    CARGO_BIN="${INVOKER_HOME}/.cargo/bin/cargo"
    return
  fi

  if [[ "$INSTALL_RUST" -eq 0 ]]; then
    die "cargo not found. Re-run with --install-rust or install Rust manually."
  fi

  require_cmd curl
  log "Installing Rust toolchain for user ${INVOKER_USER}"
  run_as_invoker bash -lc 'curl https://sh.rustup.rs -sSf | sh -s -- -y'
  CARGO_BIN="${INVOKER_HOME}/.cargo/bin/cargo"
  [[ -x "$CARGO_BIN" ]] || die "cargo was not found after rustup installation"
}

build_binary() {
  if [[ "$SKIP_BUILD" -eq 1 ]]; then
    log "Skipping build (--skip-build)"
    return
  fi

  log "Building release binary with cargo"
  run_as_invoker "$CARGO_BIN" build --release --locked
}

write_config_template() {
  local cfg_path="$1"
  cat >"$cfg_path" <<EOF
[listener]
bind_addr = "${LISTENER_BIND_ADDR}"
max_line_bytes = 262144
read_timeout_secs = 30
max_connections = 1024
ingress_channel_capacity = 50000
parsed_channel_capacity = 50000

[schema]
path = "${CONFIG_DIR}/schema.yaml"
time_field = "time"
time_format = "%a %b %d %H:%M:%S %Y"
timezone = "UTC"
strict_type_validation = true

[writer]
output_dir = "${STATE_DIR}/data"
batch_rows = 10000
flush_interval_secs = 5
target_file_rows = 250000
compression = "zstd"

[dlq]
path = "${STATE_DIR}/dlq"
channel_capacity = 20000

[retention]
enabled = true
local_days = 14
sweep_interval_secs = 3600

[metrics]
enabled = true
bind_addr = "${METRICS_BIND_ADDR}"
dashboard_enabled = true
stats_window_hours = 24
degraded_error_ratio = 0.02
critical_error_ratio = 0.10
degraded_stale_seconds = 300
critical_stale_seconds = 900

[durability]
enabled = true
path = "${STATE_DIR}/spool"
fsync_every = 100
max_log_bytes = 536870912
EOF
}

write_unit_template() {
  local unit_tmp="$1"
  cat >"$unit_tmp" <<EOF
[Unit]
Description=NSS Ingestor (TCP -> Parquet)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${SERVICE_USER}
Group=${SERVICE_GROUP}
WorkingDirectory=${STATE_DIR}
ExecStart=${BIN_PATH} run --config ${CONFIG_DIR}/config.toml
Restart=always
RestartSec=5
LimitNOFILE=262144
NoNewPrivileges=true
AmbientCapabilities=CAP_NET_BIND_SERVICE
CapabilityBoundingSet=CAP_NET_BIND_SERVICE
PrivateTmp=true
ProtectSystem=full
ProtectHome=true
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true
ReadWritePaths=${STATE_DIR}
ReadOnlyPaths=${CONFIG_DIR}

[Install]
WantedBy=multi-user.target
EOF
}

install_or_stage() {
  local src="$1"
  local dest="$2"
  local mode="$3"
  local owner="$4"
  local group="$5"

  if [[ -f "$dest" ]]; then
    "${SUDO[@]}" install -m "$mode" -o "$owner" -g "$group" "$src" "${dest}.new"
    warn "Existing ${dest} preserved. Wrote ${dest}.new"
  else
    "${SUDO[@]}" install -m "$mode" -o "$owner" -g "$group" "$src" "$dest"
  fi
}

install_layout() {
  [[ -f "${REPO_DIR}/Cargo.toml" ]] || die "Cargo.toml not found in ${REPO_DIR}"

  local built_bin="${REPO_DIR}/target/release/nss-ingestor"
  [[ -x "$built_bin" ]] || die "release binary not found: ${built_bin}"

  if ! id -u "$SERVICE_USER" >/dev/null 2>&1; then
    log "Creating service user ${SERVICE_USER}"
    "${SUDO[@]}" useradd --system --create-home --home-dir "$STATE_DIR" --shell /sbin/nologin "$SERVICE_USER"
  fi

  log "Creating directories"
  "${SUDO[@]}" install -d -m 0750 -o root -g "$SERVICE_GROUP" "$CONFIG_DIR"
  "${SUDO[@]}" install -d -m 0750 -o "$SERVICE_USER" -g "$SERVICE_GROUP" "$STATE_DIR"
  "${SUDO[@]}" install -d -m 0750 -o "$SERVICE_USER" -g "$SERVICE_GROUP" "${STATE_DIR}/data"
  "${SUDO[@]}" install -d -m 0750 -o "$SERVICE_USER" -g "$SERVICE_GROUP" "${STATE_DIR}/dlq"
  "${SUDO[@]}" install -d -m 0750 -o "$SERVICE_USER" -g "$SERVICE_GROUP" "${STATE_DIR}/spool"

  log "Installing binary to ${BIN_PATH}"
  "${SUDO[@]}" install -m 0755 "$built_bin" "$BIN_PATH"

  local tmp_cfg tmp_unit tmp_feed_input tmp_schema
  tmp_cfg="$(mktemp)"
  tmp_unit="$(mktemp)"
  tmp_feed_input="$(mktemp)"
  tmp_schema="$(mktemp)"
  trap "rm -f '$tmp_cfg' '$tmp_unit' '$tmp_feed_input' '$tmp_schema'" EXIT

  write_config_template "$tmp_cfg"
  install_or_stage "$tmp_cfg" "${CONFIG_DIR}/config.toml" 0640 root "$SERVICE_GROUP"

  local schema_dest="${CONFIG_DIR}/schema.yaml"
  local use_generated_schema=0

  if [[ -z "$FEED_TEMPLATE" && -n "$FEED_TEMPLATE_FILE" ]]; then
    [[ -f "$FEED_TEMPLATE_FILE" ]] || die "feed template file not found: $FEED_TEMPLATE_FILE"
    FEED_TEMPLATE="$(cat "$FEED_TEMPLATE_FILE")"
  fi

  if [[ -z "$FEED_TEMPLATE" && "$NO_PROMPT_FEED_TEMPLATE" -eq 0 && ! -f "$schema_dest" && -t 0 && -t 1 ]]; then
    log "Paste your NSS feed output template (single CSV line), then press Enter."
    log "Press Enter with empty input to skip and use schema.example.yaml."
    IFS= read -r FEED_TEMPLATE || true
  fi

  if [[ -n "$FEED_TEMPLATE" ]]; then
    printf '%s\n' "$FEED_TEMPLATE" >"$tmp_feed_input"
    log "Generating schema from provided NSS feed template"
    "$built_bin" generate-schema --feed-template-file "$tmp_feed_input" --output "$tmp_schema" --force
    install_or_stage "$tmp_schema" "$schema_dest" 0640 root "$SERVICE_GROUP"
    use_generated_schema=1
  else
    install_or_stage "${REPO_DIR}/schema.example.yaml" "$schema_dest" 0640 root "$SERVICE_GROUP"
  fi

  write_unit_template "$tmp_unit"
  install_or_stage "$tmp_unit" "$UNIT_PATH" 0644 root root

  log "Validating config as ${SERVICE_USER}"
  "${SUDO[@]}" -u "$SERVICE_USER" "$BIN_PATH" validate-config --config "${CONFIG_DIR}/config.toml"
  if [[ "$use_generated_schema" -eq 1 ]]; then
    log "Installed schema generated from NSS feed template"
  else
    warn "Installed default schema.example.yaml (no feed template provided)"
  fi

  if command -v systemctl >/dev/null 2>&1 && [[ -d /run/systemd/system ]]; then
    log "Reloading systemd units"
    "${SUDO[@]}" systemctl daemon-reload
    if [[ "$NO_START" -eq 0 ]]; then
      log "Enabling and starting ${SERVICE_NAME}"
      "${SUDO[@]}" systemctl enable --now "$SERVICE_NAME"
      "${SUDO[@]}" systemctl --no-pager --full status "$SERVICE_NAME" || true
    else
      log "Skipping service start (--no-start)"
    fi
  else
    warn "systemd not detected. Skipping unit reload/start."
  fi
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --install-deps)
        INSTALL_DEPS=1
        shift
        ;;
      --install-rust)
        INSTALL_RUST=1
        shift
        ;;
      --skip-build)
        SKIP_BUILD=1
        shift
        ;;
      --no-start)
        NO_START=1
        shift
        ;;
      --bind-addr)
        [[ $# -ge 2 ]] || die "--bind-addr requires a value"
        LISTENER_BIND_ADDR="$2"
        shift 2
        ;;
      --metrics-addr)
        [[ $# -ge 2 ]] || die "--metrics-addr requires a value"
        METRICS_BIND_ADDR="$2"
        shift 2
        ;;
      --feed-template)
        [[ $# -ge 2 ]] || die "--feed-template requires a value"
        FEED_TEMPLATE="$2"
        shift 2
        ;;
      --feed-template-file)
        [[ $# -ge 2 ]] || die "--feed-template-file requires a value"
        FEED_TEMPLATE_FILE="$2"
        shift 2
        ;;
      --no-prompt-feed-template)
        NO_PROMPT_FEED_TEMPLATE=1
        shift
        ;;
      --allow-unsupported-os)
        ALLOW_UNSUPPORTED_OS=1
        shift
        ;;
      --allow-root)
        ALLOW_ROOT=1
        shift
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        die "unknown argument: $1"
        ;;
    esac
  done
}

main() {
  parse_args "$@"
  setup_sudo

  if [[ -n "$FEED_TEMPLATE" && -n "$FEED_TEMPLATE_FILE" ]]; then
    die "use either --feed-template or --feed-template-file, not both"
  fi

  if [[ "$INVOKER_USER" == "root" && "$INSTALL_RUST" -eq 1 && "$ALLOW_ROOT" -eq 0 ]]; then
    die "--install-rust from a root account would install Rust under /root. Run from a privileged non-root user (sudo), or pass --allow-root if intentional."
  fi

  if ! is_supported_os && [[ "$ALLOW_UNSUPPORTED_OS" -eq 0 ]]; then
    die "unsupported OS. This installer targets RHEL/Rocky family. Use --allow-unsupported-os to continue."
  fi

  cd "$REPO_DIR"

  if [[ "$INSTALL_DEPS" -eq 1 ]]; then
    ensure_deps
  fi

  ensure_cargo
  log "Using cargo at ${CARGO_BIN}"
  build_binary
  install_layout

  log "Install complete."
  log "Check service: sudo systemctl status ${SERVICE_NAME} --no-pager"
  log "Metrics: curl -s http://${METRICS_BIND_ADDR}/metrics | head"
}

main "$@"
