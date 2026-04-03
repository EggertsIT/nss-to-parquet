use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::Semaphore;
use tokio::sync::{mpsc, watch};
use tracing::{debug, error, info, warn};

use crate::config::AppConfig;
use crate::durability::Durability;
use crate::metrics::Metrics;
use crate::types::{DlqRecord, RawRecord};

#[derive(Clone)]
struct ConnectionCtx {
    raw_tx: mpsc::Sender<RawRecord>,
    dlq_tx: mpsc::Sender<DlqRecord>,
    metrics: Arc<Metrics>,
    durability: Option<Arc<Durability>>,
    max_line_bytes: usize,
    read_timeout_secs: u64,
}

pub async fn run_tcp_listener(
    cfg: AppConfig,
    raw_tx: mpsc::Sender<RawRecord>,
    dlq_tx: mpsc::Sender<DlqRecord>,
    metrics: Arc<Metrics>,
    durability: Option<Arc<Durability>>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(&cfg.listener.bind_addr).await?;
    let limiter = Arc::new(Semaphore::new(cfg.listener.max_connections.max(1)));
    info!(bind = %cfg.listener.bind_addr, "tcp listener started");

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    break;
                }
            }
            accepted = listener.accept() => {
                let (socket, peer) = match accepted {
                    Ok(v) => v,
                    Err(err) => {
                        warn!(error = %err, "accept failed");
                        continue;
                    }
                };

                let permit = match Arc::clone(&limiter).try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        metrics.observe_connection_dropped(1);
                        warn!("connection dropped: max_connections limit reached");
                        continue;
                    }
                };

                let peer_addr = peer.to_string();
                let ctx = ConnectionCtx {
                    raw_tx: raw_tx.clone(),
                    dlq_tx: dlq_tx.clone(),
                    metrics: Arc::clone(&metrics),
                    durability: durability.clone(),
                    max_line_bytes: cfg.listener.max_line_bytes,
                    read_timeout_secs: cfg.listener.read_timeout_secs.max(1),
                };
                let mut conn_shutdown = shutdown.clone();

                tokio::spawn(async move {
                    let _permit = permit;
                    if let Err(err) =
                        handle_connection(socket, Some(peer_addr), ctx, &mut conn_shutdown).await
                    {
                        error!(error = %err, "connection handler failed");
                    }
                });
            }
        }
    }

    info!("tcp listener stopped");
    Ok(())
}

async fn handle_connection(
    socket: tokio::net::TcpStream,
    peer_addr: Option<String>,
    ctx: ConnectionCtx,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<()> {
    let mut reader = BufReader::new(socket);
    let mut buf = Vec::with_capacity(8192);

    loop {
        buf.clear();
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    break;
                }
            }
            result = tokio::time::timeout(Duration::from_secs(ctx.read_timeout_secs), reader.read_until(b'\n', &mut buf)) => {
                let read = match result {
                    Ok(Ok(read)) => read,
                    Ok(Err(err)) => return Err(err.into()),
                    Err(_) => {
                        ctx.metrics.observe_connection_timeout(1);
                        warn!(peer = ?peer_addr, "closing idle connection due to read timeout");
                        break;
                    }
                };
                if read == 0 {
                    break;
                }
                if read > ctx.max_line_bytes {
                    ctx.metrics.observe_parsed_error(1);
                    let _ = ctx.dlq_tx.send(DlqRecord {
                        raw_line: String::from_utf8_lossy(&buf).into_owned(),
                        reason: format!("line exceeded max_line_bytes ({})", ctx.max_line_bytes),
                        peer_addr: peer_addr.clone(),
                        received_at: Utc::now(),
                    }).await;
                    continue;
                }

                while matches!(buf.last(), Some(b'\n') | Some(b'\r')) {
                    buf.pop();
                }
                if buf.is_empty() {
                    continue;
                }

                let line = String::from_utf8_lossy(&buf).into_owned();
                let now = Utc::now();
                let spool_seq = if let Some(durability) = ctx.durability.as_ref() {
                    match durability.append(&line, peer_addr.as_deref(), now) {
                        Ok(seq) => seq,
                        Err(err) => {
                            ctx.metrics.observe_durability_error(1);
                            error!(error = %err, "durability append failed; dropping record");
                            continue;
                        }
                    }
                } else {
                    None
                };
                let rec = RawRecord {
                    line,
                    peer_addr: peer_addr.clone(),
                    received_at: now,
                    spool_seq,
                };
                ctx.metrics.observe_raw_received(1);
                if ctx.raw_tx.send(rec).await.is_err() {
                    debug!("raw record channel closed; stopping connection task");
                    break;
                }
            }
        }
    }
    Ok(())
}
