use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::config::DurabilityConfig;
use crate::types::RawRecord;

#[derive(Debug)]
pub struct Durability {
    enabled: bool,
    log_path: PathBuf,
    checkpoint_path: PathBuf,
    file: Mutex<File>,
    next_seq: AtomicU64,
    committed_seq: AtomicU64,
    fsync_every: u64,
    max_log_bytes: u64,
    append_count: AtomicU64,
}

#[derive(Debug, Serialize, Deserialize)]
struct SpoolEntry {
    seq: u64,
    peer_addr: Option<String>,
    received_at: DateTime<Utc>,
    line: String,
}

impl Durability {
    pub fn initialize(cfg: &DurabilityConfig) -> Result<Option<Arc<Self>>> {
        if !cfg.enabled {
            return Ok(None);
        }

        std::fs::create_dir_all(&cfg.path)
            .with_context(|| format!("failed to create spool dir {}", cfg.path.display()))?;

        let log_path = cfg.path.join("spool.log");
        let checkpoint_path = cfg.path.join("checkpoint");
        let committed = read_checkpoint(&checkpoint_path)?;
        let last_seq = scan_last_seq(&log_path)?;
        let next_seq = last_seq.max(committed);

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .with_context(|| format!("failed to open spool log {}", log_path.display()))?;

        Ok(Some(Arc::new(Self {
            enabled: true,
            log_path,
            checkpoint_path,
            file: Mutex::new(file),
            next_seq: AtomicU64::new(next_seq),
            committed_seq: AtomicU64::new(committed),
            fsync_every: cfg.fsync_every.max(1),
            max_log_bytes: cfg.max_log_bytes.max(1),
            append_count: AtomicU64::new(0),
        })))
    }

    pub fn append(
        &self,
        line: &str,
        peer_addr: Option<&str>,
        received_at: DateTime<Utc>,
    ) -> Result<Option<u64>> {
        if !self.enabled {
            return Ok(None);
        }

        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst) + 1;
        let entry = SpoolEntry {
            seq,
            peer_addr: peer_addr.map(ToOwned::to_owned),
            received_at,
            line: line.to_string(),
        };
        let json = serde_json::to_string(&entry)?;

        let mut file = self
            .file
            .lock()
            .expect("durability spool file lock poisoned");
        file.write_all(json.as_bytes())?;
        file.write_all(b"\n")?;
        file.flush()?;

        let count = self.append_count.fetch_add(1, Ordering::Relaxed) + 1;
        if count.is_multiple_of(self.fsync_every) {
            file.sync_data()?;
        }

        Ok(Some(seq))
    }

    pub fn replay_uncommitted(&self) -> Result<Vec<RawRecord>> {
        if !self.enabled {
            return Ok(Vec::new());
        }

        let committed = self.committed_seq.load(Ordering::Relaxed);
        let file = match File::open(&self.log_path) {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => {
                return Err(err).with_context(|| {
                    format!(
                        "failed to open spool log for replay {}",
                        self.log_path.display()
                    )
                });
            }
        };

        let mut out = Vec::new();
        let reader = BufReader::new(file);
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let entry: SpoolEntry = serde_json::from_str(&line)
                .with_context(|| "failed parsing spool entry during replay".to_string())?;
            if entry.seq > committed {
                out.push(RawRecord {
                    line: entry.line,
                    peer_addr: entry.peer_addr,
                    received_at: entry.received_at,
                    spool_seq: Some(entry.seq),
                });
            }
        }
        Ok(out)
    }

    pub fn commit(&self, upto_seq: u64) -> Result<()> {
        if !self.enabled || upto_seq == 0 {
            return Ok(());
        }

        let mut current = self.committed_seq.load(Ordering::Relaxed);
        while upto_seq > current {
            match self.committed_seq.compare_exchange(
                current,
                upto_seq,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    write_checkpoint(&self.checkpoint_path, upto_seq)?;
                    self.maybe_compact(upto_seq)?;
                    return Ok(());
                }
                Err(observed) => current = observed,
            }
        }
        Ok(())
    }

    fn maybe_compact(&self, committed: u64) -> Result<()> {
        let size = std::fs::metadata(&self.log_path)
            .map(|m| m.len())
            .unwrap_or(0);
        if size < self.max_log_bytes {
            return Ok(());
        }

        let mut guard = self
            .file
            .lock()
            .expect("durability spool file lock poisoned");
        guard.flush()?;
        guard.sync_data()?;

        let input = File::open(&self.log_path)
            .with_context(|| format!("failed opening spool log {}", self.log_path.display()))?;
        let mut reader = BufReader::new(input);
        let tmp_path = self.log_path.with_extension("compact");
        let mut tmp = File::create(&tmp_path)
            .with_context(|| format!("failed creating compacted spool {}", tmp_path.display()))?;

        let mut line = String::new();
        loop {
            line.clear();
            let read = reader.read_line(&mut line)?;
            if read == 0 {
                break;
            }
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let entry: SpoolEntry = serde_json::from_str(trimmed)
                .with_context(|| "failed parsing spool entry during compaction".to_string())?;
            if entry.seq > committed {
                tmp.write_all(trimmed.as_bytes())?;
                tmp.write_all(b"\n")?;
            }
        }
        tmp.flush()?;
        tmp.sync_data()?;
        drop(tmp);

        std::fs::rename(&tmp_path, &self.log_path).with_context(|| {
            format!(
                "failed replacing spool log {} -> {}",
                tmp_path.display(),
                self.log_path.display()
            )
        })?;

        let reopened = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)
            .with_context(|| format!("failed reopening spool log {}", self.log_path.display()))?;
        *guard = reopened;
        Ok(())
    }
}

fn read_checkpoint(path: &Path) -> Result<u64> {
    match std::fs::read_to_string(path) {
        Ok(value) => {
            let value = value.trim();
            if value.is_empty() {
                Ok(0)
            } else {
                value.parse::<u64>().with_context(|| {
                    format!("failed parsing durability checkpoint '{}'", path.display())
                })
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(err) => Err(err).with_context(|| format!("failed reading {}", path.display())),
    }
}

fn write_checkpoint(path: &Path, seq: u64) -> Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, seq.to_string())
        .with_context(|| format!("failed writing checkpoint tmp {}", tmp.display()))?;
    std::fs::rename(&tmp, path).with_context(|| {
        format!(
            "failed atomically replacing checkpoint {} -> {}",
            tmp.display(),
            path.display()
        )
    })?;
    Ok(())
}

fn scan_last_seq(path: &Path) -> Result<u64> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(err) => {
            return Err(err)
                .with_context(|| format!("failed opening spool log {}", path.display()));
        }
    };
    let reader = BufReader::new(file);
    let mut max_seq = 0_u64;
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let entry: SpoolEntry = serde_json::from_str(&line)
            .with_context(|| "failed parsing spool entry while scanning".to_string())?;
        max_seq = max_seq.max(entry.seq);
    }
    Ok(max_seq)
}
