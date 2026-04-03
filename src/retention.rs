use std::path::Path;
use std::time::Duration;

use anyhow::Result;
use chrono::{Days, NaiveDate, Utc};
use tokio::sync::watch;
use tracing::{info, warn};

use crate::config::AppConfig;

pub async fn run_retention_loop(
    cfg: AppConfig,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<()> {
    if !cfg.retention.enabled {
        return Ok(());
    }

    let mut interval = tokio::time::interval(Duration::from_secs(
        cfg.retention.sweep_interval_secs.max(60),
    ));

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    break;
                }
            }
            _ = interval.tick() => {
                if let Err(err) = sweep_once(&cfg.writer.output_dir, cfg.retention.local_days) {
                    warn!(error = %err, "retention sweep failed");
                }
            }
        }
    }
    Ok(())
}

fn sweep_once(output_dir: &Path, local_days: i64) -> Result<()> {
    if local_days <= 0 {
        return Ok(());
    }
    if !output_dir.exists() {
        return Ok(());
    }

    let now = Utc::now().date_naive();
    let cutoff = now
        .checked_sub_days(Days::new(local_days as u64))
        .unwrap_or(now);

    for entry in std::fs::read_dir(output_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Some(date_str) = name.strip_prefix("dt=") else {
            continue;
        };
        let Ok(date) = NaiveDate::parse_from_str(date_str, "%Y-%m-%d") else {
            continue;
        };
        if date < cutoff {
            std::fs::remove_dir_all(entry.path())?;
            info!(partition = %name, "retention removed partition");
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn removes_old_partitions() {
        let dir = tempdir().unwrap();
        let old = dir.path().join("dt=2000-01-01");
        let new = dir
            .path()
            .join(format!("dt={}", Utc::now().date_naive().format("%Y-%m-%d")));
        fs::create_dir_all(&old).unwrap();
        fs::create_dir_all(&new).unwrap();

        sweep_once(dir.path(), 14).unwrap();
        assert!(!old.exists());
        assert!(new.exists());
    }
}
