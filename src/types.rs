use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct RawRecord {
    pub line: String,
    pub peer_addr: Option<String>,
    pub received_at: DateTime<Utc>,
    pub spool_seq: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct ParsedRecord {
    pub values: Vec<Option<String>>,
    pub event_time: DateTime<Utc>,
    pub spool_seq: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct DlqRecord {
    pub raw_line: String,
    pub reason: String,
    pub peer_addr: Option<String>,
    pub received_at: DateTime<Utc>,
}
