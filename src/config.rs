//! Runtime configuration.

use std::path::PathBuf;

use crate::db::EvictionPolicy;
use crate::persist::aof::FsyncPolicy;

/// Server configuration. [`Config::default`] is an ephemeral, loopback-only server.
#[derive(Debug, Clone)]
pub struct Config {
    /// Address to listen on.
    pub bind: String,
    /// TCP port (`0` picks a free one).
    pub port: u16,
    /// Number of keyspace shards.
    pub shards: usize,
    /// Directory for the AOF and the snapshot.
    pub dir: PathBuf,
    /// Enables the append-only file.
    pub aof_enabled: bool,
    /// AOF fsync policy.
    pub aof_fsync: FsyncPolicy,
    /// AOF file name inside `dir`.
    pub aof_filename: String,
    /// Snapshot file name inside `dir`.
    pub snapshot_filename: String,
    /// Memory limit in bytes (`0` = unlimited).
    pub maxmemory: usize,
    /// What to evict when `maxmemory` is reached.
    pub maxmemory_policy: EvictionPolicy,
    /// Password required by `AUTH` (none = open server).
    pub requirepass: Option<String>,
    /// Maximum simultaneous connections.
    pub max_clients: usize,
    /// Bytes a subscriber may lag behind before it is disconnected.
    pub pubsub_buffer_limit: usize,
    /// Largest accepted bulk string.
    pub max_bulk_len: usize,
    /// Active-expiry cycles per second.
    pub hz: u32,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            bind: "127.0.0.1".into(),
            port: 6380,
            shards: 16,
            dir: PathBuf::from("."),
            aof_enabled: false,
            aof_fsync: FsyncPolicy::EverySec,
            aof_filename: "appendonly.aof".into(),
            snapshot_filename: "dump.fdb".into(),
            maxmemory: 0,
            maxmemory_policy: EvictionPolicy::NoEviction,
            requirepass: None,
            max_clients: 10_000,
            pubsub_buffer_limit: 8 * 1024 * 1024,
            max_bulk_len: 512 * 1024 * 1024,
            hz: 10,
        }
    }
}

impl Config {
    /// Full path of the AOF.
    pub fn aof_path(&self) -> PathBuf {
        self.dir.join(&self.aof_filename)
    }

    /// Full path of the snapshot.
    pub fn snapshot_path(&self) -> PathBuf {
        self.dir.join(&self.snapshot_filename)
    }
}

/// Parses a byte size with an optional `k`, `kb`, `m`, `mb`, `g` or `gb` suffix.
pub fn parse_bytes(s: &str) -> Option<usize> {
    let lower = s.trim().to_ascii_lowercase();
    let (digits, mult) = [
        ("gb", 1usize << 30),
        ("mb", 1 << 20),
        ("kb", 1 << 10),
        ("g", 1 << 30),
        ("m", 1 << 20),
        ("k", 1 << 10),
    ]
    .iter()
    .find_map(|(suffix, m)| lower.strip_suffix(suffix).map(|d| (d, *m)))
    .unwrap_or((lower.as_str(), 1));
    digits.trim().parse::<usize>().ok()?.checked_mul(mult)
}

#[cfg(test)]
mod tests {
    use super::parse_bytes;

    #[test]
    fn byte_sizes() {
        assert_eq!(parse_bytes("1024"), Some(1024));
        assert_eq!(parse_bytes("64mb"), Some(64 << 20));
        assert_eq!(parse_bytes("2G"), Some(2 << 30));
        assert_eq!(parse_bytes("3k"), Some(3072));
        assert_eq!(parse_bytes("x"), None);
        assert_eq!(parse_bytes("-1"), None);
        assert_eq!(parse_bytes("99999999999999999999g"), None);
    }
}
