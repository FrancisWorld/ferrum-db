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
    /// Most bytes buffered for one connection's unparsed input.
    pub max_query_buffer: usize,
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
            max_bulk_len: 64 * 1024 * 1024,
            max_query_buffer: 128 * 1024 * 1024,
            hz: 10,
        }
    }
}

/// How exposed a configuration is to the network.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Exposure {
    /// Loopback only, or password protected.
    Safe,
    /// Reachable from the network without a password, explicitly allowed by the operator.
    OpenByRequest,
}

impl Config {
    /// Refuses to serve a non-loopback address without a password unless the operator
    /// explicitly opted in (`--unsafe-no-auth`).
    pub fn check_exposure(&self, allow_no_auth: bool) -> Result<Exposure, String> {
        let loopback = self.bind.eq_ignore_ascii_case("localhost")
            || self
                .bind
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback());
        if loopback || self.requirepass.is_some() {
            return Ok(Exposure::Safe);
        }
        if allow_no_auth {
            return Ok(Exposure::OpenByRequest);
        }
        Err(format!(
            "refusing to listen on {} without a password: anyone who can reach the port could read and \
             change all data. Set --requirepass / FERRUM_REQUIREPASS, bind to 127.0.0.1, or pass \
             --unsafe-no-auth (FERRUM_ALLOW_NO_AUTH=1) if you really want an open server",
            self.bind
        ))
    }

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
    use super::{Config, Exposure, parse_bytes};

    fn cfg(bind: &str, pass: Option<&str>) -> Config {
        Config {
            bind: bind.into(),
            requirepass: pass.map(Into::into),
            ..Config::default()
        }
    }

    #[test]
    fn exposure_rules() {
        for loopback in ["127.0.0.1", "::1", "localhost", "127.1.2.3"] {
            assert_eq!(
                cfg(loopback, None).check_exposure(false),
                Ok(Exposure::Safe),
                "{loopback}"
            );
        }
        for open in ["0.0.0.0", "::", "192.168.1.5", "example.internal", ""] {
            assert!(cfg(open, None).check_exposure(false).is_err(), "{open}");
            assert_eq!(
                cfg(open, None).check_exposure(true),
                Ok(Exposure::OpenByRequest),
                "{open}"
            );
            assert_eq!(
                cfg(open, Some("pw")).check_exposure(false),
                Ok(Exposure::Safe),
                "{open}"
            );
        }
    }

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
