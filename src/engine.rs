//! The engine ties the keyspace, durability, pub/sub and statistics together and
//! executes commands against them.

use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;

use crate::clock::Clock;
use crate::cmd::{self, Command, Ctx, DENYOOM, LockPlan};
use crate::config::{Config, parse_bytes};
use crate::db::{Db, EvictionPolicy};
use crate::error::CmdError;
use crate::persist::PersistError;
use crate::persist::aof::{Aof, AofError, FsyncPolicy};
use crate::persist::loader::{self, LoadReport};
use crate::persist::{rewrite, snapshot};
use crate::pubsub::Hub;
use crate::resp::{Frame, Protocol};

/// Number of keys examined per shard in one active-expiry pass.
const EXPIRE_BUDGET: usize = 200;

/// Counters exposed through `INFO`.
#[derive(Debug, Default)]
pub struct Stats {
    /// Commands executed.
    pub commands: AtomicU64,
    /// Connections accepted since start.
    pub connections: AtomicU64,
    /// Connections currently open.
    pub clients: AtomicUsize,
    /// Connections refused because `max_clients` was reached.
    pub rejected_connections: AtomicU64,
}

/// The server state shared by all connections.
pub struct Engine {
    db: Db,
    hub: Hub,
    aof: OnceLock<Arc<Aof>>,
    config: Config,
    maxmemory: AtomicUsize,
    policy: AtomicU8,
    stats: Stats,
    started: Instant,
    last_save: Arc<AtomicU64>,
    saving: Arc<AtomicBool>,
    rewrite_pause_us: AtomicU64,
    next_client_id: AtomicU64,
    loaded: LoadReport,
}

/// Conservative upper bound on how much a memory-growing command can add, derived
/// from its arguments: every argument could be a key (one entry header) and costs its
/// own bytes, which also covers hash fields and sorted-set members.
///
/// It does not bound commands whose result is larger than their arguments
/// (`SUNIONSTORE` and friends), which can still overshoot `maxmemory`.
fn growth_estimate(args: &[Bytes]) -> usize {
    args[1..].iter().fold(0usize, |sum, a| {
        sum.saturating_add(a.len())
            .saturating_add(crate::db::ENTRY_OVERHEAD)
    })
}

fn policy_code(p: EvictionPolicy) -> u8 {
    match p {
        EvictionPolicy::NoEviction => 0,
        EvictionPolicy::AllKeysLru => 1,
        EvictionPolicy::VolatileLru => 2,
    }
}

fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

impl Engine {
    /// Builds an engine, recovering state from disk according to `config`.
    ///
    /// Recovery order: the AOF if enabled and present, otherwise the snapshot. When
    /// the AOF is enabled but absent while a snapshot exists, the snapshot is
    /// converted into a fresh AOF so durability starts from a complete base.
    pub fn open(config: Config, clock: Arc<dyn Clock>) -> Result<Arc<Engine>, PersistError> {
        std::fs::create_dir_all(&config.dir)?;
        let db = Db::new(config.shards, clock);
        let mut engine = Engine {
            db,
            hub: Hub::new(),
            aof: OnceLock::new(),
            maxmemory: AtomicUsize::new(config.maxmemory),
            policy: AtomicU8::new(policy_code(config.maxmemory_policy)),
            stats: Stats::default(),
            started: Instant::now(),
            last_save: Arc::new(AtomicU64::new(unix_secs())),
            saving: Default::default(),
            rewrite_pause_us: AtomicU64::new(0),
            next_client_id: AtomicU64::new(1),
            loaded: LoadReport::default(),
            config,
        };
        let (aof_path, snap_path) = (engine.config.aof_path(), engine.config.snapshot_path());
        if engine.config.aof_enabled && aof_path.exists() {
            let report = loader::load_aof(&aof_path, &engine)?;
            engine.loaded = report;
            tracing::info!(
                commands = engine.loaded.commands,
                "recovered dataset from AOF"
            );
        } else if snap_path.exists() {
            let entries = snapshot::read_snapshot(&snap_path)?;
            tracing::info!(keys = entries.len(), "recovered dataset from snapshot");
            engine.db.restore(entries);
            if engine.config.aof_enabled {
                let dump = engine.db.lock_all().dump(0);
                rewrite::write_aof_file(&aof_path, &dump)?;
            }
        }
        if engine.config.aof_enabled {
            let aof = Aof::open(&aof_path, engine.config.aof_fsync)?;
            let _ = engine.aof.set(Arc::new(aof));
        }
        Ok(Arc::new(engine))
    }

    /// The keyspace.
    pub fn db(&self) -> &Db {
        &self.db
    }

    /// Pub/sub registry.
    pub fn hub(&self) -> &Hub {
        &self.hub
    }

    /// Static configuration.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Runtime statistics.
    pub fn stats(&self) -> &Stats {
        &self.stats
    }

    /// The append-only file, when enabled.
    pub fn aof(&self) -> Option<&Aof> {
        self.aof.get().map(Arc::as_ref)
    }

    /// What recovery replayed at startup.
    pub fn load_report(&self) -> &LoadReport {
        &self.loaded
    }

    pub(crate) fn next_client_id(&self) -> u64 {
        self.next_client_id.fetch_add(1, Ordering::Relaxed)
    }

    fn policy(&self) -> EvictionPolicy {
        match self.policy.load(Ordering::Relaxed) {
            1 => EvictionPolicy::AllKeysLru,
            2 => EvictionPolicy::VolatileLru,
            _ => EvictionPolicy::NoEviction,
        }
    }

    /// Runs one pass of active expiry; returns the number of reclaimed keys.
    pub fn expire_cycle(&self) -> usize {
        self.db.expire_cycle(EXPIRE_BUDGET, |key| self.log_del(key))
    }

    /// Stops background durability work and flushes the AOF.
    pub fn shutdown(&self) {
        if let Some(aof) = self.aof.get() {
            aof.shutdown();
        }
    }

    fn log_del(&self, key: &Bytes) {
        if let Some(aof) = self.aof.get() {
            aof.append(&[vec![Bytes::from_static(b"DEL"), key.clone()]]);
        }
    }

    /// Appends effects to the AOF; returns a sequence number to wait on when the
    /// policy is `always`.
    pub(crate) fn log(&self, effects: Vec<Vec<Bytes>>) -> Option<u64> {
        if effects.is_empty() {
            return None;
        }
        let aof = self.aof.get()?;
        let seq = aof.append(&effects);
        (aof.policy() == FsyncPolicy::Always).then_some(seq)
    }

    /// Waits until `seq` is durable.
    pub(crate) async fn wait_durable(&self, seq: u64) -> Result<(), AofError> {
        match self.aof.get() {
            Some(aof) => aof.wait_synced(seq).await,
            None => Ok(()),
        }
    }

    /// Evicts until `extra` more bytes fit under `maxmemory`, or fails with `OOM` when
    /// that is impossible. A request that could never fit is refused without evicting.
    pub(crate) fn reclaim_memory(&self, extra: usize) -> Result<(), CmdError> {
        let limit = self.maxmemory.load(Ordering::Relaxed);
        if limit == 0 {
            return Ok(());
        }
        if extra > limit {
            return Err(CmdError::Oom);
        }
        let policy = self.policy();
        while self.db.used_memory().saturating_add(extra) > limit {
            if !self.db.evict_one(policy, |key| self.log_del(key)) {
                return Err(CmdError::Oom);
            }
        }
        Ok(())
    }

    fn check_writable(&self, cmd: &Command) -> Result<(), CmdError> {
        if cmd.is_write() && self.aof.get().is_some_and(|a| a.failed()) {
            return Err(CmdError::Raw(AofError::WriteFailed.to_string()));
        }
        Ok(())
    }

    fn lock_for<'a>(&'a self, cmd: &Command, args: &'a [Bytes]) -> crate::db::Locked<'a> {
        match cmd.lock {
            LockPlan::None => self.db.lock_none(),
            LockPlan::Keys { .. } => self.db.lock_keys(cmd.keys(args)),
            LockPlan::All => self.db.lock_all(),
            LockPlan::Scan => {
                let cursor = std::str::from_utf8(&args[1])
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                self.db.lock_shard(cmd::decode_cursor(cursor).0)
            }
        }
    }

    /// Executes one validated command outside a transaction.
    pub(crate) fn execute(
        &self,
        cmd: &'static Command,
        args: &[Bytes],
        proto: Protocol,
    ) -> (Frame, Option<u64>) {
        if let Err(e) = self.check_writable(cmd) {
            return (e.into(), None);
        }
        if cmd.flags & DENYOOM != 0
            && let Err(e) = self.reclaim_memory(growth_estimate(args))
        {
            return (e.into(), None);
        }
        let now = self.db.now_ms();
        let locked = self.lock_for(cmd, args);
        let mut ctx = Ctx::new(self, locked, now, proto);
        let frame = ctx.run(cmd, args);
        // Appended while the shard locks are still held so log order equals execution order.
        let seq = self.log(std::mem::take(&mut ctx.effects));
        (frame, seq)
    }

    /// Executes a queued transaction atomically. `watched` holds `(key, version)`
    /// pairs recorded by `WATCH`; a changed version aborts with a null reply.
    pub(crate) fn execute_transaction(
        &self,
        queue: &[Vec<Bytes>],
        watched: &[(Bytes, u64)],
        proto: Protocol,
    ) -> (Frame, Option<u64>) {
        let commands: Vec<&'static Command> =
            queue.iter().filter_map(|a| cmd::lookup(&a[0])).collect();
        if let Some(e) = commands.iter().find_map(|c| self.check_writable(c).err()) {
            self.unwatch(watched);
            return (e.into(), None);
        }
        let growth: usize = commands
            .iter()
            .zip(queue)
            .filter(|(c, _)| c.flags & DENYOOM != 0)
            .map(|(_, args)| growth_estimate(args))
            .fold(0, usize::saturating_add);
        if growth > 0
            && let Err(e) = self.reclaim_memory(growth)
        {
            self.unwatch(watched);
            return (e.into(), None);
        }
        let now = self.db.now_ms();
        let mut locked = self.db.lock_all();
        let conflict = watched
            .iter()
            .any(|(k, v)| locked.shard(k).version(k) != *v);
        for (k, _) in watched {
            locked.shard(k).unwatch(k);
        }
        if conflict {
            return (Frame::NullArray, None);
        }
        let mut ctx = Ctx::new(self, locked, now, proto);
        let replies: Vec<Frame> = commands
            .iter()
            .zip(queue)
            .map(|(c, args)| ctx.run(c, args))
            .collect();
        let mut effects = std::mem::take(&mut ctx.effects);
        if effects.len() > 1 {
            // Replayed as one unit: a crash mid-block drops the whole transaction.
            effects.insert(0, vec![Bytes::from_static(b"MULTI")]);
            effects.push(vec![Bytes::from_static(b"EXEC")]);
        }
        let seq = self.log(effects);
        (Frame::Array(replies), seq)
    }

    /// Registers a `WATCH` on `key` and returns its version.
    pub(crate) fn watch(&self, key: &Bytes) -> u64 {
        self.db.lock_keys([key.as_ref()]).shard(key).watch(key)
    }

    /// Drops watch registrations.
    pub(crate) fn unwatch(&self, watched: &[(Bytes, u64)]) {
        for (key, _) in watched {
            self.db.lock_keys([key.as_ref()]).shard(key).unwatch(key);
        }
    }

    /// Applies a command from the AOF: nothing expires and nothing is logged again.
    pub fn replay(&self, args: &[Bytes]) -> Result<(), CmdError> {
        let cmd = cmd::resolve(args)?;
        let locked = self.lock_for(cmd, args);
        let mut ctx = Ctx::new(self, locked, 0, Protocol::Resp2);
        match ctx.run(cmd, args) {
            Frame::Error(e) => Err(CmdError::Raw(e)),
            _ => Ok(()),
        }
    }

    // ---- persistence commands -------------------------------------------------

    fn snapshot_path(&self) -> std::path::PathBuf {
        self.config.snapshot_path()
    }

    /// `SAVE`: dumps the dataset and writes the snapshot on the calling thread.
    pub fn save(&self) -> Result<(), CmdError> {
        let claim = self.claim_save()?;
        let entries = self.db.lock_all().dump(self.db.now_ms());
        let result = snapshot::write_snapshot(&self.snapshot_path(), &entries);
        drop(claim);
        result.map_err(|e| CmdError::other(format!("snapshot failed: {e}")))?;
        self.last_save.store(unix_secs(), Ordering::Relaxed);
        Ok(())
    }

    /// `BGSAVE`: dumps under lock, then writes the snapshot on a background thread.
    pub fn bgsave(&self) -> Result<(), CmdError> {
        let claim = self.claim_save()?;
        let entries = self.db.lock_all().dump(self.db.now_ms());
        let (path, last_save) = (self.snapshot_path(), self.last_save.clone());
        std::thread::Builder::new()
            .name("ferrum-bgsave".into())
            .spawn(move || {
                match snapshot::write_snapshot(&path, &entries) {
                    Ok(bytes) => {
                        last_save.store(unix_secs(), Ordering::Relaxed);
                        tracing::info!(bytes, "background snapshot finished");
                    }
                    Err(e) => tracing::error!(error = %e, "background snapshot failed"),
                }
                drop(claim);
            })
            .map_err(|e| CmdError::other(format!("cannot spawn snapshot thread: {e}")))?;
        Ok(())
    }

    fn claim_save(&self) -> Result<SaveClaim, CmdError> {
        if self.saving.swap(true, Ordering::AcqRel) {
            return Err(CmdError::other("Background save already in progress"));
        }
        Ok(SaveClaim(self.saving.clone()))
    }

    /// `BGREWRITEAOF`: compacts the AOF on a background thread.
    pub fn bgrewriteaof(&self) -> Result<(), CmdError> {
        let aof = self
            .aof
            .get()
            .cloned()
            .ok_or_else(|| CmdError::other("AOF is not enabled"))?;
        let slot = aof
            .claim_rewrite()
            .map_err(|e| CmdError::other(e.to_string()))?;
        let started = Instant::now();
        // Dump and diff start must describe the same instant, so both happen under all locks.
        let entries = {
            let mut locked = self.db.lock_all();
            let entries = locked.dump(self.db.now_ms());
            aof.start_diff();
            entries
        };
        self.rewrite_pause_us
            .store(started.elapsed().as_micros() as u64, Ordering::Relaxed);
        std::thread::Builder::new()
            .name("ferrum-rewrite".into())
            .spawn(move || match aof.finish_rewrite(slot, &entries) {
                Ok(bytes) => tracing::info!(bytes, "AOF rewrite finished"),
                Err(e) => {
                    tracing::error!(error = %e, "AOF rewrite failed; the old file stays in use")
                }
            })
            .map_err(|e| CmdError::other(format!("cannot spawn rewrite thread: {e}")))?;
        Ok(())
    }

    /// Unix time of the last successful snapshot (or process start).
    pub fn last_save_secs(&self) -> u64 {
        self.last_save.load(Ordering::Relaxed)
    }

    // ---- configuration and introspection --------------------------------------

    /// Readable configuration parameters as `(name, value)` pairs.
    pub fn config_entries(&self) -> Vec<(String, String)> {
        let c = &self.config;
        vec![
            (
                "maxmemory".into(),
                self.maxmemory.load(Ordering::Relaxed).to_string(),
            ),
            ("maxmemory-policy".into(), self.policy().as_str().into()),
            (
                "appendonly".into(),
                if c.aof_enabled { "yes" } else { "no" }.into(),
            ),
            ("appendfsync".into(), c.aof_fsync.as_str().into()),
            ("dir".into(), c.dir.display().to_string()),
            ("port".into(), c.port.to_string()),
            ("shards".into(), c.shards.to_string()),
            ("hz".into(), c.hz.to_string()),
            ("maxclients".into(), c.max_clients.to_string()),
            (
                "pubsub-buffer-limit".into(),
                c.pubsub_buffer_limit.to_string(),
            ),
        ]
    }

    /// `CONFIG SET` for the parameters that can change at runtime.
    pub fn config_set(&self, name: &str, value: &str) -> Result<(), CmdError> {
        match name {
            "maxmemory" => {
                let bytes =
                    parse_bytes(value).ok_or_else(|| CmdError::other("invalid maxmemory value"))?;
                self.maxmemory.store(bytes, Ordering::Relaxed);
                Ok(())
            }
            "maxmemory-policy" => {
                let p = EvictionPolicy::parse(value).ok_or_else(|| {
                    CmdError::other(
                        "invalid maxmemory-policy (noeviction, allkeys-lru, volatile-lru)",
                    )
                })?;
                self.policy.store(policy_code(p), Ordering::Relaxed);
                Ok(())
            }
            other => Err(CmdError::other(format!(
                "parameter '{other}' is not settable at runtime"
            ))),
        }
    }

    /// Renders `INFO`, optionally restricted to one section.
    pub fn info(&self, section: Option<&str>) -> String {
        use std::fmt::Write as _;
        let want = |name: &str| {
            section.is_none_or(|s| s == name || s == "all" || s == "default" || s == "everything")
        };
        let mut out = String::new();
        let c = self.db.counters();
        if want("server") {
            let _ = write!(
                out,
                "# Server\r\nferrum_version:{}\r\nprocess_id:{}\r\ntcp_port:{}\r\nuptime_in_seconds:{}\r\nshards:{}\r\n\r\n",
                env!("CARGO_PKG_VERSION"),
                std::process::id(),
                self.config.port,
                self.started.elapsed().as_secs(),
                self.db.shard_count()
            );
        }
        if want("clients") {
            let _ = write!(
                out,
                "# Clients\r\nconnected_clients:{}\r\n\r\n",
                self.stats.clients.load(Ordering::Relaxed)
            );
        }
        if want("memory") {
            let _ = write!(
                out,
                "# Memory\r\nused_memory:{}\r\nmaxmemory:{}\r\nmaxmemory_policy:{}\r\n\r\n",
                self.db.used_memory(),
                self.maxmemory.load(Ordering::Relaxed),
                self.policy().as_str()
            );
        }
        if want("persistence") {
            let _ = write!(
                out,
                "# Persistence\r\naof_enabled:{}\r\n",
                u8::from(self.aof.get().is_some())
            );
            if let Some(aof) = self.aof.get() {
                let _ = write!(
                    out,
                    "aof_fsync:{}\r\naof_current_size:{}\r\naof_rewrite_in_progress:{}\r\naof_rewrites_completed:{}\r\naof_last_rewrite_pause_us:{}\r\naof_write_failed:{}\r\n",
                    aof.policy().as_str(),
                    aof.size(),
                    u8::from(aof.rewrite_in_progress()),
                    aof.rewrites_completed(),
                    self.rewrite_pause_us.load(Ordering::Relaxed),
                    u8::from(aof.failed())
                );
            }
            let _ = write!(
                out,
                "aof_loaded_commands:{}\r\naof_truncated_bytes_at_load:{}\r\nrdb_last_save_time:{}\r\nsave_in_progress:{}\r\n\r\n",
                self.loaded.commands,
                self.loaded.truncated_bytes,
                self.last_save_secs(),
                u8::from(self.saving.load(Ordering::Relaxed))
            );
        }
        if want("stats") {
            let _ = write!(
                out,
                "# Stats\r\ntotal_connections_received:{}\r\ntotal_commands_processed:{}\r\nrejected_connections:{}\r\nexpired_keys:{}\r\nevicted_keys:{}\r\npubsub_messages_published:{}\r\npubsub_slow_subscribers_disconnected:{}\r\n\r\n",
                self.stats.connections.load(Ordering::Relaxed),
                self.stats.commands.load(Ordering::Relaxed),
                self.stats.rejected_connections.load(Ordering::Relaxed),
                c.expired_keys.load(Ordering::Relaxed),
                c.evicted_keys.load(Ordering::Relaxed),
                self.hub.published(),
                self.hub.slow_disconnects()
            );
        }
        if want("keyspace") {
            let keys = self.db.key_count();
            let _ = write!(out, "# Keyspace\r\n");
            if keys > 0 {
                let _ = write!(
                    out,
                    "db0:keys={keys},expires={}\r\n",
                    c.volatile_keys.load(Ordering::Relaxed)
                );
            }
            out.push_str("\r\n");
        }
        out
    }
}

struct SaveClaim(Arc<AtomicBool>);

impl Drop for SaveClaim {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}
