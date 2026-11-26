//! The sharded in-memory keyspace.
//!
//! Keys are hashed onto a fixed number of shards, each guarded by its own mutex.
//! Commands lock only the shards of the keys they touch, always in ascending shard
//! order, which rules out lock-order deadlocks. `EXEC` locks every shard, which is
//! what makes a transaction atomic with respect to all other clients.

mod shard;
mod value;
mod zset;

use std::sync::Arc;
use std::sync::atomic::Ordering;

use ahash::RandomState;
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard};

use crate::clock::Clock;

pub use shard::{Counters, Entry, Shard};
pub use value::{ELEM_OVERHEAD, ENTRY_OVERHEAD, HashVal, ListVal, SetVal, Value};
pub use zset::{Inserted, ScoreRange, ZSet};

/// Which keys `maxmemory` eviction may remove.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionPolicy {
    /// Never evict; writes fail with `OOM` once the limit is reached.
    NoEviction,
    /// Evict the approximately least recently used key among all keys.
    AllKeysLru,
    /// Evict the approximately least recently used key among keys with a TTL.
    VolatileLru,
}

impl EvictionPolicy {
    /// Parses the `maxmemory-policy` spelling.
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "noeviction" => Some(Self::NoEviction),
            "allkeys-lru" => Some(Self::AllKeysLru),
            "volatile-lru" => Some(Self::VolatileLru),
            _ => None,
        }
    }

    /// The `maxmemory-policy` spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoEviction => "noeviction",
            Self::AllKeysLru => "allkeys-lru",
            Self::VolatileLru => "volatile-lru",
        }
    }
}

/// Number of random keys examined per eviction round, as in Redis' default.
const LRU_SAMPLES: usize = 5;

/// A point-in-time copy of one key, used by snapshots and AOF rewrite.
#[derive(Debug, Clone)]
pub struct DumpEntry {
    /// The key.
    pub key: Bytes,
    /// A copy of the value (cheap: payloads are reference-counted).
    pub value: Value,
    /// Absolute expiry in ms.
    pub expire_at: Option<u64>,
}

/// The whole keyspace.
pub struct Db {
    shards: Box<[Mutex<Shard>]>,
    hasher: RandomState,
    counters: Arc<Counters>,
    clock: Arc<dyn Clock>,
}

impl Db {
    /// Creates a keyspace with `shard_count` shards (at least 1).
    pub fn new(shard_count: usize, clock: Arc<dyn Clock>) -> Self {
        let counters = Arc::new(Counters::default());
        let shards = (0..shard_count.max(1))
            .map(|_| Mutex::new(Shard::new(counters.clone())))
            .collect();
        Db {
            shards,
            hasher: RandomState::new(),
            counters,
            clock,
        }
    }

    /// Shared counters (memory, key counts, expiry and eviction totals).
    pub fn counters(&self) -> &Counters {
        &self.counters
    }

    /// Current time according to the configured clock.
    pub fn now_ms(&self) -> u64 {
        self.clock.now_ms()
    }

    /// Number of shards.
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Shard that owns `key`.
    pub fn shard_index(&self, key: &[u8]) -> usize {
        (self.hasher.hash_one(key) % self.shards.len() as u64) as usize
    }

    /// Locks the shards owning `keys` (deduplicated, ascending order).
    pub fn lock_keys<'a, 'k>(&'a self, keys: impl IntoIterator<Item = &'k [u8]>) -> Locked<'a> {
        let mut idx: Vec<usize> = keys.into_iter().map(|k| self.shard_index(k)).collect();
        idx.sort_unstable();
        idx.dedup();
        self.lock_indices(idx)
    }

    /// Locks one shard by index.
    pub fn lock_shard(&self, idx: usize) -> Locked<'_> {
        self.lock_indices(vec![idx % self.shards.len()])
    }

    /// Locks every shard.
    pub fn lock_all(&self) -> Locked<'_> {
        self.lock_indices((0..self.shards.len()).collect())
    }

    /// Locks nothing; for commands that only need connection-level state.
    pub fn lock_none(&self) -> Locked<'_> {
        Locked {
            db: self,
            guards: Vec::new(),
        }
    }

    fn lock_indices(&self, idx: Vec<usize>) -> Locked<'_> {
        let guards = idx
            .into_iter()
            .map(|i| (i, self.shards[i].lock()))
            .collect();
        Locked { db: self, guards }
    }

    /// Approximate bytes currently in use.
    pub fn used_memory(&self) -> usize {
        self.counters.used_memory.load(Ordering::Relaxed)
    }

    /// Number of keys.
    pub fn key_count(&self) -> usize {
        self.counters.keys.load(Ordering::Relaxed)
    }

    /// Reclaims elapsed TTLs, at most `budget` keys per shard, calling `on_removed`
    /// for every deleted key while its shard is still locked (so the caller can
    /// append to the AOF in order). Returns the number of reclaimed keys.
    pub fn expire_cycle(&self, budget: usize, mut on_removed: impl FnMut(&Bytes)) -> usize {
        let now = self.now_ms();
        let mut total = 0;
        for shard in self.shards.iter() {
            let mut guard = shard.lock();
            total += guard.expire_due(now, budget);
            for key in guard.take_removed() {
                on_removed(&key);
            }
        }
        total
    }

    /// Evicts one approximately-LRU key. Returns false when nothing was evictable.
    pub fn evict_one(&self, policy: EvictionPolicy, mut on_removed: impl FnMut(&Bytes)) -> bool {
        if policy == EvictionPolicy::NoEviction {
            return false;
        }
        let volatile_only = policy == EvictionPolicy::VolatileLru;
        let mut best: Option<(usize, Bytes, u64)> = None;
        let mut found = 0;
        for _ in 0..LRU_SAMPLES * 4 {
            if found >= LRU_SAMPLES {
                break;
            }
            let idx = fastrand::usize(..self.shards.len());
            let guard = self.shards[idx].lock();
            let Some((key, last, volatile)) = guard.sample() else {
                continue;
            };
            if volatile_only && !volatile {
                continue;
            }
            found += 1;
            if best.as_ref().is_none_or(|(_, _, oldest)| last < *oldest) {
                best = Some((idx, key.clone(), last));
            }
        }
        let Some((idx, key, _)) = best else {
            return self.evict_fallback(volatile_only, &mut on_removed);
        };
        let mut guard = self.shards[idx].lock();
        let evicted = guard.evict(&key);
        for k in guard.take_removed() {
            on_removed(&k);
        }
        evicted || self.evict_fallback(volatile_only, &mut on_removed)
    }

    /// Sampling can miss when the keyspace is tiny or mostly empty shards; fall back
    /// to a deterministic scan so a small dataset can still be evicted.
    fn evict_fallback(&self, volatile_only: bool, on_removed: &mut impl FnMut(&Bytes)) -> bool {
        for shard in self.shards.iter() {
            let mut guard = shard.lock();
            let victim = guard
                .iter()
                .filter(|(_, e)| !volatile_only || e.expire_at.is_some())
                .min_by_key(|(_, e)| e.last_access)
                .map(|(k, _)| k.clone());
            if let Some(key) = victim {
                guard.evict(&key);
                for k in guard.take_removed() {
                    on_removed(&k);
                }
                return true;
            }
        }
        false
    }
}

/// A set of locked shards. Dropping it releases them.
pub struct Locked<'a> {
    db: &'a Db,
    guards: Vec<(usize, MutexGuard<'a, Shard>)>,
}

impl<'a> Locked<'a> {
    /// The owning database.
    pub fn db(&self) -> &'a Db {
        self.db
    }

    /// The locked shard that owns `key`.
    ///
    /// # Panics
    /// Panics if `key` was not part of the lock set; that would be a bug in the
    /// command table's key positions.
    pub fn shard(&mut self, key: &[u8]) -> &mut Shard {
        let idx = self.db.shard_index(key);
        self.shard_by_index(idx)
            .expect("key's shard must be locked by the command's lock plan")
    }

    /// The locked shard with index `idx`, if held.
    pub fn shard_by_index(&mut self, idx: usize) -> Option<&mut Shard> {
        let pos = self.guards.binary_search_by_key(&idx, |(i, _)| *i).ok()?;
        Some(&mut *self.guards[pos].1)
    }

    /// All locked shards in ascending order.
    pub fn shards_mut(&mut self) -> impl Iterator<Item = &mut Shard> {
        self.guards.iter_mut().map(|(_, g)| &mut **g)
    }

    /// Settles memory accounting on every held shard and drains the keys that were
    /// removed by expiry, so the caller can log them as `DEL`.
    pub fn settle(&mut self) -> Vec<Bytes> {
        let mut removed = Vec::new();
        for shard in self.shards_mut() {
            shard.settle();
            removed.extend(shard.take_removed());
        }
        removed
    }

    /// Copies every live entry (point-in-time view; requires all shards locked).
    pub fn dump(&mut self, now: u64) -> Vec<DumpEntry> {
        let mut out = Vec::new();
        for shard in self.shards_mut() {
            out.extend(
                shard
                    .iter()
                    .filter(|(_, e)| e.expire_at.is_none_or(|at| at > now))
                    .map(|(k, e)| DumpEntry {
                        key: k.clone(),
                        value: e.value.clone(),
                        expire_at: e.expire_at,
                    }),
            );
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::ManualClock;

    fn db(shards: usize) -> (Db, Arc<ManualClock>) {
        let clock = Arc::new(ManualClock::new(1_000));
        (Db::new(shards, clock.clone()), clock)
    }

    fn put(db: &Db, key: &str, val: &str, ttl: Option<u64>) {
        let now = db.now_ms();
        let mut l = db.lock_keys([key.as_bytes()]);
        l.shard(key.as_bytes()).insert(
            Bytes::copy_from_slice(key.as_bytes()),
            Value::Str(Bytes::copy_from_slice(val.as_bytes())),
            ttl,
            now,
        );
    }

    #[test]
    fn keys_spread_across_shards_and_locks_are_deduplicated() {
        let (db, _) = db(8);
        let keys: Vec<String> = (0..64).map(|i| format!("key{i}")).collect();
        let used: std::collections::HashSet<_> =
            keys.iter().map(|k| db.shard_index(k.as_bytes())).collect();
        assert!(used.len() > 4);
        let l = db.lock_keys(
            keys.iter()
                .map(|k| k.as_bytes())
                .chain(keys.iter().map(|k| k.as_bytes())),
        );
        assert_eq!(l.guards.len(), used.len());
    }

    #[test]
    fn expire_cycle_reports_removed_keys_in_lock() {
        let (db, clock) = db(4);
        for i in 0..20 {
            put(&db, &format!("k{i}"), "v", Some(1_050));
        }
        put(&db, "keep", "v", None);
        assert_eq!(db.expire_cycle(100, |_| {}), 0);
        clock.advance(100);
        let mut removed = Vec::new();
        assert_eq!(db.expire_cycle(100, |k| removed.push(k.clone())), 20);
        assert_eq!(removed.len(), 20);
        assert_eq!(db.key_count(), 1);
    }

    #[test]
    fn eviction_prefers_older_keys() {
        let (db, clock) = db(4);
        for i in 0..200 {
            clock.advance(1);
            put(&db, &format!("k{i}"), "v", None);
        }
        let mut evicted_ages = Vec::new();
        for _ in 0..20 {
            assert!(db.evict_one(EvictionPolicy::AllKeysLru, |k| {
                let n: u64 = std::str::from_utf8(&k[1..]).unwrap().parse().unwrap();
                evicted_ages.push(n);
            }));
        }
        let mean = evicted_ages.iter().sum::<u64>() as f64 / evicted_ages.len() as f64;
        assert!(
            mean < 100.0,
            "5-sample LRU should skew towards old keys, mean index {mean}"
        );
        assert_eq!(db.key_count(), 180);
        assert_eq!(db.counters().evicted_keys.load(Ordering::Relaxed), 20);
    }

    #[test]
    fn volatile_lru_only_touches_keys_with_ttl() {
        let (db, _) = db(2);
        put(&db, "persistent", "v", None);
        assert!(!db.evict_one(EvictionPolicy::VolatileLru, |_| {}));
        put(&db, "temp", "v", Some(1_000_000));
        let mut gone = Vec::new();
        assert!(db.evict_one(EvictionPolicy::VolatileLru, |k| gone.push(k.clone())));
        assert_eq!(gone, vec![Bytes::from_static(b"temp")]);
        assert!(!db.evict_one(EvictionPolicy::NoEviction, |_| {}));
        assert_eq!(db.key_count(), 1);
    }

    #[test]
    fn dump_skips_expired_entries() {
        let (db, clock) = db(2);
        put(&db, "a", "1", Some(1_010));
        put(&db, "b", "2", None);
        clock.advance(50);
        let now = db.now_ms();
        let dump = db.lock_all().dump(now);
        assert_eq!(dump.len(), 1);
        assert_eq!(dump[0].key.as_ref(), b"b");
    }

    #[test]
    fn policy_names_round_trip() {
        for p in [
            EvictionPolicy::NoEviction,
            EvictionPolicy::AllKeysLru,
            EvictionPolicy::VolatileLru,
        ] {
            assert_eq!(EvictionPolicy::parse(p.as_str()), Some(p));
        }
        assert_eq!(EvictionPolicy::parse("random"), None);
    }
}
