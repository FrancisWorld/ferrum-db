//! One shard of the keyspace: entries, the TTL index and WATCH bookkeeping.
//!
//! A shard is always accessed under its mutex. All time-dependent behaviour takes
//! `now` explicitly so the same code serves the real clock, a manual test clock and
//! AOF replay (where `now == 0` disables expiry).

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use ahash::RandomState;
use bytes::Bytes;
use indexmap::IndexMap;

use super::value::{ENTRY_OVERHEAD, Value};

/// Process-wide counters shared by all shards.
#[derive(Debug, Default)]
pub struct Counters {
    /// Approximate bytes used by all keys and values.
    pub used_memory: AtomicUsize,
    /// Number of live keys (including logically expired ones not yet reclaimed).
    pub keys: AtomicUsize,
    /// Number of keys carrying a TTL.
    pub volatile_keys: AtomicUsize,
    /// Keys removed because their TTL elapsed.
    pub expired_keys: AtomicU64,
    /// Keys removed to honour `maxmemory`.
    pub evicted_keys: AtomicU64,
}

impl Counters {
    fn adjust_memory(&self, delta: isize) {
        if delta >= 0 {
            self.used_memory
                .fetch_add(delta as usize, Ordering::Relaxed);
        } else {
            self.used_memory
                .fetch_sub(delta.unsigned_abs(), Ordering::Relaxed);
        }
    }
}

/// A stored key's payload and metadata.
#[derive(Debug, Clone)]
pub struct Entry {
    /// The value.
    pub value: Value,
    /// Absolute expiry in milliseconds since the epoch.
    pub expire_at: Option<u64>,
    /// Last access time in milliseconds, for approximated LRU.
    pub last_access: u64,
    mem: usize,
}

#[derive(Debug, Default)]
struct WatchSlot {
    refs: usize,
    version: u64,
}

/// A slice of the keyspace guarded by one mutex.
#[derive(Debug)]
pub struct Shard {
    map: IndexMap<Bytes, Entry, RandomState>,
    expires: BTreeSet<(u64, Bytes)>,
    watched: HashMap<Bytes, WatchSlot, RandomState>,
    /// Keys mutated since the last [`Shard::settle`].
    pending: Vec<Bytes>,
    /// Keys deleted by expiry or eviction that must be propagated as `DEL`.
    removed_log: Vec<Bytes>,
    counters: Arc<Counters>,
}

impl Shard {
    /// Creates an empty shard reporting into `counters`.
    pub fn new(counters: Arc<Counters>) -> Self {
        Shard {
            map: IndexMap::default(),
            expires: BTreeSet::new(),
            watched: HashMap::default(),
            pending: Vec::new(),
            removed_log: Vec::new(),
            counters,
        }
    }

    /// Number of entries (expired-but-unreclaimed ones included).
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// True when the shard holds no entries.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Shared read access with lazy expiry. Refreshes the LRU clock.
    pub fn get(&mut self, key: &[u8], now: u64) -> Option<&Entry> {
        if self.expire_if_due(key, now) {
            return None;
        }
        let entry = self.map.get_mut(key)?;
        entry.last_access = now;
        Some(entry)
    }

    /// Mutable access with lazy expiry. The key is marked dirty: watchers are
    /// invalidated and its memory is re-measured by [`Shard::settle`].
    pub fn get_mut(&mut self, key: &[u8], now: u64) -> Option<&mut Entry> {
        if self.expire_if_due(key, now) {
            return None;
        }
        let idx = self.map.get_index_of(key)?;
        let k = self.map.get_index(idx)?.0.clone();
        self.touch(&k);
        self.pending.push(k);
        let entry = self.map.get_index_mut(idx)?.1;
        entry.last_access = now;
        Some(entry)
    }

    /// Inserts or replaces `key`, replacing any previous TTL with `expire_at`.
    pub fn insert(&mut self, key: Bytes, value: Value, expire_at: Option<u64>, now: u64) {
        let mem = ENTRY_OVERHEAD + key.len() + value.mem_size();
        self.touch(&key);
        let entry = Entry {
            value,
            expire_at,
            last_access: now,
            mem,
        };
        match self.map.insert(key.clone(), entry) {
            Some(old) => {
                if let Some(at) = old.expire_at {
                    self.expires.remove(&(at, key.clone()));
                    self.counters.volatile_keys.fetch_sub(1, Ordering::Relaxed);
                }
                self.counters.adjust_memory(mem as isize - old.mem as isize);
            }
            None => {
                self.counters.keys.fetch_add(1, Ordering::Relaxed);
                self.counters.adjust_memory(mem as isize);
            }
        }
        if let Some(at) = expire_at {
            self.expires.insert((at, key));
            self.counters.volatile_keys.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Removes `key` if it is live. Returns the removed entry.
    pub fn remove(&mut self, key: &[u8], now: u64) -> Option<Entry> {
        if self.expire_if_due(key, now) {
            return None;
        }
        self.remove_raw(key)
    }

    /// Sets or clears the TTL of a live key; returns false when it does not exist.
    pub fn set_expire(&mut self, key: &[u8], at: Option<u64>, now: u64) -> bool {
        if self.expire_if_due(key, now) {
            return false;
        }
        let Some(idx) = self.map.get_index_of(key) else {
            return false;
        };
        let Some((k, entry)) = self.map.get_index_mut(idx) else {
            return false;
        };
        let k = k.clone();
        if let Some(old) = entry.expire_at {
            self.expires.remove(&(old, k.clone()));
            self.counters.volatile_keys.fetch_sub(1, Ordering::Relaxed);
        }
        entry.expire_at = at;
        if let Some(at) = at {
            self.expires.insert((at, k.clone()));
            self.counters.volatile_keys.fetch_add(1, Ordering::Relaxed);
        }
        self.touch(&k);
        true
    }

    /// Re-measures dirty keys and deletes aggregates that became empty.
    pub fn settle(&mut self) {
        for key in std::mem::take(&mut self.pending) {
            let Some(entry) = self.map.get_mut(&key) else {
                continue;
            };
            if entry.value.is_empty_aggregate() {
                self.remove_raw(&key);
                continue;
            }
            let mem = ENTRY_OVERHEAD + key.len() + entry.value.mem_size();
            self.counters
                .adjust_memory(mem as isize - entry.mem as isize);
            entry.mem = mem;
        }
    }

    /// Takes the keys that were deleted by expiry or eviction since the last call.
    pub fn take_removed(&mut self) -> Vec<Bytes> {
        std::mem::take(&mut self.removed_log)
    }

    /// Reclaims up to `budget` keys whose TTL elapsed. Returns the number reclaimed.
    pub fn expire_due(&mut self, now: u64, budget: usize) -> usize {
        let mut reclaimed = 0;
        while reclaimed < budget {
            let Some((at, key)) = self.expires.first().cloned() else {
                break;
            };
            if at > now {
                break;
            }
            self.remove_raw(&key);
            self.removed_log.push(key);
            self.counters.expired_keys.fetch_add(1, Ordering::Relaxed);
            reclaimed += 1;
        }
        reclaimed
    }

    /// Removes an arbitrary key for eviction and records it for propagation.
    pub fn evict(&mut self, key: &[u8]) -> bool {
        match self.remove_raw(key) {
            Some(entry) => {
                self.removed_log.push(Bytes::copy_from_slice(key));
                self.counters.evicted_keys.fetch_add(1, Ordering::Relaxed);
                drop(entry);
                true
            }
            None => false,
        }
    }

    /// Picks a random entry for eviction sampling: `(key, last_access, has_ttl)`.
    pub fn sample(&self) -> Option<(&Bytes, u64, bool)> {
        if self.map.is_empty() {
            return None;
        }
        let (k, e) = self.map.get_index(fastrand::usize(..self.map.len()))?;
        Some((k, e.last_access, e.expire_at.is_some()))
    }

    /// Reads entry `idx` of the internal order without side effects (for `SCAN`).
    pub fn entry_at(&self, idx: usize) -> Option<(&Bytes, &Entry)> {
        self.map.get_index(idx)
    }

    /// Iterates all entries without expiring anything (for snapshots).
    pub fn iter(&self) -> impl Iterator<Item = (&Bytes, &Entry)> {
        self.map.iter()
    }

    /// Deletes everything; returns how many keys were dropped.
    pub fn clear(&mut self) -> usize {
        let n = self.map.len();
        let mem: usize = self.map.values().map(|e| e.mem).sum();
        self.counters.adjust_memory(-(mem as isize));
        self.counters.keys.fetch_sub(n, Ordering::Relaxed);
        self.counters
            .volatile_keys
            .fetch_sub(self.expires.len(), Ordering::Relaxed);
        self.map.clear();
        self.expires.clear();
        self.pending.clear();
        for slot in self.watched.values_mut() {
            slot.version += 1;
        }
        n
    }

    /// Registers interest in `key` and returns its current version.
    pub fn watch(&mut self, key: &Bytes) -> u64 {
        let slot = self.watched.entry(key.clone()).or_default();
        slot.refs += 1;
        slot.version
    }

    /// Drops one registration made by [`Shard::watch`].
    pub fn unwatch(&mut self, key: &[u8]) {
        if let Some(slot) = self.watched.get_mut(key) {
            slot.refs -= 1;
            if slot.refs == 0 {
                self.watched.remove(key);
            }
        }
    }

    /// Current version of a watched key (0 if nobody watches it).
    pub fn version(&self, key: &[u8]) -> u64 {
        self.watched.get(key).map_or(0, |s| s.version)
    }

    fn touch(&mut self, key: &[u8]) {
        if let Some(slot) = self.watched.get_mut(key) {
            slot.version += 1;
        }
    }

    fn expire_if_due(&mut self, key: &[u8], now: u64) -> bool {
        let due = self
            .map
            .get(key)
            .and_then(|e| e.expire_at)
            .is_some_and(|at| at <= now);
        if due {
            self.remove_raw(key);
            self.removed_log.push(Bytes::copy_from_slice(key));
            self.counters.expired_keys.fetch_add(1, Ordering::Relaxed);
        }
        due
    }

    fn remove_raw(&mut self, key: &[u8]) -> Option<Entry> {
        let (k, entry) = self.map.swap_remove_entry(key)?;
        if let Some(at) = entry.expire_at {
            self.expires.remove(&(at, k.clone()));
            self.counters.volatile_keys.fetch_sub(1, Ordering::Relaxed);
        }
        self.touch(&k);
        self.counters.keys.fetch_sub(1, Ordering::Relaxed);
        self.counters.adjust_memory(-(entry.mem as isize));
        Some(entry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    fn shard() -> (Shard, Arc<Counters>) {
        let c = Arc::new(Counters::default());
        (Shard::new(c.clone()), c)
    }

    #[test]
    fn lazy_expiry_removes_and_logs() {
        let (mut s, c) = shard();
        s.insert(b("k"), Value::Str(b("v")), Some(100), 0);
        assert!(s.get(b"k", 99).is_some());
        assert!(s.get(b"k", 100).is_none());
        assert_eq!(s.take_removed(), vec![b("k")]);
        assert_eq!(c.keys.load(Ordering::Relaxed), 0);
        assert_eq!(c.volatile_keys.load(Ordering::Relaxed), 0);
        assert_eq!(c.used_memory.load(Ordering::Relaxed), 0);
        assert_eq!(c.expired_keys.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn active_expiry_honours_budget_and_order() {
        let (mut s, _) = shard();
        for i in 0..10u64 {
            s.insert(b(&format!("k{i}")), Value::Str(b("v")), Some(10 + i), 0);
        }
        s.insert(b("forever"), Value::Str(b("v")), None, 0);
        assert_eq!(s.expire_due(5, 100), 0);
        assert_eq!(s.expire_due(14, 3), 3);
        assert_eq!(s.expire_due(14, 100), 2);
        assert_eq!(s.len(), 6);
        assert_eq!(s.take_removed().len(), 5);
    }

    #[test]
    fn replacing_a_key_clears_its_old_ttl() {
        let (mut s, c) = shard();
        s.insert(b("k"), Value::Str(b("v")), Some(10), 0);
        s.insert(b("k"), Value::Str(b("w")), None, 0);
        assert_eq!(s.expire_due(1000, 10), 0);
        assert_eq!(c.volatile_keys.load(Ordering::Relaxed), 0);
        assert!(s.set_expire(b"k", Some(50), 0));
        assert!(s.set_expire(b"k", None, 0));
        assert_eq!(s.expire_due(1000, 10), 0);
    }

    #[test]
    fn watch_versions_bump_on_every_mutation_path() {
        let (mut s, _) = shard();
        let v0 = s.watch(&b("k"));
        s.insert(b("k"), Value::Str(b("v")), None, 0);
        let v1 = s.version(b"k");
        assert!(v1 > v0);
        s.get_mut(b"k", 0);
        let v2 = s.version(b"k");
        assert!(v2 > v1);
        s.set_expire(b"k", Some(5), 0);
        let v3 = s.version(b"k");
        assert!(v3 > v2);
        s.expire_due(10, 10);
        assert!(s.version(b"k") > v3);
        s.unwatch(b"k");
        assert_eq!(s.version(b"k"), 0);
    }

    #[test]
    fn settle_remeasures_memory_and_drops_empty_aggregates() {
        let (mut s, c) = shard();
        s.insert(b("l"), Value::List(Default::default()), None, 0);
        let base = c.used_memory.load(Ordering::Relaxed);
        if let Some(Value::List(l)) = s.get_mut(b"l", 0).map(|e| &mut e.value) {
            l.push_back(b("0123456789"));
        }
        s.settle();
        assert!(c.used_memory.load(Ordering::Relaxed) > base);
        if let Some(Value::List(l)) = s.get_mut(b"l", 0).map(|e| &mut e.value) {
            l.pop_front();
        }
        s.settle();
        assert!(s.get(b"l", 0).is_none(), "empty list must be deleted");
        assert_eq!(c.used_memory.load(Ordering::Relaxed), 0);
        assert_eq!(c.keys.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn clear_resets_counters_and_invalidates_watchers() {
        let (mut s, c) = shard();
        s.insert(b("a"), Value::Str(b("1")), Some(9), 0);
        let before = s.watch(&b("a"));
        assert_eq!(s.clear(), 1);
        assert!(s.version(b"a") > before);
        assert_eq!(c.used_memory.load(Ordering::Relaxed), 0);
        assert_eq!(c.volatile_keys.load(Ordering::Relaxed), 0);
    }
}
