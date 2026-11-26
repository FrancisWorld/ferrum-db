//! Value types stored in the keyspace, each tracking its own approximate heap size
//! so memory accounting stays O(1) per mutation.

use std::collections::VecDeque;

use ahash::RandomState;
use bytes::Bytes;
use indexmap::{IndexMap, IndexSet};

use super::zset::ZSet;

/// Fixed per-key bookkeeping estimate (hash slot, entry header, expiry index).
pub const ENTRY_OVERHEAD: usize = 96;
/// Fixed per-element estimate for collection members.
pub const ELEM_OVERHEAD: usize = 40;

/// A stored value.
#[derive(Debug, Clone)]
pub enum Value {
    /// Binary-safe string.
    Str(Bytes),
    /// Doubly ended list.
    List(ListVal),
    /// Field-to-value map.
    Hash(HashVal),
    /// Unordered unique members.
    Set(SetVal),
    /// Members ordered by score.
    ZSet(ZSet),
}

impl Value {
    /// Name reported by the `TYPE` command.
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Str(_) => "string",
            Value::List(_) => "list",
            Value::Hash(_) => "hash",
            Value::Set(_) => "set",
            Value::ZSet(_) => "zset",
        }
    }

    /// Approximate heap footprint in bytes.
    pub fn mem_size(&self) -> usize {
        match self {
            Value::Str(b) => b.len(),
            Value::List(l) => l.bytes,
            Value::Hash(h) => h.bytes,
            Value::Set(s) => s.bytes,
            Value::ZSet(z) => z.mem_size(),
        }
    }

    /// Aggregates are deleted when they become empty; strings never are.
    pub fn is_empty_aggregate(&self) -> bool {
        match self {
            Value::Str(_) => false,
            Value::List(l) => l.items.is_empty(),
            Value::Hash(h) => h.map.is_empty(),
            Value::Set(s) => s.set.is_empty(),
            Value::ZSet(z) => z.len() == 0,
        }
    }
}

/// List with size tracking.
#[derive(Debug, Clone, Default)]
pub struct ListVal {
    items: VecDeque<Bytes>,
    bytes: usize,
}

impl ListVal {
    /// Number of elements.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// True when the list has no elements.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Read-only view of the elements.
    pub fn items(&self) -> &VecDeque<Bytes> {
        &self.items
    }

    /// Prepends an element.
    pub fn push_front(&mut self, b: Bytes) {
        self.bytes += b.len() + ELEM_OVERHEAD;
        self.items.push_front(b);
    }

    /// Appends an element.
    pub fn push_back(&mut self, b: Bytes) {
        self.bytes += b.len() + ELEM_OVERHEAD;
        self.items.push_back(b);
    }

    /// Removes and returns the first element.
    pub fn pop_front(&mut self) -> Option<Bytes> {
        let b = self.items.pop_front()?;
        self.bytes -= b.len() + ELEM_OVERHEAD;
        Some(b)
    }

    /// Removes and returns the last element.
    pub fn pop_back(&mut self) -> Option<Bytes> {
        let b = self.items.pop_back()?;
        self.bytes -= b.len() + ELEM_OVERHEAD;
        Some(b)
    }

    /// Replaces the element at `idx`; returns false when out of range.
    pub fn set(&mut self, idx: usize, b: Bytes) -> bool {
        let Some(slot) = self.items.get_mut(idx) else {
            return false;
        };
        self.bytes = self.bytes - slot.len() + b.len();
        *slot = b;
        true
    }

    /// Keeps only the inclusive index range `start..=stop` (already clamped).
    pub fn retain_range(&mut self, start: usize, stop: usize) {
        let keep = stop.saturating_sub(start) + 1;
        self.items.drain(..start.min(self.items.len()));
        self.items.truncate(keep);
        self.bytes = self.items.iter().map(|b| b.len() + ELEM_OVERHEAD).sum();
    }

    /// Removes up to `count` occurrences of `needle` (`count == 0` removes all,
    /// negative counts scan from the tail). Returns how many were removed.
    pub fn remove_matching(&mut self, count: i64, needle: &[u8]) -> usize {
        let limit = if count == 0 {
            usize::MAX
        } else {
            count.unsigned_abs() as usize
        };
        let mut removed = 0;
        if count >= 0 {
            let mut kept = VecDeque::with_capacity(self.items.len());
            for item in self.items.drain(..) {
                if removed < limit && item.as_ref() == needle {
                    removed += 1;
                } else {
                    kept.push_back(item);
                }
            }
            self.items = kept;
        } else {
            let mut kept = VecDeque::with_capacity(self.items.len());
            while let Some(item) = self.items.pop_back() {
                if removed < limit && item.as_ref() == needle {
                    removed += 1;
                } else {
                    kept.push_front(item);
                }
            }
            self.items = kept;
        }
        self.bytes = self.items.iter().map(|b| b.len() + ELEM_OVERHEAD).sum();
        removed
    }
}

/// Hash with size tracking. Insertion-ordered so iteration is deterministic and
/// random sampling is O(1).
#[derive(Debug, Clone, Default)]
pub struct HashVal {
    map: IndexMap<Bytes, Bytes, RandomState>,
    bytes: usize,
}

impl HashVal {
    /// Number of fields.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// True when there are no fields.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Looks up a field.
    pub fn get(&self, field: &[u8]) -> Option<&Bytes> {
        self.map.get(field)
    }

    /// Iterates fields in insertion order.
    pub fn iter(&self) -> impl Iterator<Item = (&Bytes, &Bytes)> {
        self.map.iter()
    }

    /// Sets a field; returns true when the field is new.
    pub fn insert(&mut self, field: Bytes, value: Bytes) -> bool {
        let (flen, vlen) = (field.len(), value.len());
        match self.map.insert(field, value) {
            Some(old) => {
                self.bytes = self.bytes - old.len() + vlen;
                false
            }
            None => {
                self.bytes += flen + vlen + ELEM_OVERHEAD;
                true
            }
        }
    }

    /// Removes a field; returns true when it existed.
    pub fn remove(&mut self, field: &[u8]) -> bool {
        match self.map.swap_remove_entry(field) {
            Some((k, v)) => {
                self.bytes -= k.len() + v.len() + ELEM_OVERHEAD;
                true
            }
            None => false,
        }
    }
}

/// Set with size tracking.
#[derive(Debug, Clone, Default)]
pub struct SetVal {
    set: IndexSet<Bytes, RandomState>,
    bytes: usize,
}

impl SetVal {
    /// Number of members.
    pub fn len(&self) -> usize {
        self.set.len()
    }

    /// True when there are no members.
    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    /// Membership test.
    pub fn contains(&self, m: &[u8]) -> bool {
        self.set.contains(m)
    }

    /// Iterates members in insertion order.
    pub fn iter(&self) -> impl Iterator<Item = &Bytes> {
        self.set.iter()
    }

    /// Member at position `idx` of the internal order (used for random picks).
    pub fn get_index(&self, idx: usize) -> Option<&Bytes> {
        self.set.get_index(idx)
    }

    /// Adds a member; returns true when it is new.
    pub fn insert(&mut self, m: Bytes) -> bool {
        let len = m.len();
        let added = self.set.insert(m);
        if added {
            self.bytes += len + ELEM_OVERHEAD;
        }
        added
    }

    /// Removes a member; returns true when it existed.
    pub fn remove(&mut self, m: &[u8]) -> bool {
        match self.set.swap_take(m) {
            Some(old) => {
                self.bytes -= old.len() + ELEM_OVERHEAD;
                true
            }
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[test]
    fn list_size_tracking_matches_contents() {
        let mut l = ListVal::default();
        for s in ["a", "bb", "ccc", "bb", "bb"] {
            l.push_back(b(s));
        }
        l.push_front(b("zz"));
        l.set(1, b("long-value"));
        let expect = |l: &ListVal| {
            l.items()
                .iter()
                .map(|x| x.len() + ELEM_OVERHEAD)
                .sum::<usize>()
        };
        assert_eq!(l.bytes, expect(&l));
        assert_eq!(l.remove_matching(-2, b"bb"), 2);
        assert_eq!(l.bytes, expect(&l));
        l.retain_range(1, 2);
        assert_eq!(l.len(), 2);
        assert_eq!(l.bytes, expect(&l));
        l.pop_front();
        l.pop_back();
        assert_eq!(l.bytes, 0);
    }

    #[test]
    fn list_remove_directions() {
        let mut l = ListVal::default();
        for s in ["x", "a", "x", "b", "x"] {
            l.push_back(b(s));
        }
        assert_eq!(l.remove_matching(1, b"x"), 1);
        assert_eq!(l.items().front().unwrap().as_ref(), b"a");
        assert_eq!(l.remove_matching(-1, b"x"), 1);
        assert_eq!(l.items().back().unwrap().as_ref(), b"b");
        assert_eq!(l.remove_matching(0, b"x"), 1);
    }

    #[test]
    fn hash_and_set_tracking() {
        let mut h = HashVal::default();
        assert!(h.insert(b("f"), b("v")));
        assert!(!h.insert(b("f"), b("vvvv")));
        assert_eq!(h.bytes, 1 + 4 + ELEM_OVERHEAD);
        assert!(h.remove(b"f"));
        assert!(!h.remove(b"f"));
        assert_eq!(h.bytes, 0);

        let mut s = SetVal::default();
        assert!(s.insert(b("m")));
        assert!(!s.insert(b("m")));
        assert!(s.remove(b"m"));
        assert_eq!(s.bytes, 0);
    }
}
