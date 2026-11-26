//! Sorted set: a hash map for O(1) member lookup plus an arena-backed skip list
//! with per-link spans, giving O(log n) insert, delete, rank and range-by-rank.
//!
//! Members are ordered by `(score, member bytes)`. Scores are never NaN and `-0.0`
//! is normalised to `0.0`, so plain `partial_cmp` is a total order here.

use std::cmp::Ordering;

use ahash::RandomState;
use bytes::Bytes;
use std::collections::HashMap;

use super::value::ELEM_OVERHEAD;

const MAX_LEVEL: usize = 32;
const NIL: usize = usize::MAX;
const HEADER: usize = 0;

#[derive(Debug, Clone, Copy)]
struct Link {
    forward: usize,
    span: usize,
}

#[derive(Debug, Clone)]
struct Node {
    member: Bytes,
    score: f64,
    backward: usize,
    links: Vec<Link>,
}

#[derive(Debug, Clone)]
struct SkipList {
    nodes: Vec<Node>,
    free: Vec<usize>,
    tail: usize,
    level: usize,
    len: usize,
    rng: u64,
}

/// An inclusive/exclusive score interval.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScoreRange {
    /// Lower bound.
    pub min: f64,
    /// Whether the lower bound itself is excluded.
    pub min_exclusive: bool,
    /// Upper bound.
    pub max: f64,
    /// Whether the upper bound itself is excluded.
    pub max_exclusive: bool,
}

impl ScoreRange {
    fn gte_min(&self, score: f64) -> bool {
        if self.min_exclusive {
            score > self.min
        } else {
            score >= self.min
        }
    }

    fn lte_max(&self, score: f64) -> bool {
        if self.max_exclusive {
            score < self.max
        } else {
            score <= self.max
        }
    }

    fn is_empty(&self) -> bool {
        self.min > self.max || (self.min == self.max && (self.min_exclusive || self.max_exclusive))
    }
}

fn cmp_key(a_score: f64, a_member: &[u8], b_score: f64, b_member: &[u8]) -> Ordering {
    a_score
        .partial_cmp(&b_score)
        .unwrap_or(Ordering::Equal)
        .then_with(|| a_member.cmp(b_member))
}

impl SkipList {
    fn new() -> Self {
        let header = Node {
            member: Bytes::new(),
            score: 0.0,
            backward: NIL,
            links: vec![
                Link {
                    forward: NIL,
                    span: 0
                };
                MAX_LEVEL
            ],
        };
        SkipList {
            nodes: vec![header],
            free: Vec::new(),
            tail: NIL,
            level: 1,
            len: 0,
            rng: 0x9E37_79B9_7F4A_7C15,
        }
    }

    fn random_level(&mut self) -> usize {
        let mut level = 1;
        // xorshift64*; each extra level has probability 1/4.
        loop {
            self.rng ^= self.rng >> 12;
            self.rng ^= self.rng << 25;
            self.rng ^= self.rng >> 27;
            let r = self.rng.wrapping_mul(0x2545_F491_4F6C_DD1D);
            if level < MAX_LEVEL && (r >> 32) & 3 == 0 {
                level += 1;
            } else {
                return level;
            }
        }
    }

    fn before(&self, idx: usize, score: f64, member: &[u8]) -> bool {
        let n = &self.nodes[idx];
        cmp_key(n.score, &n.member, score, member) == Ordering::Less
    }

    fn insert(&mut self, score: f64, member: Bytes) {
        let mut update = [HEADER; MAX_LEVEL];
        let mut rank = [0usize; MAX_LEVEL];
        let mut x = HEADER;
        for i in (0..self.level).rev() {
            rank[i] = if i == self.level - 1 { 0 } else { rank[i + 1] };
            loop {
                let fwd = self.nodes[x].links[i].forward;
                if fwd != NIL && self.before(fwd, score, &member) {
                    rank[i] += self.nodes[x].links[i].span;
                    x = fwd;
                } else {
                    break;
                }
            }
            update[i] = x;
        }
        let lvl = self.random_level();
        if lvl > self.level {
            for i in self.level..lvl {
                rank[i] = 0;
                update[i] = HEADER;
                self.nodes[HEADER].links[i].span = self.len;
            }
            self.level = lvl;
        }
        let node = Node {
            member,
            score,
            backward: NIL,
            links: vec![
                Link {
                    forward: NIL,
                    span: 0
                };
                lvl
            ],
        };
        let idx = match self.free.pop() {
            Some(slot) => {
                self.nodes[slot] = node;
                slot
            }
            None => {
                self.nodes.push(node);
                self.nodes.len() - 1
            }
        };
        for i in 0..lvl {
            let u = update[i];
            let ulink = self.nodes[u].links[i];
            self.nodes[idx].links[i] = Link {
                forward: ulink.forward,
                span: ulink.span - (rank[0] - rank[i]),
            };
            self.nodes[u].links[i] = Link {
                forward: idx,
                span: (rank[0] - rank[i]) + 1,
            };
        }
        for (i, &u) in update.iter().enumerate().take(self.level).skip(lvl) {
            self.nodes[u].links[i].span += 1;
        }
        self.nodes[idx].backward = if update[0] == HEADER { NIL } else { update[0] };
        let next = self.nodes[idx].links[0].forward;
        if next != NIL {
            self.nodes[next].backward = idx;
        } else {
            self.tail = idx;
        }
        self.len += 1;
    }

    fn delete(&mut self, score: f64, member: &[u8]) -> bool {
        let mut update = [HEADER; MAX_LEVEL];
        let mut x = HEADER;
        for i in (0..self.level).rev() {
            loop {
                let fwd = self.nodes[x].links[i].forward;
                if fwd != NIL && self.before(fwd, score, member) {
                    x = fwd;
                } else {
                    break;
                }
            }
            update[i] = x;
        }
        let target = self.nodes[update[0]].links[0].forward;
        if target == NIL
            || cmp_key(
                self.nodes[target].score,
                &self.nodes[target].member,
                score,
                member,
            ) != Ordering::Equal
        {
            return false;
        }
        for (i, &u) in update.iter().enumerate().take(self.level) {
            if self.nodes[u].links[i].forward == target {
                let t = self.nodes[target].links[i];
                self.nodes[u].links[i].span = self.nodes[u].links[i].span + t.span - 1;
                self.nodes[u].links[i].forward = t.forward;
            } else {
                self.nodes[u].links[i].span -= 1;
            }
        }
        let next = self.nodes[target].links[0].forward;
        let back = self.nodes[target].backward;
        if next != NIL {
            self.nodes[next].backward = back;
        } else {
            self.tail = back;
        }
        while self.level > 1 && self.nodes[HEADER].links[self.level - 1].forward == NIL {
            self.level -= 1;
        }
        self.len -= 1;
        self.nodes[target].member = Bytes::new();
        self.nodes[target].links = Vec::new();
        self.free.push(target);
        true
    }

    /// 1-based rank of `(score, member)`, or 0 when absent.
    fn rank_of(&self, score: f64, member: &[u8]) -> usize {
        let mut rank = 0;
        let mut x = HEADER;
        for i in (0..self.level).rev() {
            loop {
                let fwd = self.nodes[x].links[i].forward;
                if fwd != NIL && !self.after(fwd, score, member) {
                    rank += self.nodes[x].links[i].span;
                    x = fwd;
                } else {
                    break;
                }
            }
            if x != HEADER && self.nodes[x].member.as_ref() == member {
                return rank;
            }
        }
        0
    }

    fn after(&self, idx: usize, score: f64, member: &[u8]) -> bool {
        let n = &self.nodes[idx];
        cmp_key(n.score, &n.member, score, member) == Ordering::Greater
    }

    /// Node at 1-based `rank`.
    fn by_rank(&self, rank: usize) -> Option<usize> {
        if rank == 0 || rank > self.len {
            return None;
        }
        let mut traversed = 0;
        let mut x = HEADER;
        for i in (0..self.level).rev() {
            loop {
                let link = self.nodes[x].links[i];
                if link.forward != NIL && traversed + link.span <= rank {
                    traversed += link.span;
                    x = link.forward;
                } else {
                    break;
                }
            }
            if traversed == rank {
                return Some(x);
            }
        }
        None
    }

    fn first_in_range(&self, range: &ScoreRange) -> Option<usize> {
        if range.is_empty() {
            return None;
        }
        let mut x = HEADER;
        for i in (0..self.level).rev() {
            loop {
                let fwd = self.nodes[x].links[i].forward;
                if fwd != NIL && !range.gte_min(self.nodes[fwd].score) {
                    x = fwd;
                } else {
                    break;
                }
            }
        }
        let candidate = self.nodes[x].links[0].forward;
        (candidate != NIL && range.lte_max(self.nodes[candidate].score)).then_some(candidate)
    }

    fn last_in_range(&self, range: &ScoreRange) -> Option<usize> {
        if range.is_empty() {
            return None;
        }
        let mut x = HEADER;
        for i in (0..self.level).rev() {
            loop {
                let fwd = self.nodes[x].links[i].forward;
                if fwd != NIL && range.lte_max(self.nodes[fwd].score) {
                    x = fwd;
                } else {
                    break;
                }
            }
        }
        (x != HEADER && range.gte_min(self.nodes[x].score)).then_some(x)
    }
}

/// Result of [`ZSet::insert`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Inserted {
    /// The member did not exist.
    Added,
    /// The member existed and its score changed.
    Updated,
    /// The member existed with the same score.
    Unchanged,
}

/// A sorted set.
#[derive(Debug, Clone)]
pub struct ZSet {
    dict: HashMap<Bytes, f64, RandomState>,
    list: SkipList,
    bytes: usize,
}

impl Default for ZSet {
    fn default() -> Self {
        ZSet::new()
    }
}

impl ZSet {
    /// Creates an empty sorted set.
    pub fn new() -> Self {
        ZSet {
            dict: HashMap::default(),
            list: SkipList::new(),
            bytes: 0,
        }
    }

    /// Number of members.
    pub fn len(&self) -> usize {
        self.dict.len()
    }

    /// True when the set has no members.
    pub fn is_empty(&self) -> bool {
        self.dict.is_empty()
    }

    /// Approximate heap footprint.
    pub fn mem_size(&self) -> usize {
        self.bytes
    }

    /// Score of `member`.
    pub fn score(&self, member: &[u8]) -> Option<f64> {
        self.dict.get(member).copied()
    }

    /// Adds `member` or moves it to `score`. `score` must not be NaN.
    pub fn insert(&mut self, member: Bytes, score: f64) -> Inserted {
        debug_assert!(!score.is_nan());
        let score = if score == 0.0 { 0.0 } else { score };
        match self.dict.get(&member).copied() {
            Some(old) if old == score => Inserted::Unchanged,
            Some(old) => {
                self.list.delete(old, &member);
                self.list.insert(score, member.clone());
                self.dict.insert(member, score);
                Inserted::Updated
            }
            None => {
                self.bytes += member.len() + 8 + 2 * ELEM_OVERHEAD;
                self.list.insert(score, member.clone());
                self.dict.insert(member, score);
                Inserted::Added
            }
        }
    }

    /// Removes `member`; returns true when it existed.
    pub fn remove(&mut self, member: &[u8]) -> bool {
        match self.dict.remove_entry(member) {
            Some((m, score)) => {
                self.list.delete(score, &m);
                self.bytes -= m.len() + 8 + 2 * ELEM_OVERHEAD;
                true
            }
            None => false,
        }
    }

    /// 0-based rank of `member` (from the highest score when `rev`).
    pub fn rank(&self, member: &[u8], rev: bool) -> Option<usize> {
        let score = self.score(member)?;
        let r = self.list.rank_of(score, member) - 1;
        Some(if rev { self.len() - 1 - r } else { r })
    }

    /// Members at positions `start..=stop` in iteration order (`rev` counts from the
    /// highest score). Both indices must already be clamped to `0..len`.
    pub fn range(&self, start: usize, stop: usize, rev: bool) -> Vec<(Bytes, f64)> {
        if self.is_empty() || start > stop || start >= self.len() {
            return Vec::new();
        }
        let stop = stop.min(self.len() - 1);
        let want = stop - start + 1;
        let from = if rev { self.len() - 1 - start } else { start };
        self.walk(self.list.by_rank(from + 1), want, rev, |_| true)
    }

    /// Members whose score lies in `range`, honouring `LIMIT offset count`.
    pub fn range_by_score(
        &self,
        range: &ScoreRange,
        rev: bool,
        offset: usize,
        count: Option<usize>,
    ) -> Vec<(Bytes, f64)> {
        let mut start = if rev {
            self.list.last_in_range(range)
        } else {
            self.list.first_in_range(range)
        };
        for _ in 0..offset {
            start = start.and_then(|i| self.step(i, rev));
        }
        let in_range = |score: f64| {
            if rev {
                range.gte_min(score)
            } else {
                range.lte_max(score)
            }
        };
        self.walk(start, count.unwrap_or(usize::MAX), rev, |n| {
            in_range(n.score)
        })
    }

    /// Number of members whose score lies in `range`.
    pub fn count_in_range(&self, range: &ScoreRange) -> usize {
        let (Some(first), Some(last)) = (
            self.list.first_in_range(range),
            self.list.last_in_range(range),
        ) else {
            return 0;
        };
        let rank = |i: usize| {
            self.list
                .rank_of(self.list.nodes[i].score, &self.list.nodes[i].member)
        };
        (rank(last) + 1).saturating_sub(rank(first))
    }

    /// Removes and returns up to `count` lowest (or highest) members.
    pub fn pop(&mut self, highest: bool, count: usize) -> Vec<(Bytes, f64)> {
        if count == 0 {
            return Vec::new();
        }
        let popped = self.range(0, count - 1, highest);
        for (m, _) in &popped {
            self.remove(m);
        }
        popped
    }

    /// Removes members at positions `start..=stop`; returns how many were removed.
    pub fn remove_range_by_rank(&mut self, start: usize, stop: usize) -> usize {
        let doomed = self.range(start, stop, false);
        for (m, _) in &doomed {
            self.remove(m);
        }
        doomed.len()
    }

    /// Removes members whose score lies in `range`; returns how many were removed.
    pub fn remove_range_by_score(&mut self, range: &ScoreRange) -> usize {
        let doomed = self.range_by_score(range, false, 0, None);
        for (m, _) in &doomed {
            self.remove(m);
        }
        doomed.len()
    }

    /// Iterates all members in ascending order.
    pub fn iter(&self) -> impl Iterator<Item = (&Bytes, f64)> {
        let mut cur = self.list.nodes[HEADER].links[0].forward;
        std::iter::from_fn(move || {
            if cur == NIL {
                return None;
            }
            let n = &self.list.nodes[cur];
            cur = n.links[0].forward;
            Some((&n.member, n.score))
        })
    }

    fn step(&self, idx: usize, rev: bool) -> Option<usize> {
        let next = if rev {
            self.list.nodes[idx].backward
        } else {
            self.list.nodes[idx].links[0].forward
        };
        (next != NIL).then_some(next)
    }

    fn walk(
        &self,
        mut cur: Option<usize>,
        mut want: usize,
        rev: bool,
        keep: impl Fn(&Node) -> bool,
    ) -> Vec<(Bytes, f64)> {
        let mut out = Vec::new();
        while let Some(i) = cur {
            if want == 0 || !keep(&self.list.nodes[i]) {
                break;
            }
            let n = &self.list.nodes[i];
            out.push((n.member.clone(), n.score));
            want -= 1;
            cur = self.step(i, rev);
        }
        out
    }

    /// Verifies every skip-list invariant; used by tests.
    #[cfg(test)]
    pub(crate) fn check_invariants(&self) {
        let l = &self.list;
        assert_eq!(l.len, self.dict.len());
        // Level 0 order and positions.
        let mut pos_of = HashMap::new();
        let mut prev = HEADER;
        let mut cur = l.nodes[HEADER].links[0].forward;
        let mut pos = 0;
        let mut last: Option<(f64, Bytes)> = None;
        while cur != NIL {
            pos += 1;
            pos_of.insert(cur, pos);
            let n = &l.nodes[cur];
            assert_eq!(n.backward, if prev == HEADER { NIL } else { prev });
            if let Some((s, m)) = &last {
                assert_eq!(
                    cmp_key(*s, m, n.score, &n.member),
                    Ordering::Less,
                    "order violated"
                );
            }
            assert_eq!(self.dict.get(&n.member).copied(), Some(n.score));
            last = Some((n.score, n.member.clone()));
            prev = cur;
            cur = n.links[0].forward;
        }
        assert_eq!(pos, l.len);
        assert_eq!(l.tail, if l.len == 0 { NIL } else { prev });
        // Span consistency on every active level.
        for lvl in 0..l.level {
            let mut x = HEADER;
            let mut at = 0;
            loop {
                let link = l.nodes[x].links[lvl];
                if link.forward == NIL {
                    assert_eq!(at + link.span, l.len, "tail span at level {lvl}");
                    break;
                }
                assert_eq!(pos_of[&link.forward], at + link.span, "span at level {lvl}");
                at += link.span;
                x = link.forward;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use proptest::prelude::*;

    use super::*;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    fn full() -> ScoreRange {
        ScoreRange {
            min: f64::NEG_INFINITY,
            min_exclusive: false,
            max: f64::INFINITY,
            max_exclusive: false,
        }
    }

    #[test]
    fn basic_ordering_and_ties() {
        let mut z = ZSet::new();
        assert_eq!(z.insert(b("b"), 1.0), Inserted::Added);
        assert_eq!(z.insert(b("a"), 1.0), Inserted::Added);
        assert_eq!(z.insert(b("c"), 0.5), Inserted::Added);
        assert_eq!(z.insert(b("c"), 0.5), Inserted::Unchanged);
        let names: Vec<_> = z.range(0, 10, false).into_iter().map(|(m, _)| m).collect();
        assert_eq!(names, vec![b("c"), b("a"), b("b")]);
        assert_eq!(z.rank(b"a", false), Some(1));
        assert_eq!(z.rank(b"a", true), Some(1));
        assert_eq!(z.rank(b"nope", false), None);
        assert_eq!(z.insert(b("c"), 9.0), Inserted::Updated);
        assert_eq!(z.rank(b"c", false), Some(2));
        z.check_invariants();
    }

    #[test]
    fn negative_zero_is_normalised() {
        let mut z = ZSet::new();
        z.insert(b("a"), -0.0);
        assert_eq!(z.insert(b("a"), 0.0), Inserted::Unchanged);
        assert!(z.score(b"a").unwrap().is_sign_positive());
    }

    #[test]
    fn score_ranges_with_exclusive_bounds() {
        let mut z = ZSet::new();
        for (i, m) in ["a", "b", "c", "d", "e"].iter().enumerate() {
            z.insert(b(m), i as f64);
        }
        let r = ScoreRange {
            min: 1.0,
            min_exclusive: true,
            max: 3.0,
            max_exclusive: false,
        };
        let got: Vec<_> = z
            .range_by_score(&r, false, 0, None)
            .into_iter()
            .map(|(m, _)| m)
            .collect();
        assert_eq!(got, vec![b("c"), b("d")]);
        assert_eq!(z.count_in_range(&r), 2);
        let rev: Vec<_> = z
            .range_by_score(&r, true, 0, None)
            .into_iter()
            .map(|(m, _)| m)
            .collect();
        assert_eq!(rev, vec![b("d"), b("c")]);
        let limited = z.range_by_score(&full(), false, 1, Some(2));
        assert_eq!(limited.len(), 2);
        assert_eq!(limited[0].0, b("b"));
        let empty = ScoreRange {
            min: 2.0,
            min_exclusive: true,
            max: 2.0,
            max_exclusive: false,
        };
        assert_eq!(z.count_in_range(&empty), 0);
    }

    #[test]
    fn pop_and_remove_ranges() {
        let mut z = ZSet::new();
        for i in 0..10 {
            z.insert(b(&format!("m{i}")), i as f64);
        }
        assert_eq!(z.pop(false, 2).len(), 2);
        assert_eq!(z.pop(true, 1)[0].0, b("m9"));
        assert!(z.pop(true, 0).is_empty());
        assert_eq!(z.remove_range_by_rank(0, 1), 2);
        let r = ScoreRange {
            min: 5.0,
            min_exclusive: false,
            max: 6.0,
            max_exclusive: false,
        };
        assert_eq!(z.remove_range_by_score(&r), 2);
        assert_eq!(z.len(), 3);
        z.check_invariants();
    }

    #[test]
    fn memory_accounting_returns_to_zero() {
        let mut z = ZSet::new();
        for i in 0..100 {
            z.insert(b(&format!("member-{i}")), i as f64);
        }
        assert!(z.mem_size() > 0);
        for i in 0..100 {
            assert!(z.remove(format!("member-{i}").as_bytes()));
        }
        assert_eq!(z.mem_size(), 0);
        z.check_invariants();
    }

    #[derive(Debug, Clone)]
    enum Op {
        Add(u8, i8),
        Rem(u8),
        PopMin(u8),
        PopMax(u8),
        RemRank(u8, u8),
        RemScore(i8, i8),
    }

    fn op() -> impl Strategy<Value = Op> {
        prop_oneof![
            4 => (0u8..24, -6i8..6).prop_map(|(m, s)| Op::Add(m, s)),
            2 => (0u8..24).prop_map(Op::Rem),
            1 => (0u8..4).prop_map(Op::PopMin),
            1 => (0u8..4).prop_map(Op::PopMax),
            1 => (0u8..8, 0u8..8).prop_map(|(a, c)| Op::RemRank(a, c)),
            1 => (-6i8..6, -6i8..6).prop_map(|(a, c)| Op::RemScore(a, c)),
        ]
    }

    /// Reference model: a `BTreeMap` keyed by `(score, member)`.
    #[derive(Default)]
    struct Model {
        by_member: BTreeMap<Vec<u8>, i8>,
    }

    impl Model {
        fn sorted(&self) -> Vec<(Vec<u8>, f64)> {
            let mut v: Vec<_> = self
                .by_member
                .iter()
                .map(|(m, s)| (m.clone(), f64::from(*s)))
                .collect();
            v.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap().then_with(|| a.0.cmp(&b.0)));
            v
        }
    }

    fn member(i: u8) -> Vec<u8> {
        format!("m{i:02}").into_bytes()
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(200))]

        #[test]
        fn behaves_like_the_model(ops in proptest::collection::vec(op(), 1..120)) {
            let mut z = ZSet::new();
            let mut model = Model::default();
            for op in ops {
                match op {
                    Op::Add(m, s) => {
                        let inserted = z.insert(Bytes::from(member(m)), f64::from(s));
                        let prev = model.by_member.insert(member(m), s);
                        let expect = match prev { None => Inserted::Added, Some(p) if p == s => Inserted::Unchanged, Some(_) => Inserted::Updated };
                        prop_assert_eq!(inserted, expect);
                    }
                    Op::Rem(m) => {
                        prop_assert_eq!(z.remove(&member(m)), model.by_member.remove(&member(m)).is_some());
                    }
                    Op::PopMin(n) => {
                        let got: Vec<_> = z.pop(false, n as usize).into_iter().map(|(m, s)| (m.to_vec(), s)).collect();
                        let want: Vec<_> = model.sorted().into_iter().take(n as usize).collect();
                        for (m, _) in &want { model.by_member.remove(m); }
                        prop_assert_eq!(got, want);
                    }
                    Op::PopMax(n) => {
                        let got: Vec<_> = z.pop(true, n as usize).into_iter().map(|(m, s)| (m.to_vec(), s)).collect();
                        let want: Vec<_> = model.sorted().into_iter().rev().take(n as usize).collect();
                        for (m, _) in &want { model.by_member.remove(m); }
                        prop_assert_eq!(got, want);
                    }
                    Op::RemRank(a, c) => {
                        let (a, c) = (a.min(c) as usize, a.max(c) as usize);
                        let sorted = model.sorted();
                        let doomed: Vec<_> = sorted.iter().skip(a).take(c - a + 1).cloned().collect();
                        for (m, _) in &doomed { model.by_member.remove(m); }
                        prop_assert_eq!(z.remove_range_by_rank(a, c), doomed.len());
                    }
                    Op::RemScore(a, c) => {
                        let (lo, hi) = (a.min(c), a.max(c));
                        let doomed: Vec<_> = model.sorted().into_iter().filter(|(_, s)| *s >= f64::from(lo) && *s <= f64::from(hi)).collect();
                        for (m, _) in &doomed { model.by_member.remove(m); }
                        let r = ScoreRange { min: f64::from(lo), min_exclusive: false, max: f64::from(hi), max_exclusive: false };
                        prop_assert_eq!(z.remove_range_by_score(&r), doomed.len());
                    }
                }
                z.check_invariants();
                let sorted = model.sorted();
                prop_assert_eq!(z.len(), sorted.len());
                let all: Vec<_> = z.iter().map(|(m, s)| (m.to_vec(), s)).collect();
                prop_assert_eq!(&all, &sorted);
                for (i, (m, s)) in sorted.iter().enumerate() {
                    prop_assert_eq!(z.rank(m, false), Some(i));
                    prop_assert_eq!(z.rank(m, true), Some(sorted.len() - 1 - i));
                    prop_assert_eq!(z.score(m), Some(*s));
                }
                if !sorted.is_empty() {
                    let mid = sorted.len() / 2;
                    let got: Vec<_> = z.range(1.min(mid), mid, true).into_iter().map(|(m, s)| (m.to_vec(), s)).collect();
                    let want: Vec<_> = sorted.iter().rev().skip(1.min(mid)).take(mid - 1.min(mid) + 1).cloned().collect();
                    prop_assert_eq!(got, want);
                }
                for (lo, hi, lx, hx) in [(-3.0, 2.0, false, false), (-3.0, 2.0, true, true), (0.0, 0.0, false, false), (1.0, -1.0, false, false)] {
                    let r = ScoreRange { min: lo, min_exclusive: lx, max: hi, max_exclusive: hx };
                    let want: Vec<_> = sorted.iter().filter(|(_, s)| (if lx { *s > lo } else { *s >= lo }) && (if hx { *s < hi } else { *s <= hi })).cloned().collect();
                    prop_assert_eq!(z.count_in_range(&r), want.len());
                    let got: Vec<_> = z.range_by_score(&r, false, 0, None).into_iter().map(|(m, s)| (m.to_vec(), s)).collect();
                    prop_assert_eq!(&got, &want);
                    let got_rev: Vec<_> = z.range_by_score(&r, true, 0, None).into_iter().map(|(m, s)| (m.to_vec(), s)).collect();
                    let want_rev: Vec<_> = want.iter().rev().cloned().collect();
                    prop_assert_eq!(got_rev, want_rev);
                }
            }
        }
    }
}
