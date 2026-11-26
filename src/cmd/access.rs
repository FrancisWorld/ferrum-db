//! Argument parsing and typed keyspace access shared by all command modules.

use bytes::Bytes;

use crate::db::{HashVal, ListVal, SetVal, Shard, Value, ZSet};
use crate::error::CmdError;

/// Strict integer parsing: optional `-`, digits only, no whitespace or `+`.
pub fn parse_i64(b: &[u8]) -> Result<i64, CmdError> {
    if b.first() == Some(&b'+') {
        return Err(CmdError::NotInteger);
    }
    std::str::from_utf8(b)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(CmdError::NotInteger)
}

/// Parses a finite float (used by `INCRBYFLOAT`); rejects NaN and infinities.
pub fn parse_f64(b: &[u8]) -> Result<f64, CmdError> {
    let v = parse_score(b).map_err(|_| CmdError::NotFloat)?;
    if v.is_finite() {
        Ok(v)
    } else {
        Err(CmdError::NotFloat)
    }
}

/// Parses a score: a float or `inf` / `+inf` / `-inf`; NaN is rejected.
pub fn parse_score(b: &[u8]) -> Result<f64, CmdError> {
    let s = std::str::from_utf8(b).map_err(|_| CmdError::NotFloat)?;
    if s.is_empty() || s.trim() != s {
        return Err(CmdError::NotFloat);
    }
    match s.parse::<f64>() {
        Ok(v) if !v.is_nan() => Ok(v),
        _ => Err(CmdError::NotFloat),
    }
}

/// Case-insensitive ASCII comparison against an upper-case literal.
pub fn is(arg: &[u8], upper: &str) -> bool {
    arg.eq_ignore_ascii_case(upper.as_bytes())
}

/// Resolves Redis-style inclusive indices (negative counts from the end) into a
/// clamped `(start, stop)` pair, or `None` for an empty range.
pub fn clamp_range(start: i64, stop: i64, len: usize) -> Option<(usize, usize)> {
    let len = len as i64;
    let start = if start < 0 {
        (len + start).max(0)
    } else {
        start
    };
    let stop = if stop < 0 {
        len + stop
    } else {
        stop.min(len - 1)
    };
    (start <= stop && start < len).then_some((start as usize, stop as usize))
}

macro_rules! typed_access {
    ($read:ident, $write:ident, $create:ident, $variant:ident, $ty:ty) => {
        /// Read access to a key that must be of this type (`None` when absent).
        pub fn $read<'s>(
            shard: &'s mut Shard,
            key: &[u8],
            now: u64,
        ) -> Result<Option<&'s $ty>, CmdError> {
            match shard.get(key, now) {
                None => Ok(None),
                Some(e) => match &e.value {
                    Value::$variant(v) => Ok(Some(v)),
                    _ => Err(CmdError::WrongType),
                },
            }
        }

        /// Write access to an existing key of this type (`None` when absent).
        pub fn $write<'s>(
            shard: &'s mut Shard,
            key: &[u8],
            now: u64,
        ) -> Result<Option<&'s mut $ty>, CmdError> {
            if shard.get(key, now).is_none() {
                return Ok(None);
            }
            match shard.get_mut(key, now).map(|e| &mut e.value) {
                Some(Value::$variant(v)) => Ok(Some(v)),
                _ => Err(CmdError::WrongType),
            }
        }

        /// Write access that creates an empty value when the key is absent.
        pub fn $create<'s>(
            shard: &'s mut Shard,
            key: &Bytes,
            now: u64,
        ) -> Result<&'s mut $ty, CmdError> {
            if shard.get(key, now).is_none() {
                shard.insert(key.clone(), Value::$variant(<$ty>::default()), None, now);
            }
            match shard.get_mut(key, now).map(|e| &mut e.value) {
                Some(Value::$variant(v)) => Ok(v),
                _ => Err(CmdError::WrongType),
            }
        }
    };
}

typed_access!(list_ref, list_mut, list_create, List, ListVal);
typed_access!(hash_ref, hash_mut, hash_create, Hash, HashVal);
typed_access!(set_ref, set_mut, set_create, Set, SetVal);
typed_access!(zset_ref, zset_mut, zset_create, ZSet, ZSet);

/// Read access to a string key.
pub fn str_ref(shard: &mut Shard, key: &[u8], now: u64) -> Result<Option<Bytes>, CmdError> {
    match shard.get(key, now) {
        None => Ok(None),
        Some(e) => match &e.value {
            Value::Str(b) => Ok(Some(b.clone())),
            _ => Err(CmdError::WrongType),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_parsing_is_strict() {
        assert_eq!(parse_i64(b"-12"), Ok(-12));
        assert_eq!(parse_i64(b"+1"), Err(CmdError::NotInteger));
        assert_eq!(parse_i64(b" 1"), Err(CmdError::NotInteger));
        assert_eq!(parse_i64(b"9223372036854775808"), Err(CmdError::NotInteger));
        assert_eq!(parse_i64(b""), Err(CmdError::NotInteger));
    }

    #[test]
    fn score_parsing() {
        assert_eq!(parse_score(b"1.5"), Ok(1.5));
        assert_eq!(parse_score(b"-inf"), Ok(f64::NEG_INFINITY));
        assert_eq!(parse_score(b"+inf"), Ok(f64::INFINITY));
        assert!(parse_score(b"nan").is_err());
        assert!(parse_score(b"1 ").is_err());
        assert!(parse_f64(b"inf").is_err());
    }

    #[test]
    fn range_clamping() {
        assert_eq!(clamp_range(0, -1, 5), Some((0, 4)));
        assert_eq!(clamp_range(-100, 100, 5), Some((0, 4)));
        assert_eq!(clamp_range(2, 1, 5), None);
        assert_eq!(clamp_range(5, 9, 5), None);
        assert_eq!(clamp_range(-2, -1, 5), Some((3, 4)));
        assert_eq!(clamp_range(0, 0, 0), None);
    }
}
