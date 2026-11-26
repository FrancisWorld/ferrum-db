//! Generic keyspace commands: deletion, TTLs, renaming, scanning.

use bytes::Bytes;

use super::access::{is, parse_i64};
use super::{Command, Ctx, LockPlan, NO_TX, WRITE};
use crate::error::{CmdError, Reply};
use crate::glob;
use crate::resp::Frame;

const ONE_KEY: LockPlan = LockPlan::Keys {
    first: 1,
    last: 1,
    step: 1,
};
const ALL_ARGS: LockPlan = LockPlan::Keys {
    first: 1,
    last: -1,
    step: 1,
};

pub(super) static COMMANDS: &[Command] = &[
    Command {
        name: "DEL",
        arity: -2,
        flags: WRITE,
        lock: ALL_ARGS,
        handler: del,
    },
    Command {
        name: "UNLINK",
        arity: -2,
        flags: WRITE,
        lock: ALL_ARGS,
        handler: del,
    },
    Command {
        name: "EXISTS",
        arity: -2,
        flags: 0,
        lock: ALL_ARGS,
        handler: exists,
    },
    Command {
        name: "EXPIRE",
        arity: -3,
        flags: WRITE,
        lock: ONE_KEY,
        handler: expire,
    },
    Command {
        name: "PEXPIRE",
        arity: -3,
        flags: WRITE,
        lock: ONE_KEY,
        handler: pexpire,
    },
    Command {
        name: "EXPIREAT",
        arity: -3,
        flags: WRITE,
        lock: ONE_KEY,
        handler: expireat,
    },
    Command {
        name: "PEXPIREAT",
        arity: -3,
        flags: WRITE,
        lock: ONE_KEY,
        handler: pexpireat,
    },
    Command {
        name: "TTL",
        arity: 2,
        flags: 0,
        lock: ONE_KEY,
        handler: ttl,
    },
    Command {
        name: "PTTL",
        arity: 2,
        flags: 0,
        lock: ONE_KEY,
        handler: pttl,
    },
    Command {
        name: "PERSIST",
        arity: 2,
        flags: WRITE,
        lock: ONE_KEY,
        handler: persist,
    },
    Command {
        name: "TYPE",
        arity: 2,
        flags: 0,
        lock: ONE_KEY,
        handler: type_,
    },
    Command {
        name: "RENAME",
        arity: 3,
        flags: WRITE,
        lock: LockPlan::Keys {
            first: 1,
            last: 2,
            step: 1,
        },
        handler: rename,
    },
    Command {
        name: "RENAMENX",
        arity: 3,
        flags: WRITE,
        lock: LockPlan::Keys {
            first: 1,
            last: 2,
            step: 1,
        },
        handler: renamenx,
    },
    Command {
        name: "KEYS",
        arity: 2,
        flags: 0,
        lock: LockPlan::All,
        handler: keys,
    },
    Command {
        name: "SCAN",
        arity: -2,
        flags: NO_TX,
        lock: LockPlan::Scan,
        handler: scan,
    },
    Command {
        name: "RANDOMKEY",
        arity: 1,
        flags: 0,
        lock: LockPlan::All,
        handler: randomkey,
    },
    Command {
        name: "DBSIZE",
        arity: 1,
        flags: 0,
        lock: LockPlan::None,
        handler: dbsize,
    },
    Command {
        name: "FLUSHALL",
        arity: -1,
        flags: WRITE,
        lock: LockPlan::All,
        handler: flushall,
    },
    Command {
        name: "FLUSHDB",
        arity: -1,
        flags: WRITE,
        lock: LockPlan::All,
        handler: flushall,
    },
];

fn del(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let removed = args[1..]
        .iter()
        .filter(|k| ctx.shard(k).remove(k, now).is_some())
        .count();
    if removed > 0 {
        ctx.propagate_args(args);
    }
    Ok(Frame::Integer(removed as i64))
}

fn exists(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let n = args[1..]
        .iter()
        .filter(|k| ctx.shard(k).get(k, now).is_some())
        .count();
    Ok(Frame::Integer(n as i64))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Unit {
    Seconds,
    Millis,
}

fn expire_generic(ctx: &mut Ctx, args: &[Bytes], unit: Unit, absolute: bool) -> Reply {
    let now = ctx.now;
    let key = &args[1];
    let raw = parse_i64(&args[2])?;
    let name = String::from_utf8_lossy(&args[0]).to_ascii_lowercase();
    let invalid = || CmdError::InvalidExpire(name.clone());
    let ms = if unit == Unit::Seconds {
        raw.checked_mul(1000).ok_or_else(invalid)?
    } else {
        raw
    };
    let at = if absolute {
        ms
    } else {
        i64::try_from(now)
            .ok()
            .and_then(|n| n.checked_add(ms))
            .ok_or_else(invalid)?
    };

    let (mut nx, mut xx, mut gt, mut lt) = (false, false, false, false);
    for opt in &args[3..] {
        match opt.to_ascii_uppercase().as_slice() {
            b"NX" => nx = true,
            b"XX" => xx = true,
            b"GT" => gt = true,
            b"LT" => lt = true,
            _ => return Err(CmdError::Syntax),
        }
    }
    if (nx && (xx || gt || lt)) || (gt && lt) {
        return Err(CmdError::other(
            "NX and XX, GT or LT options at the same time are not compatible",
        ));
    }

    let shard = ctx.shard(key);
    let Some(current) = shard.get(key, now).map(|e| e.expire_at) else {
        return Ok(Frame::Integer(0));
    };
    // A key without TTL behaves as an infinite TTL for GT / LT.
    let allowed = match current {
        None => !xx && !gt,
        Some(cur) => !nx && (!gt || at.max(0) as u64 > cur) && (!lt || (at.max(0) as u64) < cur),
    };
    if !allowed {
        return Ok(Frame::Integer(0));
    }
    if at <= now as i64 {
        shard.remove(key, now);
        ctx.propagate(vec![Bytes::from_static(b"DEL"), key.clone()]);
    } else {
        shard.set_expire(key, Some(at as u64), now);
        ctx.propagate(vec![
            Bytes::from_static(b"PEXPIREAT"),
            key.clone(),
            Bytes::from(at.to_string()),
        ]);
    }
    Ok(Frame::Integer(1))
}

fn expire(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    expire_generic(ctx, args, Unit::Seconds, false)
}

fn pexpire(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    expire_generic(ctx, args, Unit::Millis, false)
}

fn expireat(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    expire_generic(ctx, args, Unit::Seconds, true)
}

fn pexpireat(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    expire_generic(ctx, args, Unit::Millis, true)
}

fn remaining(ctx: &mut Ctx, key: &[u8], per_unit: u64) -> Reply {
    let now = ctx.now;
    Ok(Frame::Integer(match ctx.shard(key).get(key, now) {
        None => -2,
        Some(e) => match e.expire_at {
            None => -1,
            // Round to nearest second like Redis does.
            Some(at) => (at.saturating_sub(now).saturating_add(per_unit / 2) / per_unit) as i64,
        },
    }))
}

fn ttl(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    remaining(ctx, &args[1], 1000)
}

fn pttl(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    remaining(ctx, &args[1], 1)
}

fn persist(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let shard = ctx.shard(&args[1]);
    if shard
        .get(&args[1], now)
        .is_some_and(|e| e.expire_at.is_some())
    {
        shard.set_expire(&args[1], None, now);
        ctx.propagate_args(args);
        return Ok(Frame::Integer(1));
    }
    Ok(Frame::Integer(0))
}

fn type_(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let name = ctx
        .shard(&args[1])
        .get(&args[1], now)
        .map_or("none", |e| e.value.type_name());
    Ok(Frame::simple(name))
}

fn rename_generic(ctx: &mut Ctx, args: &[Bytes], only_if_absent: bool) -> Reply {
    let now = ctx.now;
    let (src, dst) = (&args[1], &args[2]);
    if ctx.shard(src).get(src, now).is_none() {
        return Err(CmdError::NoSuchKey);
    }
    if src == dst {
        return Ok(if only_if_absent {
            Frame::Integer(0)
        } else {
            Frame::OK
        });
    }
    if only_if_absent && ctx.shard(dst).get(dst, now).is_some() {
        return Ok(Frame::Integer(0));
    }
    let Some(entry) = ctx.shard(src).remove(src, now) else {
        return Err(CmdError::NoSuchKey);
    };
    let shard = ctx.shard(dst);
    shard.remove(dst, now);
    shard.insert(dst.clone(), entry.value, entry.expire_at, now);
    ctx.propagate_args(args);
    Ok(if only_if_absent {
        Frame::Integer(1)
    } else {
        Frame::OK
    })
}

fn rename(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    rename_generic(ctx, args, false)
}

fn renamenx(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    rename_generic(ctx, args, true)
}

fn keys(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let pattern = &args[1];
    let mut out = Vec::new();
    for shard in ctx.locked.shards_mut() {
        out.extend(
            shard
                .iter()
                .filter(|(_, e)| e.expire_at.is_none_or(|at| at > now))
                .filter(|(k, _)| glob::matches(pattern, k))
                .map(|(k, _)| Frame::Bulk(k.clone())),
        );
    }
    Ok(Frame::Array(out))
}

/// Cursor layout: high 32 bits hold the shard, low 32 bits the number of slots of
/// that shard still to visit. Slots are visited from the end towards 0 so that
/// `swap_remove` during iteration can only cause repeats, never omissions.
const FRESH_SHARD: u64 = 0xFFFF_FFFF;

/// Splits a `SCAN` cursor into `(shard, remaining_slots)`.
pub(crate) fn decode_cursor(cursor: u64) -> (usize, u64) {
    if cursor == 0 {
        (0, FRESH_SHARD)
    } else {
        ((cursor >> 32) as usize, cursor & 0xFFFF_FFFF)
    }
}

fn scan(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let cursor: u64 = std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| CmdError::other("invalid cursor"))?;
    let (mut pattern, mut count, mut type_filter) = (None::<&Bytes>, 10usize, None::<String>);
    let mut i = 2;
    while i < args.len() {
        let value = args.get(i + 1).ok_or(CmdError::Syntax)?;
        if is(&args[i], "MATCH") {
            pattern = Some(value);
        } else if is(&args[i], "COUNT") {
            count = usize::try_from(parse_i64(value)?)
                .ok()
                .filter(|&n| n > 0)
                .ok_or(CmdError::Syntax)?;
        } else if is(&args[i], "TYPE") {
            type_filter = Some(String::from_utf8_lossy(value).to_ascii_lowercase());
        } else {
            return Err(CmdError::Syntax);
        }
        i += 2;
    }

    let (shard_idx, remaining) = decode_cursor(cursor);
    let shard_count = ctx.locked.db().shard_count();
    if shard_idx >= shard_count {
        return Err(CmdError::other("invalid cursor"));
    }
    let shard = ctx
        .locked
        .shard_by_index(shard_idx)
        .ok_or_else(|| CmdError::other("invalid cursor"))?;
    let mut pos = (remaining as usize).min(shard.len());
    let mut found = Vec::new();
    for _ in 0..count {
        if pos == 0 {
            break;
        }
        pos -= 1;
        let Some((key, entry)) = shard.entry_at(pos) else {
            continue;
        };
        if entry.expire_at.is_some_and(|at| at <= now) {
            continue;
        }
        if pattern.is_some_and(|p| !glob::matches(p, key)) {
            continue;
        }
        if type_filter
            .as_deref()
            .is_some_and(|t| t != entry.value.type_name())
        {
            continue;
        }
        found.push(Frame::Bulk(key.clone()));
    }
    let next = if pos > 0 {
        ((shard_idx as u64) << 32) | pos as u64
    } else if shard_idx + 1 < shard_count {
        (((shard_idx + 1) as u64) << 32) | FRESH_SHARD
    } else {
        0
    };
    Ok(Frame::Array(vec![
        Frame::bulk(next.to_string()),
        Frame::Array(found),
    ]))
}

fn randomkey(ctx: &mut Ctx, _args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let mut live: Vec<Bytes> = Vec::new();
    for shard in ctx.locked.shards_mut() {
        live.extend(
            shard
                .iter()
                .filter(|(_, e)| e.expire_at.is_none_or(|at| at > now))
                .map(|(k, _)| k.clone()),
        );
    }
    if live.is_empty() {
        return Ok(Frame::Null);
    }
    Ok(Frame::Bulk(live.swap_remove(fastrand::usize(..live.len()))))
}

fn dbsize(ctx: &mut Ctx, _args: &[Bytes]) -> Reply {
    Ok(Frame::Integer(ctx.locked.db().key_count() as i64))
}

fn flushall(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    for arg in &args[1..] {
        if !(is(arg, "ASYNC") || is(arg, "SYNC")) {
            return Err(CmdError::Syntax);
        }
    }
    for shard in ctx.locked.shards_mut() {
        shard.clear();
    }
    ctx.propagate(vec![Bytes::from_static(b"FLUSHALL")]);
    Ok(Frame::OK)
}
