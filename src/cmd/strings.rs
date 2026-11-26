//! String commands.

use bytes::Bytes;

use super::access::{is, parse_f64, parse_i64, str_ref};
use super::{Command, Ctx, DENYOOM, LockPlan, WRITE};
use crate::db::Value;
use crate::error::{CmdError, Reply};
use crate::resp::{Frame, format_double};

const MAX_STRING_LEN: usize = 512 * 1024 * 1024;

const ONE_KEY: LockPlan = LockPlan::Keys {
    first: 1,
    last: 1,
    step: 1,
};

pub(super) static COMMANDS: &[Command] = &[
    Command {
        name: "GET",
        arity: 2,
        flags: 0,
        lock: ONE_KEY,
        handler: get,
    },
    Command {
        name: "SET",
        arity: -3,
        flags: WRITE | DENYOOM,
        lock: ONE_KEY,
        handler: set,
    },
    Command {
        name: "SETNX",
        arity: 3,
        flags: WRITE | DENYOOM,
        lock: ONE_KEY,
        handler: setnx,
    },
    Command {
        name: "SETEX",
        arity: 4,
        flags: WRITE | DENYOOM,
        lock: ONE_KEY,
        handler: setex,
    },
    Command {
        name: "PSETEX",
        arity: 4,
        flags: WRITE | DENYOOM,
        lock: ONE_KEY,
        handler: psetex,
    },
    Command {
        name: "GETSET",
        arity: 3,
        flags: WRITE | DENYOOM,
        lock: ONE_KEY,
        handler: getset,
    },
    Command {
        name: "GETDEL",
        arity: 2,
        flags: WRITE,
        lock: ONE_KEY,
        handler: getdel,
    },
    Command {
        name: "GETEX",
        arity: -2,
        flags: WRITE,
        lock: ONE_KEY,
        handler: getex,
    },
    Command {
        name: "MGET",
        arity: -2,
        flags: 0,
        lock: LockPlan::Keys {
            first: 1,
            last: -1,
            step: 1,
        },
        handler: mget,
    },
    Command {
        name: "MSET",
        arity: -3,
        flags: WRITE | DENYOOM,
        lock: LockPlan::Keys {
            first: 1,
            last: -2,
            step: 2,
        },
        handler: mset,
    },
    Command {
        name: "MSETNX",
        arity: -3,
        flags: WRITE | DENYOOM,
        lock: LockPlan::Keys {
            first: 1,
            last: -2,
            step: 2,
        },
        handler: msetnx,
    },
    Command {
        name: "INCR",
        arity: 2,
        flags: WRITE | DENYOOM,
        lock: ONE_KEY,
        handler: incr,
    },
    Command {
        name: "DECR",
        arity: 2,
        flags: WRITE | DENYOOM,
        lock: ONE_KEY,
        handler: decr,
    },
    Command {
        name: "INCRBY",
        arity: 3,
        flags: WRITE | DENYOOM,
        lock: ONE_KEY,
        handler: incrby,
    },
    Command {
        name: "DECRBY",
        arity: 3,
        flags: WRITE | DENYOOM,
        lock: ONE_KEY,
        handler: decrby,
    },
    Command {
        name: "INCRBYFLOAT",
        arity: 3,
        flags: WRITE | DENYOOM,
        lock: ONE_KEY,
        handler: incrbyfloat,
    },
    Command {
        name: "APPEND",
        arity: 3,
        flags: WRITE | DENYOOM,
        lock: ONE_KEY,
        handler: append,
    },
    Command {
        name: "STRLEN",
        arity: 2,
        flags: 0,
        lock: ONE_KEY,
        handler: strlen,
    },
    Command {
        name: "GETRANGE",
        arity: 4,
        flags: 0,
        lock: ONE_KEY,
        handler: getrange,
    },
    Command {
        name: "SETRANGE",
        arity: 4,
        flags: WRITE | DENYOOM,
        lock: ONE_KEY,
        handler: setrange,
    },
];

#[derive(Clone, Copy)]
enum Expiry {
    At(u64),
    Keep,
    Persist,
}

/// Parses `EX|PX|EXAT|PXAT <n>` into an absolute millisecond deadline.
fn parse_expiry(opt: &[u8], raw: &[u8], now: u64, cmd: &str) -> Result<u64, CmdError> {
    let n = parse_i64(raw)?;
    let invalid = || CmdError::InvalidExpire(cmd.into());
    if n <= 0 {
        return Err(invalid());
    }
    let n = n as u64;
    let (ms, absolute) = match opt.to_ascii_uppercase().as_slice() {
        b"EX" => (n.checked_mul(1000).ok_or_else(invalid)?, false),
        b"PX" => (n, false),
        b"EXAT" => (n.checked_mul(1000).ok_or_else(invalid)?, true),
        _ => (n, true),
    };
    if absolute {
        Ok(ms)
    } else {
        now.checked_add(ms).ok_or_else(invalid)
    }
}

fn get(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    Ok(str_ref(ctx.shard(&args[1]), &args[1], now)?.map_or(Frame::Null, Frame::Bulk))
}

fn set(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let (key, value) = (&args[1], &args[2]);
    let (mut nx, mut xx, mut want_old) = (false, false, false);
    let mut expiry: Option<Expiry> = None;
    let mut i = 3;
    while i < args.len() {
        let opt = &args[i];
        if is(opt, "NX") && !xx {
            nx = true;
        } else if is(opt, "XX") && !nx {
            xx = true;
        } else if is(opt, "GET") {
            want_old = true;
        } else if is(opt, "KEEPTTL") && expiry.is_none() {
            expiry = Some(Expiry::Keep);
        } else if ["EX", "PX", "EXAT", "PXAT"].iter().any(|o| is(opt, o)) && expiry.is_none() {
            let raw = args.get(i + 1).ok_or(CmdError::Syntax)?;
            expiry = Some(Expiry::At(parse_expiry(opt, raw, now, "set")?));
            i += 1;
        } else {
            return Err(CmdError::Syntax);
        }
        i += 1;
    }

    let shard = ctx.shard(key);
    let old = if want_old {
        str_ref(shard, key, now)?
    } else {
        None
    };
    let (exists, old_ttl) = match shard.get(key, now) {
        Some(e) => (true, e.expire_at),
        None => (false, None),
    };
    let reply_old = || old.clone().map_or(Frame::Null, Frame::Bulk);
    if (nx && exists) || (xx && !exists) {
        return Ok(if want_old { reply_old() } else { Frame::Null });
    }
    let (expire_at, log_opt) = match expiry {
        Some(Expiry::At(at)) => (Some(at), Some(("PXAT", Some(at)))),
        Some(Expiry::Keep) => (old_ttl, Some(("KEEPTTL", None))),
        _ => (None, None),
    };
    shard.insert(key.clone(), Value::Str(value.clone()), expire_at, now);

    let mut log = vec![args[0].clone(), key.clone(), value.clone()];
    if let Some((opt, at)) = log_opt {
        log.push(Bytes::from_static(opt.as_bytes()));
        if let Some(at) = at {
            log.push(Bytes::from(at.to_string()));
        }
    }
    ctx.propagate(log);
    Ok(if want_old { reply_old() } else { Frame::OK })
}

fn setnx(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let shard = ctx.shard(&args[1]);
    if shard.get(&args[1], now).is_some() {
        return Ok(Frame::Integer(0));
    }
    shard.insert(args[1].clone(), Value::Str(args[2].clone()), None, now);
    ctx.propagate_args(args);
    Ok(Frame::Integer(1))
}

fn setex_common(ctx: &mut Ctx, args: &[Bytes], opt: &'static [u8], cmd: &str) -> Reply {
    let now = ctx.now;
    let at = parse_expiry(opt, &args[2], now, cmd)?;
    ctx.shard(&args[1])
        .insert(args[1].clone(), Value::Str(args[3].clone()), Some(at), now);
    ctx.propagate(vec![
        Bytes::from_static(b"SET"),
        args[1].clone(),
        args[3].clone(),
        Bytes::from_static(b"PXAT"),
        Bytes::from(at.to_string()),
    ]);
    Ok(Frame::OK)
}

fn setex(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    setex_common(ctx, args, b"EX", "setex")
}

fn psetex(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    setex_common(ctx, args, b"PX", "psetex")
}

fn getset(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let shard = ctx.shard(&args[1]);
    let old = str_ref(shard, &args[1], now)?;
    shard.insert(args[1].clone(), Value::Str(args[2].clone()), None, now);
    ctx.propagate(vec![
        Bytes::from_static(b"SET"),
        args[1].clone(),
        args[2].clone(),
    ]);
    Ok(old.map_or(Frame::Null, Frame::Bulk))
}

fn getdel(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let shard = ctx.shard(&args[1]);
    let Some(old) = str_ref(shard, &args[1], now)? else {
        return Ok(Frame::Null);
    };
    shard.remove(&args[1], now);
    ctx.propagate(vec![Bytes::from_static(b"DEL"), args[1].clone()]);
    Ok(Frame::Bulk(old))
}

fn getex(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let key = &args[1];
    let change = match args.len() {
        2 => None,
        3 if is(&args[2], "PERSIST") => Some(Expiry::Persist),
        4 if ["EX", "PX", "EXAT", "PXAT"].iter().any(|o| is(&args[2], o)) => {
            Some(Expiry::At(parse_expiry(&args[2], &args[3], now, "getex")?))
        }
        _ => return Err(CmdError::Syntax),
    };
    let shard = ctx.shard(key);
    let Some(value) = str_ref(shard, key, now)? else {
        return Ok(Frame::Null);
    };
    match change {
        Some(Expiry::At(at)) => {
            shard.set_expire(key, Some(at), now);
            ctx.propagate(vec![
                Bytes::from_static(b"PEXPIREAT"),
                key.clone(),
                Bytes::from(at.to_string()),
            ]);
        }
        Some(Expiry::Persist) if shard.get(key, now).is_some_and(|e| e.expire_at.is_some()) => {
            shard.set_expire(key, None, now);
            ctx.propagate(vec![Bytes::from_static(b"PERSIST"), key.clone()]);
        }
        _ => {}
    }
    Ok(Frame::Bulk(value))
}

fn mget(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let items = args[1..]
        .iter()
        .map(|k| match str_ref(ctx.shard(k), k, now) {
            Ok(Some(v)) => Frame::Bulk(v),
            _ => Frame::Null,
        })
        .collect();
    Ok(Frame::Array(items))
}

fn mset(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    if args.len().is_multiple_of(2) {
        return Err(CmdError::Arity("mset".into()));
    }
    let now = ctx.now;
    for pair in args[1..].as_chunks::<2>().0 {
        ctx.shard(&pair[0])
            .insert(pair[0].clone(), Value::Str(pair[1].clone()), None, now);
    }
    ctx.propagate_args(args);
    Ok(Frame::OK)
}

fn msetnx(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    if args.len().is_multiple_of(2) {
        return Err(CmdError::Arity("msetnx".into()));
    }
    let now = ctx.now;
    for pair in args[1..].as_chunks::<2>().0 {
        if ctx.shard(&pair[0]).get(&pair[0], now).is_some() {
            return Ok(Frame::Integer(0));
        }
    }
    for pair in args[1..].as_chunks::<2>().0 {
        ctx.shard(&pair[0])
            .insert(pair[0].clone(), Value::Str(pair[1].clone()), None, now);
    }
    ctx.propagate_args(args);
    Ok(Frame::Integer(1))
}

fn add_signed(ctx: &mut Ctx, args: &[Bytes], delta: i64) -> Reply {
    let now = ctx.now;
    let key = &args[1];
    let shard = ctx.shard(key);
    let current = match str_ref(shard, key, now)? {
        Some(b) => parse_i64(&b)?,
        None => 0,
    };
    let next = current.checked_add(delta).ok_or(CmdError::Overflow)?;
    let value = Value::Str(Bytes::from(next.to_string()));
    match shard.get_mut(key, now) {
        Some(entry) => entry.value = value,
        None => shard.insert(key.clone(), value, None, now),
    }
    ctx.propagate(vec![
        Bytes::from_static(b"INCRBY"),
        key.clone(),
        Bytes::from(delta.to_string()),
    ]);
    Ok(Frame::Integer(next))
}

fn incr(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    add_signed(ctx, args, 1)
}

fn decr(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    add_signed(ctx, args, -1)
}

fn incrby(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let delta = parse_i64(&args[2])?;
    add_signed(ctx, args, delta)
}

fn decrby(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let delta = parse_i64(&args[2])?
        .checked_neg()
        .ok_or(CmdError::Overflow)?;
    add_signed(ctx, args, delta)
}

fn incrbyfloat(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let key = &args[1];
    let delta = parse_f64(&args[2])?;
    let shard = ctx.shard(key);
    let current = match str_ref(shard, key, now)? {
        Some(b) => parse_f64(&b)?,
        None => 0.0,
    };
    let next = current + delta;
    if !next.is_finite() {
        return Err(CmdError::other("increment would produce NaN or Infinity"));
    }
    let text = Bytes::from(format_double(next));
    match shard.get_mut(key, now) {
        Some(entry) => entry.value = Value::Str(text.clone()),
        None => shard.insert(key.clone(), Value::Str(text.clone()), None, now),
    }
    ctx.propagate(vec![
        Bytes::from_static(b"SET"),
        key.clone(),
        text.clone(),
        Bytes::from_static(b"KEEPTTL"),
    ]);
    Ok(Frame::Bulk(text))
}

fn append(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let key = &args[1];
    let shard = ctx.shard(key);
    let old = str_ref(shard, key, now)?;
    let mut joined = Vec::with_capacity(old.as_ref().map_or(0, Bytes::len) + args[2].len());
    joined.extend_from_slice(old.as_deref().unwrap_or_default());
    joined.extend_from_slice(&args[2]);
    if joined.len() > MAX_STRING_LEN {
        return Err(CmdError::other(
            "string exceeds maximum allowed size (proto-max-bulk-len)",
        ));
    }
    let len = joined.len();
    let value = Value::Str(Bytes::from(joined));
    match shard.get_mut(key, now) {
        Some(entry) => entry.value = value,
        None => shard.insert(key.clone(), value, None, now),
    }
    ctx.propagate_args(args);
    Ok(Frame::Integer(len as i64))
}

fn strlen(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let len = str_ref(ctx.shard(&args[1]), &args[1], now)?.map_or(0, |b| b.len());
    Ok(Frame::Integer(len as i64))
}

fn getrange(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let (start, end) = (parse_i64(&args[2])?, parse_i64(&args[3])?);
    let Some(value) = str_ref(ctx.shard(&args[1]), &args[1], now)? else {
        return Ok(Frame::bulk(""));
    };
    Ok(match super::access::clamp_range(start, end, value.len()) {
        Some((a, b)) => Frame::Bulk(value.slice(a..=b)),
        None => Frame::bulk(""),
    })
}

fn setrange(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let key = &args[1];
    let offset = usize::try_from(parse_i64(&args[2])?)
        .map_err(|_| CmdError::other("offset is out of range"))?;
    let patch = &args[3];
    if offset.saturating_add(patch.len()) > MAX_STRING_LEN {
        return Err(CmdError::other(
            "string exceeds maximum allowed size (proto-max-bulk-len)",
        ));
    }
    let shard = ctx.shard(key);
    let old = str_ref(shard, key, now)?;
    if patch.is_empty() {
        return Ok(Frame::Integer(old.map_or(0, |b| b.len()) as i64));
    }
    let mut buf = old.map_or_else(Vec::new, |b| b.to_vec());
    if buf.len() < offset + patch.len() {
        buf.resize(offset + patch.len(), 0);
    }
    buf[offset..offset + patch.len()].copy_from_slice(patch);
    let len = buf.len();
    let value = Value::Str(Bytes::from(buf));
    match shard.get_mut(key, now) {
        Some(entry) => entry.value = value,
        None => shard.insert(key.clone(), value, None, now),
    }
    ctx.propagate_args(args);
    Ok(Frame::Integer(len as i64))
}
