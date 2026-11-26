//! Hash commands.

use bytes::Bytes;

use super::access::{hash_create, hash_mut, hash_ref, parse_f64, parse_i64};
use super::{Command, Ctx, DENYOOM, LockPlan, WRITE};
use crate::error::{CmdError, Reply};
use crate::resp::{Frame, format_double};

const ONE_KEY: LockPlan = LockPlan::Keys {
    first: 1,
    last: 1,
    step: 1,
};

pub(super) static COMMANDS: &[Command] = &[
    Command {
        name: "HSET",
        arity: -4,
        flags: WRITE | DENYOOM,
        lock: ONE_KEY,
        handler: hset,
    },
    Command {
        name: "HMSET",
        arity: -4,
        flags: WRITE | DENYOOM,
        lock: ONE_KEY,
        handler: hmset,
    },
    Command {
        name: "HSETNX",
        arity: 4,
        flags: WRITE | DENYOOM,
        lock: ONE_KEY,
        handler: hsetnx,
    },
    Command {
        name: "HGET",
        arity: 3,
        flags: 0,
        lock: ONE_KEY,
        handler: hget,
    },
    Command {
        name: "HMGET",
        arity: -3,
        flags: 0,
        lock: ONE_KEY,
        handler: hmget,
    },
    Command {
        name: "HDEL",
        arity: -3,
        flags: WRITE,
        lock: ONE_KEY,
        handler: hdel,
    },
    Command {
        name: "HEXISTS",
        arity: 3,
        flags: 0,
        lock: ONE_KEY,
        handler: hexists,
    },
    Command {
        name: "HLEN",
        arity: 2,
        flags: 0,
        lock: ONE_KEY,
        handler: hlen,
    },
    Command {
        name: "HGETALL",
        arity: 2,
        flags: 0,
        lock: ONE_KEY,
        handler: hgetall,
    },
    Command {
        name: "HKEYS",
        arity: 2,
        flags: 0,
        lock: ONE_KEY,
        handler: hkeys,
    },
    Command {
        name: "HVALS",
        arity: 2,
        flags: 0,
        lock: ONE_KEY,
        handler: hvals,
    },
    Command {
        name: "HINCRBY",
        arity: 4,
        flags: WRITE | DENYOOM,
        lock: ONE_KEY,
        handler: hincrby,
    },
    Command {
        name: "HINCRBYFLOAT",
        arity: 4,
        flags: WRITE | DENYOOM,
        lock: ONE_KEY,
        handler: hincrbyfloat,
    },
];

fn set_pairs(ctx: &mut Ctx, args: &[Bytes]) -> Result<usize, CmdError> {
    if !args.len().is_multiple_of(2) {
        return Err(CmdError::Arity(
            String::from_utf8_lossy(&args[0]).to_ascii_lowercase(),
        ));
    }
    let now = ctx.now;
    let hash = hash_create(ctx.shard(&args[1]), &args[1], now)?;
    let added = args[2..]
        .as_chunks::<2>()
        .0
        .iter()
        .filter(|p| hash.insert(p[0].clone(), p[1].clone()))
        .count();
    ctx.propagate_args(args);
    Ok(added)
}

fn hset(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    Ok(Frame::Integer(set_pairs(ctx, args)? as i64))
}

fn hmset(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    set_pairs(ctx, args)?;
    Ok(Frame::OK)
}

fn hsetnx(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let exists =
        hash_ref(ctx.shard(&args[1]), &args[1], now)?.is_some_and(|h| h.get(&args[2]).is_some());
    if exists {
        return Ok(Frame::Integer(0));
    }
    hash_create(ctx.shard(&args[1]), &args[1], now)?.insert(args[2].clone(), args[3].clone());
    ctx.propagate_args(args);
    Ok(Frame::Integer(1))
}

fn hget(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let hash = hash_ref(ctx.shard(&args[1]), &args[1], now)?;
    Ok(hash
        .and_then(|h| h.get(&args[2]))
        .map_or(Frame::Null, |v| Frame::Bulk(v.clone())))
}

fn hmget(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let hash = hash_ref(ctx.shard(&args[1]), &args[1], now)?;
    let items = args[2..]
        .iter()
        .map(|f| {
            hash.and_then(|h| h.get(f))
                .map_or(Frame::Null, |v| Frame::Bulk(v.clone()))
        })
        .collect();
    Ok(Frame::Array(items))
}

fn hdel(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let Some(hash) = hash_mut(ctx.shard(&args[1]), &args[1], now)? else {
        return Ok(Frame::Integer(0));
    };
    let removed = args[2..].iter().filter(|f| hash.remove(f)).count();
    if removed > 0 {
        ctx.propagate_args(args);
    }
    Ok(Frame::Integer(removed as i64))
}

fn hexists(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let found =
        hash_ref(ctx.shard(&args[1]), &args[1], now)?.is_some_and(|h| h.get(&args[2]).is_some());
    Ok(Frame::Integer(i64::from(found)))
}

fn hlen(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    Ok(Frame::Integer(
        hash_ref(ctx.shard(&args[1]), &args[1], now)?.map_or(0, |h| h.len()) as i64,
    ))
}

fn hgetall(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let pairs = hash_ref(ctx.shard(&args[1]), &args[1], now)?
        .map(|h| {
            h.iter()
                .map(|(k, v)| (Frame::Bulk(k.clone()), Frame::Bulk(v.clone())))
                .collect()
        })
        .unwrap_or_default();
    Ok(Frame::Map(pairs))
}

fn hkeys(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let keys = hash_ref(ctx.shard(&args[1]), &args[1], now)?
        .map(|h| h.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>());
    Ok(Frame::bulk_array(keys.unwrap_or_default()))
}

fn hvals(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let vals = hash_ref(ctx.shard(&args[1]), &args[1], now)?
        .map(|h| h.iter().map(|(_, v)| v.clone()).collect::<Vec<_>>());
    Ok(Frame::bulk_array(vals.unwrap_or_default()))
}

fn hincrby(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let delta = parse_i64(&args[3])?;
    let hash = hash_create(ctx.shard(&args[1]), &args[1], now)?;
    let current = match hash.get(&args[2]) {
        Some(v) => parse_i64(v).map_err(|_| CmdError::other("hash value is not an integer"))?,
        None => 0,
    };
    let next = current.checked_add(delta).ok_or(CmdError::Overflow)?;
    hash.insert(args[2].clone(), Bytes::from(next.to_string()));
    ctx.propagate_args(args);
    Ok(Frame::Integer(next))
}

fn hincrbyfloat(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let delta = parse_f64(&args[3])?;
    let hash = hash_create(ctx.shard(&args[1]), &args[1], now)?;
    let current = match hash.get(&args[2]) {
        Some(v) => parse_f64(v).map_err(|_| CmdError::other("hash value is not a float"))?,
        None => 0.0,
    };
    let next = current + delta;
    if !next.is_finite() {
        return Err(CmdError::other("increment would produce NaN or Infinity"));
    }
    let text = Bytes::from(format_double(next));
    hash.insert(args[2].clone(), text.clone());
    ctx.propagate(vec![
        Bytes::from_static(b"HSET"),
        args[1].clone(),
        args[2].clone(),
        text.clone(),
    ]);
    Ok(Frame::Bulk(text))
}
