//! Set commands.

use std::collections::HashSet;

use bytes::Bytes;

use super::access::{parse_i64, set_create, set_mut, set_ref};
use super::{Command, Ctx, DENYOOM, LockPlan, WRITE};
use crate::db::{SetVal, Value};
use crate::error::{CmdError, Reply};
use crate::resp::Frame;

const ONE_KEY: LockPlan = LockPlan::Keys {
    first: 1,
    last: 1,
    step: 1,
};
const ALL_KEYS: LockPlan = LockPlan::Keys {
    first: 1,
    last: -1,
    step: 1,
};
const STORE_KEYS: LockPlan = LockPlan::Keys {
    first: 1,
    last: -1,
    step: 1,
};

pub(super) static COMMANDS: &[Command] = &[
    Command {
        name: "SADD",
        arity: -3,
        flags: WRITE | DENYOOM,
        lock: ONE_KEY,
        handler: sadd,
    },
    Command {
        name: "SREM",
        arity: -3,
        flags: WRITE,
        lock: ONE_KEY,
        handler: srem,
    },
    Command {
        name: "SISMEMBER",
        arity: 3,
        flags: 0,
        lock: ONE_KEY,
        handler: sismember,
    },
    Command {
        name: "SMISMEMBER",
        arity: -3,
        flags: 0,
        lock: ONE_KEY,
        handler: smismember,
    },
    Command {
        name: "SMEMBERS",
        arity: 2,
        flags: 0,
        lock: ONE_KEY,
        handler: smembers,
    },
    Command {
        name: "SCARD",
        arity: 2,
        flags: 0,
        lock: ONE_KEY,
        handler: scard,
    },
    Command {
        name: "SPOP",
        arity: -2,
        flags: WRITE,
        lock: ONE_KEY,
        handler: spop,
    },
    Command {
        name: "SRANDMEMBER",
        arity: -2,
        flags: 0,
        lock: ONE_KEY,
        handler: srandmember,
    },
    Command {
        name: "SMOVE",
        arity: 4,
        flags: WRITE | DENYOOM,
        lock: LockPlan::Keys {
            first: 1,
            last: 2,
            step: 1,
        },
        handler: smove,
    },
    Command {
        name: "SUNION",
        arity: -2,
        flags: 0,
        lock: ALL_KEYS,
        handler: sunion,
    },
    Command {
        name: "SINTER",
        arity: -2,
        flags: 0,
        lock: ALL_KEYS,
        handler: sinter,
    },
    Command {
        name: "SDIFF",
        arity: -2,
        flags: 0,
        lock: ALL_KEYS,
        handler: sdiff,
    },
    Command {
        name: "SUNIONSTORE",
        arity: -3,
        flags: WRITE | DENYOOM,
        lock: STORE_KEYS,
        handler: sunionstore,
    },
    Command {
        name: "SINTERSTORE",
        arity: -3,
        flags: WRITE | DENYOOM,
        lock: STORE_KEYS,
        handler: sinterstore,
    },
    Command {
        name: "SDIFFSTORE",
        arity: -3,
        flags: WRITE | DENYOOM,
        lock: STORE_KEYS,
        handler: sdiffstore,
    },
];

fn sadd(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let set = set_create(ctx.shard(&args[1]), &args[1], now)?;
    let added = args[2..]
        .iter()
        .filter(|m| set.insert((*m).clone()))
        .count();
    if added > 0 {
        ctx.propagate_args(args);
    }
    Ok(Frame::Integer(added as i64))
}

fn srem(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let Some(set) = set_mut(ctx.shard(&args[1]), &args[1], now)? else {
        return Ok(Frame::Integer(0));
    };
    let removed = args[2..].iter().filter(|m| set.remove(m)).count();
    if removed > 0 {
        ctx.propagate_args(args);
    }
    Ok(Frame::Integer(removed as i64))
}

fn sismember(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let hit = set_ref(ctx.shard(&args[1]), &args[1], now)?.is_some_and(|s| s.contains(&args[2]));
    Ok(Frame::Integer(i64::from(hit)))
}

fn smismember(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let set = set_ref(ctx.shard(&args[1]), &args[1], now)?;
    Ok(Frame::Array(
        args[2..]
            .iter()
            .map(|m| Frame::Integer(i64::from(set.is_some_and(|s| s.contains(m)))))
            .collect(),
    ))
}

fn smembers(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let items = set_ref(ctx.shard(&args[1]), &args[1], now)?
        .map(|s| s.iter().cloned().map(Frame::Bulk).collect())
        .unwrap_or_default();
    Ok(Frame::Set(items))
}

fn scard(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    Ok(Frame::Integer(
        set_ref(ctx.shard(&args[1]), &args[1], now)?.map_or(0, SetVal::len) as i64,
    ))
}

fn spop(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    if args.len() > 3 {
        return Err(CmdError::Syntax);
    }
    let count = match args.get(2) {
        Some(raw) => Some(usize::try_from(parse_i64(raw)?).map_err(|_| CmdError::MustBePositive)?),
        None => None,
    };
    let Some(set) = set_mut(ctx.shard(&args[1]), &args[1], now)? else {
        return Ok(if count.is_some() {
            Frame::Set(vec![])
        } else {
            Frame::Null
        });
    };
    let mut popped = Vec::new();
    for _ in 0..count.unwrap_or(1).min(set.len()) {
        let Some(m) = set.get_index(fastrand::usize(..set.len())).cloned() else {
            break;
        };
        set.remove(&m);
        popped.push(m);
    }
    if !popped.is_empty() {
        let mut log = vec![Bytes::from_static(b"SREM"), args[1].clone()];
        log.extend(popped.iter().cloned());
        ctx.propagate(log);
    }
    Ok(match count {
        None => popped.into_iter().next().map_or(Frame::Null, Frame::Bulk),
        Some(_) => Frame::Set(popped.into_iter().map(Frame::Bulk).collect()),
    })
}

fn srandmember(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    if args.len() > 3 {
        return Err(CmdError::Syntax);
    }
    let count = args.get(2).map(|raw| parse_i64(raw)).transpose()?;
    let Some(set) = set_ref(ctx.shard(&args[1]), &args[1], now)? else {
        return Ok(if count.is_some() {
            Frame::Array(vec![])
        } else {
            Frame::Null
        });
    };
    let pick = |s: &SetVal| s.get_index(fastrand::usize(..s.len())).cloned();
    Ok(match count {
        None => pick(set).map_or(Frame::Null, Frame::Bulk),
        // Negative count: that many picks, repeats allowed.
        Some(n) if n < 0 => Frame::bulk_array((0..n.unsigned_abs()).filter_map(|_| pick(set))),
        Some(n) => {
            let mut all: Vec<Bytes> = set.iter().cloned().collect();
            fastrand::shuffle(&mut all);
            all.truncate(n as usize);
            Frame::bulk_array(all)
        }
    })
}

fn smove(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let (src, dst, member) = (&args[1], &args[2], &args[3]);
    if let Some(e) = ctx.shard(dst).get(dst, now)
        && !matches!(e.value, Value::Set(_))
    {
        return Err(CmdError::WrongType);
    }
    let Some(set) = set_mut(ctx.shard(src), src, now)? else {
        return Ok(Frame::Integer(0));
    };
    if !set.remove(member) {
        return Ok(Frame::Integer(0));
    }
    set_create(ctx.shard(dst), dst, now)?.insert(member.clone());
    ctx.propagate_args(args);
    Ok(Frame::Integer(1))
}

#[derive(Clone, Copy)]
enum Op {
    Union,
    Inter,
    Diff,
}

/// Reads every key as a set (missing keys are empty) and combines them.
fn combine(ctx: &mut Ctx, keys: &[Bytes], op: Op) -> Result<Vec<Bytes>, CmdError> {
    let now = ctx.now;
    let mut sets: Vec<HashSet<Bytes>> = Vec::with_capacity(keys.len());
    for key in keys {
        let members = set_ref(ctx.shard(key), key, now)?
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default();
        sets.push(members);
    }
    let mut rest = sets.split_off(1);
    let first = sets.pop().unwrap_or_default();
    Ok(match op {
        Op::Union => {
            let mut all = first;
            rest.iter().for_each(|s| all.extend(s.iter().cloned()));
            all.into_iter().collect()
        }
        Op::Inter => {
            rest.sort_by_key(HashSet::len);
            first
                .into_iter()
                .filter(|m| rest.iter().all(|s| s.contains(m)))
                .collect()
        }
        Op::Diff => first
            .into_iter()
            .filter(|m| !rest.iter().any(|s| s.contains(m)))
            .collect(),
    })
}

fn algebra(ctx: &mut Ctx, args: &[Bytes], op: Op) -> Reply {
    Ok(Frame::Set(
        combine(ctx, &args[1..], op)?
            .into_iter()
            .map(Frame::Bulk)
            .collect(),
    ))
}

fn algebra_store(ctx: &mut Ctx, args: &[Bytes], op: Op) -> Reply {
    let now = ctx.now;
    let members = combine(ctx, &args[2..], op)?;
    let dst = &args[1];
    let shard = ctx.shard(dst);
    shard.remove(dst, now);
    let n = members.len();
    if n > 0 {
        let mut set = SetVal::default();
        members.into_iter().for_each(|m| {
            set.insert(m);
        });
        shard.insert(dst.clone(), Value::Set(set), None, now);
    }
    ctx.propagate_args(args);
    Ok(Frame::Integer(n as i64))
}

fn sunion(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    algebra(ctx, args, Op::Union)
}

fn sinter(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    algebra(ctx, args, Op::Inter)
}

fn sdiff(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    algebra(ctx, args, Op::Diff)
}

fn sunionstore(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    algebra_store(ctx, args, Op::Union)
}

fn sinterstore(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    algebra_store(ctx, args, Op::Inter)
}

fn sdiffstore(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    algebra_store(ctx, args, Op::Diff)
}
