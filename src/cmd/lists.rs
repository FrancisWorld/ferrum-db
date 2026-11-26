//! List commands.

use bytes::Bytes;

use super::access::{clamp_range, is, list_create, list_mut, list_ref, parse_i64};
use super::{Command, Ctx, DENYOOM, LockPlan, WRITE};
use crate::db::Value;
use crate::error::{CmdError, Reply};
use crate::resp::Frame;

const ONE_KEY: LockPlan = LockPlan::Keys {
    first: 1,
    last: 1,
    step: 1,
};

pub(super) static COMMANDS: &[Command] = &[
    Command {
        name: "LPUSH",
        arity: -3,
        flags: WRITE | DENYOOM,
        lock: ONE_KEY,
        handler: lpush,
    },
    Command {
        name: "RPUSH",
        arity: -3,
        flags: WRITE | DENYOOM,
        lock: ONE_KEY,
        handler: rpush,
    },
    Command {
        name: "LPUSHX",
        arity: -3,
        flags: WRITE | DENYOOM,
        lock: ONE_KEY,
        handler: lpushx,
    },
    Command {
        name: "RPUSHX",
        arity: -3,
        flags: WRITE | DENYOOM,
        lock: ONE_KEY,
        handler: rpushx,
    },
    Command {
        name: "LPOP",
        arity: -2,
        flags: WRITE,
        lock: ONE_KEY,
        handler: lpop,
    },
    Command {
        name: "RPOP",
        arity: -2,
        flags: WRITE,
        lock: ONE_KEY,
        handler: rpop,
    },
    Command {
        name: "LLEN",
        arity: 2,
        flags: 0,
        lock: ONE_KEY,
        handler: llen,
    },
    Command {
        name: "LRANGE",
        arity: 4,
        flags: 0,
        lock: ONE_KEY,
        handler: lrange,
    },
    Command {
        name: "LINDEX",
        arity: 3,
        flags: 0,
        lock: ONE_KEY,
        handler: lindex,
    },
    Command {
        name: "LSET",
        arity: 4,
        flags: WRITE | DENYOOM,
        lock: ONE_KEY,
        handler: lset,
    },
    Command {
        name: "LTRIM",
        arity: 4,
        flags: WRITE,
        lock: ONE_KEY,
        handler: ltrim,
    },
    Command {
        name: "LREM",
        arity: 4,
        flags: WRITE,
        lock: ONE_KEY,
        handler: lrem,
    },
    Command {
        name: "LMOVE",
        arity: 5,
        flags: WRITE | DENYOOM,
        lock: LockPlan::Keys {
            first: 1,
            last: 2,
            step: 1,
        },
        handler: lmove,
    },
];

fn push(ctx: &mut Ctx, args: &[Bytes], front: bool, only_existing: bool) -> Reply {
    let now = ctx.now;
    let key = &args[1];
    let shard = ctx.shard(key);
    if only_existing && shard.get(key, now).is_none() {
        return Ok(Frame::Integer(0));
    }
    let list = list_create(shard, key, now)?;
    for v in &args[2..] {
        if front {
            list.push_front(v.clone())
        } else {
            list.push_back(v.clone())
        }
    }
    let len = list.len();
    ctx.propagate_args(args);
    Ok(Frame::Integer(len as i64))
}

fn lpush(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    push(ctx, args, true, false)
}

fn rpush(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    push(ctx, args, false, false)
}

fn lpushx(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    push(ctx, args, true, true)
}

fn rpushx(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    push(ctx, args, false, true)
}

fn pop(ctx: &mut Ctx, args: &[Bytes], front: bool) -> Reply {
    let now = ctx.now;
    if args.len() > 3 {
        return Err(CmdError::Arity(
            String::from_utf8_lossy(&args[0]).to_ascii_lowercase(),
        ));
    }
    let count = match args.get(2) {
        Some(raw) => Some(usize::try_from(parse_i64(raw)?).map_err(|_| CmdError::MustBePositive)?),
        None => None,
    };
    let Some(list) = list_mut(ctx.shard(&args[1]), &args[1], now)? else {
        return Ok(if count.is_some() {
            Frame::NullArray
        } else {
            Frame::Null
        });
    };
    let take = count.unwrap_or(1).min(list.len());
    let popped: Vec<Bytes> = (0..take)
        .filter_map(|_| {
            if front {
                list.pop_front()
            } else {
                list.pop_back()
            }
        })
        .collect();
    if !popped.is_empty() {
        ctx.propagate_args(args);
    }
    Ok(match count {
        None => popped.into_iter().next().map_or(Frame::Null, Frame::Bulk),
        Some(_) => Frame::bulk_array(popped),
    })
}

fn lpop(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    pop(ctx, args, true)
}

fn rpop(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    pop(ctx, args, false)
}

fn llen(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    Ok(Frame::Integer(
        list_ref(ctx.shard(&args[1]), &args[1], now)?.map_or(0, |l| l.len()) as i64,
    ))
}

fn lrange(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let (start, stop) = (parse_i64(&args[2])?, parse_i64(&args[3])?);
    let Some(list) = list_ref(ctx.shard(&args[1]), &args[1], now)? else {
        return Ok(Frame::Array(vec![]));
    };
    Ok(match clamp_range(start, stop, list.len()) {
        Some((a, b)) => Frame::bulk_array(list.items().range(a..=b).cloned()),
        None => Frame::Array(vec![]),
    })
}

fn resolve_index(idx: i64, len: usize) -> Option<usize> {
    let i = if idx < 0 { len as i64 + idx } else { idx };
    (0..len as i64).contains(&i).then_some(i as usize)
}

fn lindex(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let idx = parse_i64(&args[2])?;
    let Some(list) = list_ref(ctx.shard(&args[1]), &args[1], now)? else {
        return Ok(Frame::Null);
    };
    Ok(resolve_index(idx, list.len())
        .and_then(|i| list.items().get(i))
        .map_or(Frame::Null, |b| Frame::Bulk(b.clone())))
}

fn lset(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let idx = parse_i64(&args[2])?;
    let Some(list) = list_mut(ctx.shard(&args[1]), &args[1], now)? else {
        return Err(CmdError::NoSuchKey);
    };
    let i = resolve_index(idx, list.len()).ok_or(CmdError::IndexOutOfRange)?;
    list.set(i, args[3].clone());
    ctx.propagate_args(args);
    Ok(Frame::OK)
}

fn ltrim(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let (start, stop) = (parse_i64(&args[2])?, parse_i64(&args[3])?);
    let Some(list) = list_mut(ctx.shard(&args[1]), &args[1], now)? else {
        return Ok(Frame::OK);
    };
    match clamp_range(start, stop, list.len()) {
        Some((a, b)) => list.retain_range(a, b),
        None => list.retain_range(list.len(), list.len()),
    }
    ctx.propagate_args(args);
    Ok(Frame::OK)
}

fn lrem(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let count = parse_i64(&args[2])?;
    let Some(list) = list_mut(ctx.shard(&args[1]), &args[1], now)? else {
        return Ok(Frame::Integer(0));
    };
    let removed = list.remove_matching(count, &args[3]);
    if removed > 0 {
        ctx.propagate_args(args);
    }
    Ok(Frame::Integer(removed as i64))
}

fn lmove(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let side = |arg: &Bytes| -> Result<bool, CmdError> {
        if is(arg, "LEFT") {
            Ok(true)
        } else if is(arg, "RIGHT") {
            Ok(false)
        } else {
            Err(CmdError::Syntax)
        }
    };
    let (from_left, to_left) = (side(&args[3])?, side(&args[4])?);
    let (src, dst) = (&args[1], &args[2]);
    // Validate the destination type before popping so a failure cannot lose data.
    if let Some(e) = ctx.shard(dst).get(dst, now)
        && !matches!(e.value, Value::List(_))
    {
        return Err(CmdError::WrongType);
    }
    let Some(list) = list_mut(ctx.shard(src), src, now)? else {
        return Ok(Frame::Null);
    };
    let Some(item) = (if from_left {
        list.pop_front()
    } else {
        list.pop_back()
    }) else {
        return Ok(Frame::Null);
    };
    let target = list_create(ctx.shard(dst), dst, now)?;
    if to_left {
        target.push_front(item.clone())
    } else {
        target.push_back(item.clone())
    }
    ctx.propagate_args(args);
    Ok(Frame::Bulk(item))
}
