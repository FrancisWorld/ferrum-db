//! Sorted-set commands.

use bytes::Bytes;

use super::access::{clamp_range, is, parse_i64, parse_score, zset_create, zset_mut, zset_ref};
use super::{Command, Ctx, DENYOOM, LockPlan, WRITE};
use crate::db::{Inserted, ScoreRange, ZSet};
use crate::error::{CmdError, Reply};
use crate::resp::{Frame, Protocol};

const ONE_KEY: LockPlan = LockPlan::Keys {
    first: 1,
    last: 1,
    step: 1,
};

pub(super) static COMMANDS: &[Command] = &[
    Command {
        name: "ZADD",
        arity: -4,
        flags: WRITE | DENYOOM,
        lock: ONE_KEY,
        handler: zadd,
    },
    Command {
        name: "ZINCRBY",
        arity: 4,
        flags: WRITE | DENYOOM,
        lock: ONE_KEY,
        handler: zincrby,
    },
    Command {
        name: "ZREM",
        arity: -3,
        flags: WRITE,
        lock: ONE_KEY,
        handler: zrem,
    },
    Command {
        name: "ZSCORE",
        arity: 3,
        flags: 0,
        lock: ONE_KEY,
        handler: zscore,
    },
    Command {
        name: "ZCARD",
        arity: 2,
        flags: 0,
        lock: ONE_KEY,
        handler: zcard,
    },
    Command {
        name: "ZRANK",
        arity: 3,
        flags: 0,
        lock: ONE_KEY,
        handler: zrank,
    },
    Command {
        name: "ZREVRANK",
        arity: 3,
        flags: 0,
        lock: ONE_KEY,
        handler: zrevrank,
    },
    Command {
        name: "ZRANGE",
        arity: -4,
        flags: 0,
        lock: ONE_KEY,
        handler: zrange,
    },
    Command {
        name: "ZREVRANGE",
        arity: -4,
        flags: 0,
        lock: ONE_KEY,
        handler: zrevrange,
    },
    Command {
        name: "ZRANGEBYSCORE",
        arity: -4,
        flags: 0,
        lock: ONE_KEY,
        handler: zrangebyscore,
    },
    Command {
        name: "ZREVRANGEBYSCORE",
        arity: -4,
        flags: 0,
        lock: ONE_KEY,
        handler: zrevrangebyscore,
    },
    Command {
        name: "ZCOUNT",
        arity: 4,
        flags: 0,
        lock: ONE_KEY,
        handler: zcount,
    },
    Command {
        name: "ZPOPMIN",
        arity: -2,
        flags: WRITE,
        lock: ONE_KEY,
        handler: zpopmin,
    },
    Command {
        name: "ZPOPMAX",
        arity: -2,
        flags: WRITE,
        lock: ONE_KEY,
        handler: zpopmax,
    },
    Command {
        name: "ZREMRANGEBYRANK",
        arity: 4,
        flags: WRITE,
        lock: ONE_KEY,
        handler: zremrangebyrank,
    },
    Command {
        name: "ZREMRANGEBYSCORE",
        arity: 4,
        flags: WRITE,
        lock: ONE_KEY,
        handler: zremrangebyscore,
    },
];

fn zadd(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let (mut nx, mut xx, mut gt, mut lt, mut ch, mut incr) =
        (false, false, false, false, false, false);
    let mut i = 2;
    while i < args.len() {
        match args[i].to_ascii_uppercase().as_slice() {
            b"NX" => nx = true,
            b"XX" => xx = true,
            b"GT" => gt = true,
            b"LT" => lt = true,
            b"CH" => ch = true,
            b"INCR" => incr = true,
            _ => break,
        }
        i += 1;
    }
    let pairs = &args[i..];
    if pairs.is_empty() || !pairs.len().is_multiple_of(2) {
        return Err(CmdError::Syntax);
    }
    if nx && xx {
        return Err(CmdError::other(
            "XX and NX options at the same time are not compatible",
        ));
    }
    if (gt && lt) || (nx && (gt || lt)) {
        return Err(CmdError::other(
            "GT, LT, and/or NX options at the same time are not compatible",
        ));
    }
    if incr && pairs.len() != 2 {
        return Err(CmdError::other(
            "INCR option supports a single increment-element pair",
        ));
    }
    let parsed: Vec<(f64, &Bytes)> = pairs
        .as_chunks::<2>()
        .0
        .iter()
        .map(|p| parse_score(&p[0]).map(|s| (s, &p[1])))
        .collect::<Result<_, _>>()?;

    let zset = zset_create(ctx.shard(&args[1]), &args[1], now)?;
    let (mut added, mut changed) = (0i64, 0i64);
    let mut incr_result: Option<f64> = None;
    let mut log: Vec<Bytes> = vec![Bytes::from_static(b"ZADD"), args[1].clone()];
    for (score, member) in parsed {
        let existing = zset.score(member);
        let target = if incr {
            existing.unwrap_or(0.0) + score
        } else {
            score
        };
        if target.is_nan() {
            return Err(CmdError::NanScore);
        }
        let skip = match existing {
            Some(old) => nx || (gt && target <= old) || (lt && target >= old),
            None => xx,
        };
        if skip {
            continue;
        }
        match zset.insert((*member).clone(), target) {
            Inserted::Added => {
                added += 1;
                changed += 1;
            }
            Inserted::Updated => changed += 1,
            Inserted::Unchanged => {}
        }
        incr_result = Some(target);
        log.push(Bytes::from(crate::resp::format_double(target)));
        log.push((*member).clone());
    }
    if log.len() > 2 {
        ctx.propagate(log);
    }
    if incr {
        return Ok(incr_result.map_or(Frame::Null, Frame::Double));
    }
    Ok(Frame::Integer(if ch { changed } else { added }))
}

fn zincrby(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let delta = parse_score(&args[2])?;
    let zset = zset_create(ctx.shard(&args[1]), &args[1], now)?;
    let next = zset.score(&args[3]).unwrap_or(0.0) + delta;
    if next.is_nan() {
        return Err(CmdError::NanScore);
    }
    zset.insert(args[3].clone(), next);
    ctx.propagate(vec![
        Bytes::from_static(b"ZADD"),
        args[1].clone(),
        Bytes::from(crate::resp::format_double(next)),
        args[3].clone(),
    ]);
    Ok(Frame::Double(next))
}

fn zrem(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let Some(zset) = zset_mut(ctx.shard(&args[1]), &args[1], now)? else {
        return Ok(Frame::Integer(0));
    };
    let removed = args[2..].iter().filter(|m| zset.remove(m)).count();
    if removed > 0 {
        ctx.propagate_args(args);
    }
    Ok(Frame::Integer(removed as i64))
}

fn zscore(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let score = zset_ref(ctx.shard(&args[1]), &args[1], now)?.and_then(|z| z.score(&args[2]));
    Ok(score.map_or(Frame::Null, Frame::Double))
}

fn zcard(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    Ok(Frame::Integer(
        zset_ref(ctx.shard(&args[1]), &args[1], now)?.map_or(0, ZSet::len) as i64,
    ))
}

fn rank(ctx: &mut Ctx, args: &[Bytes], rev: bool) -> Reply {
    let now = ctx.now;
    let r = zset_ref(ctx.shard(&args[1]), &args[1], now)?.and_then(|z| z.rank(&args[2], rev));
    Ok(r.map_or(Frame::Null, |r| Frame::Integer(r as i64)))
}

fn zrank(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    rank(ctx, args, false)
}

fn zrevrank(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    rank(ctx, args, true)
}

/// Formats members (and optionally scores) the way each protocol expects.
fn members_reply(items: Vec<(Bytes, f64)>, with_scores: bool, proto: Protocol) -> Frame {
    if !with_scores {
        return Frame::bulk_array(items.into_iter().map(|(m, _)| m));
    }
    match proto {
        Protocol::Resp3 => Frame::Array(
            items
                .into_iter()
                .map(|(m, s)| Frame::Array(vec![Frame::Bulk(m), Frame::Double(s)]))
                .collect(),
        ),
        Protocol::Resp2 => Frame::Array(
            items
                .into_iter()
                .flat_map(|(m, s)| [Frame::Bulk(m), Frame::Double(s)])
                .collect(),
        ),
    }
}

/// Parses a score bound: `(1.5` is exclusive, otherwise inclusive.
fn parse_bound(raw: &[u8]) -> Result<(f64, bool), CmdError> {
    let (exclusive, body) = match raw.split_first() {
        Some((b'(', rest)) => (true, rest),
        _ => (false, raw),
    };
    parse_score(body)
        .map(|v| (v, exclusive))
        .map_err(|_| CmdError::BadRange)
}

fn score_range(min: &[u8], max: &[u8]) -> Result<ScoreRange, CmdError> {
    let (min, min_exclusive) = parse_bound(min)?;
    let (max, max_exclusive) = parse_bound(max)?;
    Ok(ScoreRange {
        min,
        min_exclusive,
        max,
        max_exclusive,
    })
}

struct RangeOpts {
    with_scores: bool,
    limit: Option<(usize, Option<usize>)>,
}

fn parse_range_opts(opts: &[Bytes], allow_limit: bool) -> Result<RangeOpts, CmdError> {
    let mut out = RangeOpts {
        with_scores: false,
        limit: None,
    };
    let mut i = 0;
    while i < opts.len() {
        if is(&opts[i], "WITHSCORES") {
            out.with_scores = true;
        } else if allow_limit && is(&opts[i], "LIMIT") && i + 2 < opts.len() {
            let offset = usize::try_from(parse_i64(&opts[i + 1])?).map_err(|_| CmdError::Syntax)?;
            let count = parse_i64(&opts[i + 2])?;
            out.limit = Some((offset, usize::try_from(count).ok()));
            i += 2;
        } else {
            return Err(CmdError::Syntax);
        }
        i += 1;
    }
    Ok(out)
}

fn by_rank(
    ctx: &mut Ctx,
    key: &Bytes,
    start: &[u8],
    stop: &[u8],
    rev: bool,
    opts: &RangeOpts,
) -> Reply {
    let now = ctx.now;
    let proto = ctx.proto;
    let (start, stop) = (parse_i64(start)?, parse_i64(stop)?);
    let Some(zset) = zset_ref(ctx.shard(key), key, now)? else {
        return Ok(Frame::Array(vec![]));
    };
    let items = match clamp_range(start, stop, zset.len()) {
        Some((a, b)) => zset.range(a, b, rev),
        None => Vec::new(),
    };
    Ok(members_reply(items, opts.with_scores, proto))
}

fn by_score(ctx: &mut Ctx, key: &Bytes, range: ScoreRange, rev: bool, opts: &RangeOpts) -> Reply {
    let now = ctx.now;
    let proto = ctx.proto;
    let Some(zset) = zset_ref(ctx.shard(key), key, now)? else {
        return Ok(Frame::Array(vec![]));
    };
    let (offset, count) = opts.limit.unwrap_or((0, None));
    Ok(members_reply(
        zset.range_by_score(&range, rev, offset, count),
        opts.with_scores,
        proto,
    ))
}

fn zrange(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let (mut rev, mut score_mode) = (false, false);
    let mut rest = Vec::new();
    for opt in &args[4..] {
        if is(opt, "REV") {
            rev = true;
        } else if is(opt, "BYSCORE") {
            score_mode = true;
        } else {
            rest.push(opt.clone());
        }
    }
    let opts = parse_range_opts(&rest, score_mode)?;
    if score_mode {
        // With REV the bounds are given as `max min`.
        let range = if rev {
            score_range(&args[3], &args[2])?
        } else {
            score_range(&args[2], &args[3])?
        };
        by_score(ctx, &args[1], range, rev, &opts)
    } else {
        by_rank(ctx, &args[1], &args[2], &args[3], rev, &opts)
    }
}

fn zrevrange(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let opts = parse_range_opts(&args[4..], false)?;
    by_rank(ctx, &args[1], &args[2], &args[3], true, &opts)
}

fn zrangebyscore(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let opts = parse_range_opts(&args[4..], true)?;
    by_score(
        ctx,
        &args[1],
        score_range(&args[2], &args[3])?,
        false,
        &opts,
    )
}

fn zrevrangebyscore(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let opts = parse_range_opts(&args[4..], true)?;
    by_score(ctx, &args[1], score_range(&args[3], &args[2])?, true, &opts)
}

fn zcount(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let range = score_range(&args[2], &args[3])?;
    Ok(Frame::Integer(
        zset_ref(ctx.shard(&args[1]), &args[1], now)?.map_or(0, |z| z.count_in_range(&range))
            as i64,
    ))
}

fn pop(ctx: &mut Ctx, args: &[Bytes], highest: bool) -> Reply {
    let now = ctx.now;
    let proto = ctx.proto;
    if args.len() > 3 {
        return Err(CmdError::Syntax);
    }
    let count = match args.get(2) {
        Some(raw) => usize::try_from(parse_i64(raw)?).map_err(|_| CmdError::MustBePositive)?,
        None => 1,
    };
    let Some(zset) = zset_mut(ctx.shard(&args[1]), &args[1], now)? else {
        return Ok(Frame::Array(vec![]));
    };
    let popped = zset.pop(highest, count);
    if !popped.is_empty() {
        let mut log = vec![Bytes::from_static(b"ZREM"), args[1].clone()];
        log.extend(popped.iter().map(|(m, _)| m.clone()));
        ctx.propagate(log);
    }
    Ok(members_reply(popped, true, proto))
}

fn zpopmin(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    pop(ctx, args, false)
}

fn zpopmax(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    pop(ctx, args, true)
}

fn zremrangebyrank(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let (start, stop) = (parse_i64(&args[2])?, parse_i64(&args[3])?);
    let Some(zset) = zset_mut(ctx.shard(&args[1]), &args[1], now)? else {
        return Ok(Frame::Integer(0));
    };
    let removed =
        clamp_range(start, stop, zset.len()).map_or(0, |(a, b)| zset.remove_range_by_rank(a, b));
    if removed > 0 {
        ctx.propagate_args(args);
    }
    Ok(Frame::Integer(removed as i64))
}

fn zremrangebyscore(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let now = ctx.now;
    let range = score_range(&args[2], &args[3])?;
    let Some(zset) = zset_mut(ctx.shard(&args[1]), &args[1], now)? else {
        return Ok(Frame::Integer(0));
    };
    let removed = zset.remove_range_by_score(&range);
    if removed > 0 {
        ctx.propagate_args(args);
    }
    Ok(Frame::Integer(removed as i64))
}
