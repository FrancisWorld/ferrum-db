//! Server-level commands: PING, INFO, CONFIG, persistence control and PUBLISH.

use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;

use super::access::{is, parse_i64};
use super::{Command, Ctx, LockPlan, NO_TX};
use crate::error::{CmdError, Reply};
use crate::glob;
use crate::resp::Frame;

pub(super) static COMMANDS: &[Command] = &[
    Command {
        name: "PING",
        arity: -1,
        flags: 0,
        lock: LockPlan::None,
        handler: ping,
    },
    Command {
        name: "ECHO",
        arity: 2,
        flags: 0,
        lock: LockPlan::None,
        handler: echo,
    },
    Command {
        name: "SELECT",
        arity: 2,
        flags: 0,
        lock: LockPlan::None,
        handler: select,
    },
    Command {
        name: "TIME",
        arity: 1,
        flags: 0,
        lock: LockPlan::None,
        handler: time,
    },
    Command {
        name: "INFO",
        arity: -1,
        flags: 0,
        lock: LockPlan::None,
        handler: info,
    },
    Command {
        name: "CONFIG",
        arity: -2,
        flags: 0,
        lock: LockPlan::None,
        handler: config,
    },
    Command {
        name: "COMMAND",
        arity: -1,
        flags: 0,
        lock: LockPlan::None,
        handler: command,
    },
    Command {
        name: "SAVE",
        arity: 1,
        flags: NO_TX,
        lock: LockPlan::None,
        handler: save,
    },
    Command {
        name: "BGSAVE",
        arity: 1,
        flags: NO_TX,
        lock: LockPlan::None,
        handler: bgsave,
    },
    Command {
        name: "BGREWRITEAOF",
        arity: 1,
        flags: NO_TX,
        lock: LockPlan::None,
        handler: bgrewriteaof,
    },
    Command {
        name: "LASTSAVE",
        arity: 1,
        flags: 0,
        lock: LockPlan::None,
        handler: lastsave,
    },
    Command {
        name: "PUBLISH",
        arity: 3,
        flags: 0,
        lock: LockPlan::None,
        handler: publish,
    },
    Command {
        name: "PUBSUB",
        arity: -2,
        flags: 0,
        lock: LockPlan::None,
        handler: pubsub,
    },
];

fn ping(_ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    match args.len() {
        1 => Ok(Frame::simple("PONG")),
        2 => Ok(Frame::Bulk(args[1].clone())),
        _ => Err(CmdError::Arity("ping".into())),
    }
}

fn echo(_ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    Ok(Frame::Bulk(args[1].clone()))
}

fn select(_ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    match parse_i64(&args[1])? {
        0 => Ok(Frame::OK),
        _ => Err(CmdError::other("DB index is out of range")),
    }
}

fn time(_ctx: &mut Ctx, _args: &[Bytes]) -> Reply {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    Ok(Frame::Array(vec![
        Frame::bulk(d.as_secs().to_string()),
        Frame::bulk(d.subsec_micros().to_string()),
    ]))
}

fn info(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let section = args
        .get(1)
        .map(|s| String::from_utf8_lossy(s).to_ascii_lowercase());
    Ok(Frame::bulk(ctx.engine.info(section.as_deref())))
}

fn config(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    if is(&args[1], "GET") && args.len() == 3 {
        let pattern = &args[2];
        let pairs = ctx
            .engine
            .config_entries()
            .into_iter()
            .filter(|(k, _)| glob::matches(&pattern.to_ascii_lowercase(), k.as_bytes()))
            .map(|(k, v)| (Frame::bulk(k), Frame::bulk(v)))
            .collect();
        Ok(Frame::Map(pairs))
    } else if is(&args[1], "SET") && args.len() >= 4 && args.len().is_multiple_of(2) {
        for pair in args[2..].as_chunks::<2>().0 {
            ctx.engine.config_set(
                &String::from_utf8_lossy(&pair[0]).to_ascii_lowercase(),
                &String::from_utf8_lossy(&pair[1]),
            )?;
        }
        Ok(Frame::OK)
    } else {
        Err(CmdError::other(
            "unknown or malformed CONFIG subcommand (supported: GET pattern, SET param value)",
        ))
    }
}

fn command(_ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    match args.get(1) {
        None => Ok(Frame::Array(vec![])),
        Some(sub) if is(sub, "COUNT") => Ok(Frame::Integer(super::command_count() as i64)),
        // DOCS / INFO / LIST are only introspection niceties for client libraries.
        Some(_) => Ok(Frame::Array(vec![])),
    }
}

fn save(ctx: &mut Ctx, _args: &[Bytes]) -> Reply {
    ctx.engine.save()?;
    Ok(Frame::OK)
}

fn bgsave(ctx: &mut Ctx, _args: &[Bytes]) -> Reply {
    ctx.engine.bgsave()?;
    Ok(Frame::simple("Background saving started"))
}

fn bgrewriteaof(ctx: &mut Ctx, _args: &[Bytes]) -> Reply {
    ctx.engine.bgrewriteaof()?;
    Ok(Frame::simple(
        "Background append only file rewriting started",
    ))
}

fn lastsave(ctx: &mut Ctx, _args: &[Bytes]) -> Reply {
    Ok(Frame::Integer(ctx.engine.last_save_secs() as i64))
}

fn publish(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    Ok(Frame::Integer(
        ctx.engine.hub().publish(&args[1], &args[2]) as i64
    ))
}

fn pubsub(ctx: &mut Ctx, args: &[Bytes]) -> Reply {
    let hub = ctx.engine.hub();
    if is(&args[1], "CHANNELS") && args.len() <= 3 {
        let pattern = args.get(2).map(|p| p.as_ref());
        Ok(Frame::bulk_array(hub.channels(pattern)))
    } else if is(&args[1], "NUMSUB") {
        let items = args[2..]
            .iter()
            .flat_map(|c| [Frame::Bulk(c.clone()), Frame::Integer(hub.numsub(c) as i64)])
            .collect();
        Ok(Frame::Array(items))
    } else if is(&args[1], "NUMPAT") && args.len() == 2 {
        Ok(Frame::Integer(hub.numpat() as i64))
    } else {
        Err(CmdError::other(
            "unknown PUBSUB subcommand (supported: CHANNELS, NUMSUB, NUMPAT)",
        ))
    }
}
