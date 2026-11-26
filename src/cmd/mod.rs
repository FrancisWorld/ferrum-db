//! Command table, execution context and argument helpers.
//!
//! Every data command is a plain function `fn(&mut Ctx, &[Bytes]) -> Reply`
//! registered in a static table together with its arity, flags and *lock plan*: the
//! key positions the engine must lock before the handler runs. Handlers therefore
//! never lock anything themselves, and `EXEC` can run any queued handler under a
//! lock set that covers the whole keyspace.

mod access;
mod hashes;
mod keys;
mod lists;
mod server;
mod sets;
mod strings;
mod zsets;

use std::collections::HashMap;
use std::sync::OnceLock;

use bytes::Bytes;

use crate::db::{Locked, Shard};
use crate::engine::Engine;
use crate::error::{CmdError, Reply};
use crate::resp::{Frame, Protocol};

pub use access::{is as is_option, parse_f64, parse_i64, parse_score};
pub(crate) use keys::decode_cursor;

/// The command writes data and must be propagated to the AOF.
pub const WRITE: u8 = 1;
/// The command may allocate and is refused when `maxmemory` cannot be honoured.
pub const DENYOOM: u8 = 2;
/// The command cannot be queued inside `MULTI`.
pub const NO_TX: u8 = 4;

/// Which shards a command needs locked.
#[derive(Debug, Clone, Copy)]
pub enum LockPlan {
    /// No keyspace access.
    None,
    /// Keys at `first`, `first + step`, ... up to `last` (negative counts from the end).
    Keys {
        /// Index of the first key argument.
        first: usize,
        /// Index of the last key argument (negative: relative to the end).
        last: i32,
        /// Distance between key arguments.
        step: usize,
    },
    /// Every shard.
    All,
    /// The single shard encoded in the `SCAN` cursor.
    Scan,
}

/// Handler signature.
pub type Handler = fn(&mut Ctx<'_>, &[Bytes]) -> Reply;

/// A registered command.
pub struct Command {
    /// Upper-case name.
    pub name: &'static str,
    /// Exact argument count including the name, or `-n` for "at least n".
    pub arity: i32,
    /// `WRITE | DENYOOM | NO_TX` bit set.
    pub flags: u8,
    /// Keys to lock.
    pub lock: LockPlan,
    /// Implementation.
    pub handler: Handler,
}

impl Command {
    /// True if the arity check passes for `argc` arguments (name included).
    pub fn arity_ok(&self, argc: usize) -> bool {
        if self.arity >= 0 {
            argc == self.arity as usize
        } else {
            argc >= self.arity.unsigned_abs() as usize
        }
    }

    /// Key arguments of an invocation according to the lock plan.
    pub fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        match self.lock {
            LockPlan::Keys { first, last, step } => {
                let last = if last < 0 {
                    args.len() as i64 + i64::from(last)
                } else {
                    i64::from(last)
                };
                let mut out = Vec::new();
                let mut i = first;
                while (i as i64) <= last && i < args.len() {
                    out.push(args[i].as_ref());
                    i += step;
                }
                out
            }
            _ => Vec::new(),
        }
    }

    /// True for commands that modify data.
    pub fn is_write(&self) -> bool {
        self.flags & WRITE != 0
    }
}

/// Everything a handler may touch while it runs.
pub struct Ctx<'a> {
    /// Locked shards.
    pub locked: Locked<'a>,
    /// Command time in ms; `0` while replaying the AOF (nothing expires).
    pub now: u64,
    /// Protocol of the calling connection, for dialect-dependent replies.
    pub proto: Protocol,
    /// The owning engine, for server-level commands.
    pub engine: &'a Engine,
    pub(crate) effects: Vec<Vec<Bytes>>,
}

impl<'a> Ctx<'a> {
    pub(crate) fn new(engine: &'a Engine, locked: Locked<'a>, now: u64, proto: Protocol) -> Self {
        Ctx {
            locked,
            now,
            proto,
            engine,
            effects: Vec::new(),
        }
    }

    /// The shard owning `key`.
    pub fn shard(&mut self, key: &[u8]) -> &mut Shard {
        self.locked.shard(key)
    }

    /// Records a command to be appended to the AOF if it is enabled.
    pub fn propagate(&mut self, command: Vec<Bytes>) {
        self.effects.push(command);
    }

    /// Runs `cmd`, then settles memory accounting and prepends `DEL` effects for any
    /// key that expired while the command ran. Errors become error frames.
    pub(crate) fn run(&mut self, cmd: &Command, args: &[Bytes]) -> Frame {
        let mark = self.effects.len();
        let reply = (cmd.handler)(self, args);
        let removed = self.locked.settle();
        let dels = removed
            .into_iter()
            .map(|k| vec![Bytes::from_static(b"DEL"), k]);
        self.effects.splice(mark..mark, dels);
        reply.unwrap_or_else(Frame::from)
    }

    /// Records a command given as a verbatim copy of the invocation.
    pub fn propagate_args(&mut self, args: &[Bytes]) {
        self.effects.push(args.to_vec());
    }
}

fn registry() -> &'static HashMap<&'static str, &'static Command> {
    static TABLE: OnceLock<HashMap<&'static str, &'static Command>> = OnceLock::new();
    TABLE.get_or_init(|| {
        let groups: [&'static [Command]; 7] = [
            strings::COMMANDS,
            keys::COMMANDS,
            lists::COMMANDS,
            hashes::COMMANDS,
            sets::COMMANDS,
            zsets::COMMANDS,
            server::COMMANDS,
        ];
        groups.into_iter().flatten().map(|c| (c.name, c)).collect()
    })
}

/// Looks a command up by case-insensitive name.
pub fn lookup(name: &[u8]) -> Option<&'static Command> {
    let mut buf = [0u8; 24];
    let upper = buf.get_mut(..name.len())?;
    upper.copy_from_slice(name);
    upper.make_ascii_uppercase();
    registry().get(std::str::from_utf8(upper).ok()?).copied()
}

/// Number of registered data and server commands.
pub fn command_count() -> usize {
    registry().len()
}

/// Validates arity and returns the command, or the error to report to the client.
pub fn resolve(args: &[Bytes]) -> Result<&'static Command, CmdError> {
    let name = &args[0];
    let cmd = lookup(name).ok_or_else(|| {
        CmdError::UnknownCommand(String::from_utf8_lossy(name).chars().take(64).collect())
    })?;
    if cmd.arity_ok(args.len()) {
        Ok(cmd)
    } else {
        Err(CmdError::Arity(cmd.name.to_ascii_lowercase()))
    }
}
