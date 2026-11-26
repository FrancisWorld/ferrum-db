//! Per-connection state and the connection-level commands (`MULTI`, `WATCH`,
//! `SUBSCRIBE`, `HELLO`, `AUTH`, ...) that cannot be expressed as keyspace handlers.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::{Bytes, BytesMut};
use tokio::sync::mpsc::UnboundedReceiver;

use crate::cmd::{self, NO_TX};
use crate::engine::Engine;
use crate::error::CmdError;
use crate::pubsub::{Push, Subscriber};
use crate::resp::{Frame, Protocol, encode};

/// Maximum commands queued between `MULTI` and `EXEC`.
const MAX_QUEUED: usize = 100_000;

struct Tx {
    queue: Vec<Vec<Bytes>>,
    /// A queued command was rejected; `EXEC` must refuse the whole block.
    dirty: bool,
}

/// Bytes to send to the client plus bookkeeping the connection loop needs.
#[derive(Default)]
pub struct Output {
    /// Encoded replies.
    pub buf: BytesMut,
    /// Highest AOF sequence the client must wait for (policy `always`).
    pub sync_seq: Option<u64>,
    /// The connection should be closed after flushing.
    pub close: bool,
}

/// State of one client connection.
pub struct Session {
    id: u64,
    proto: Protocol,
    authed: bool,
    name: Option<Bytes>,
    tx: Option<Tx>,
    watched: Vec<(Bytes, u64)>,
    channels: HashSet<Bytes>,
    patterns: HashSet<Bytes>,
    subscriber: Arc<Subscriber>,
    pushes: UnboundedReceiver<Push>,
}

impl Session {
    /// Creates the state for a freshly accepted connection.
    pub fn new(engine: &Engine) -> Session {
        let id = engine.next_client_id();
        let (subscriber, pushes) = Subscriber::new(id, engine.config().pubsub_buffer_limit);
        Session {
            id,
            proto: Protocol::Resp2,
            authed: engine.config().requirepass.is_none(),
            name: None,
            tx: None,
            watched: Vec::new(),
            channels: HashSet::new(),
            patterns: HashSet::new(),
            subscriber,
            pushes,
        }
    }

    /// Protocol negotiated with `HELLO`.
    pub fn protocol(&self) -> Protocol {
        self.proto
    }

    /// True while the connection holds at least one subscription.
    pub fn is_subscribed(&self) -> bool {
        !self.channels.is_empty() || !self.patterns.is_empty()
    }

    /// The subscriber handle (to watch for slow-consumer kicks).
    pub fn subscriber(&self) -> &Arc<Subscriber> {
        &self.subscriber
    }

    /// Waits for the next pub/sub message addressed to this connection.
    pub async fn next_push(&mut self) -> Option<Push> {
        let push = self.pushes.recv().await?;
        self.subscriber.consumed(&push);
        Some(push)
    }

    /// Returns a queued pub/sub message without waiting.
    pub fn try_next_push(&mut self) -> Option<Push> {
        let push = self.pushes.try_recv().ok()?;
        self.subscriber.consumed(&push);
        Some(push)
    }

    /// Encodes a pub/sub delivery for this connection's protocol.
    pub fn encode_push(&self, push: Push, out: &mut BytesMut) {
        let frame = match push {
            Push::Message { channel, payload } => {
                vec![
                    Frame::bulk("message"),
                    Frame::Bulk(channel),
                    Frame::Bulk(payload),
                ]
            }
            Push::PMessage {
                pattern,
                channel,
                payload,
            } => {
                vec![
                    Frame::bulk("pmessage"),
                    Frame::Bulk(pattern),
                    Frame::Bulk(channel),
                    Frame::Bulk(payload),
                ]
            }
        };
        encode(&Frame::Push(frame), self.proto, out);
    }

    /// Releases everything this connection registered (watches, subscriptions).
    pub fn close(&mut self, engine: &Engine) {
        engine.unwatch(&std::mem::take(&mut self.watched));
        engine.hub().remove_subscriber(
            &self.subscriber,
            self.channels.iter(),
            self.patterns.iter(),
        );
        self.channels.clear();
        self.patterns.clear();
    }

    fn reply(&self, frame: &Frame, out: &mut Output) {
        encode(frame, self.proto, &mut out.buf);
    }

    fn error(&self, e: CmdError, out: &mut Output) {
        self.reply(&Frame::from(e), out);
    }

    fn subscription_count(&self) -> i64 {
        (self.channels.len() + self.patterns.len()) as i64
    }

    /// Executes one client command and appends its reply to `out`.
    pub fn handle(&mut self, engine: &Engine, args: Vec<Bytes>, out: &mut Output) {
        engine.stats().commands.fetch_add(1, Ordering::Relaxed);
        let name = args[0].to_ascii_uppercase();
        let name = name.as_slice();

        if !self.authed && !matches!(name, b"AUTH" | b"HELLO" | b"QUIT") {
            return self.error(CmdError::Raw("NOAUTH Authentication required.".into()), out);
        }
        if self.proto == Protocol::Resp2
            && self.is_subscribed()
            && !matches!(
                name,
                b"SUBSCRIBE"
                    | b"UNSUBSCRIBE"
                    | b"PSUBSCRIBE"
                    | b"PUNSUBSCRIBE"
                    | b"PING"
                    | b"QUIT"
                    | b"RESET"
            )
        {
            let msg = format!(
                "Can't execute '{}': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
                String::from_utf8_lossy(&args[0]).to_ascii_lowercase()
            );
            return self.error(CmdError::other(msg), out);
        }

        match name {
            b"AUTH" => self.auth(engine, &args, out),
            b"HELLO" => self.hello(engine, &args, out),
            b"QUIT" => {
                self.reply(&Frame::OK, out);
                out.close = true;
            }
            b"RESET" => self.reset(engine, out),
            b"CLIENT" => self.client(&args, out),
            b"MULTI" => self.multi(&args, out),
            b"DISCARD" => self.discard(engine, &args, out),
            b"EXEC" => self.exec(engine, &args, out),
            b"WATCH" => self.watch(engine, &args, out),
            b"UNWATCH" => {
                engine.unwatch(&std::mem::take(&mut self.watched));
                self.reply(&Frame::OK, out);
            }
            b"SUBSCRIBE" | b"PSUBSCRIBE" if args.len() < 2 => {
                let e = CmdError::Arity(String::from_utf8_lossy(&args[0]).to_ascii_lowercase());
                self.error(e, out);
            }
            b"SUBSCRIBE" | b"PSUBSCRIBE" => {
                self.subscribe(engine, name == b"PSUBSCRIBE", &args, out)
            }
            b"UNSUBSCRIBE" | b"PUNSUBSCRIBE" => {
                self.unsubscribe(engine, name == b"PUNSUBSCRIBE", &args, out)
            }
            b"PING" if self.proto == Protocol::Resp2 && self.is_subscribed() => {
                let payload = args.get(1).cloned().unwrap_or_default();
                self.reply(
                    &Frame::Array(vec![Frame::bulk("pong"), Frame::Bulk(payload)]),
                    out,
                );
            }
            _ => self.run_data_command(engine, args, out),
        }
    }

    fn run_data_command(&mut self, engine: &Engine, args: Vec<Bytes>, out: &mut Output) {
        let cmd = match cmd::resolve(&args) {
            Ok(c) => c,
            Err(e) => {
                if let Some(tx) = self.tx.as_mut() {
                    tx.dirty = true;
                }
                return self.error(e, out);
            }
        };
        if let Some(tx) = self.tx.as_mut() {
            if cmd.flags & NO_TX != 0 {
                tx.dirty = true;
                let msg = format!("{} is not allowed inside MULTI", cmd.name);
                return self.error(CmdError::other(msg), out);
            }
            if tx.queue.len() >= MAX_QUEUED {
                tx.dirty = true;
                return self.error(
                    CmdError::other("too many queued commands in transaction"),
                    out,
                );
            }
            tx.queue.push(args);
            return self.reply(&Frame::simple("QUEUED"), out);
        }
        let (frame, seq) = engine.execute(cmd, &args, self.proto);
        self.reply(&frame, out);
        out.sync_seq = out.sync_seq.max(seq);
    }

    fn auth(&mut self, engine: &Engine, args: &[Bytes], out: &mut Output) {
        let (user, pass) = match args.len() {
            2 => (None, &args[1]),
            3 => (Some(&args[1]), &args[2]),
            _ => return self.error(CmdError::Arity("auth".into()), out),
        };
        match self.try_auth(engine, user, pass) {
            Ok(()) => self.reply(&Frame::OK, out),
            Err(e) => self.error(e, out),
        }
    }

    fn try_auth(
        &mut self,
        engine: &Engine,
        user: Option<&Bytes>,
        pass: &[u8],
    ) -> Result<(), CmdError> {
        let Some(expected) = engine.config().requirepass.as_deref() else {
            return Err(CmdError::other(
                "AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?",
            ));
        };
        let user_ok = user.is_none_or(|u| u.as_ref() == b"default");
        let pass_ok = constant_time_eq(expected.as_bytes(), pass);
        if user_ok & pass_ok {
            self.authed = true;
            Ok(())
        } else {
            Err(CmdError::Raw(
                "WRONGPASS invalid username-password pair or user is disabled.".into(),
            ))
        }
    }

    fn hello(&mut self, engine: &Engine, args: &[Bytes], out: &mut Output) {
        let mut proto = self.proto;
        let mut i = 1;
        if let Some(raw) = args.get(1) {
            proto = match raw.as_ref() {
                b"2" => Protocol::Resp2,
                b"3" => Protocol::Resp3,
                _ => {
                    let e = CmdError::Raw("NOPROTO unsupported protocol version".into());
                    return self.error(e, out);
                }
            };
            i = 2;
        }
        let mut name = None;
        while i < args.len() {
            if cmd::is_option(&args[i], "AUTH") && i + 2 < args.len() {
                if let Err(e) = self.try_auth(engine, Some(&args[i + 1]), &args[i + 2]) {
                    return self.error(e, out);
                }
                i += 3;
            } else if cmd::is_option(&args[i], "SETNAME") && i + 1 < args.len() {
                name = Some(args[i + 1].clone());
                i += 2;
            } else {
                return self.error(CmdError::Syntax, out);
            }
        }
        if !self.authed {
            return self.error(CmdError::Raw("NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time".into()), out);
        }
        self.proto = proto;
        if name.is_some() {
            self.name = name;
        }
        let info = Frame::Map(vec![
            (Frame::bulk("server"), Frame::bulk("ferrum-db")),
            (
                Frame::bulk("version"),
                Frame::bulk(env!("CARGO_PKG_VERSION")),
            ),
            (
                Frame::bulk("proto"),
                Frame::Integer(if proto == Protocol::Resp3 { 3 } else { 2 }),
            ),
            (Frame::bulk("id"), Frame::Integer(self.id as i64)),
            (Frame::bulk("mode"), Frame::bulk("standalone")),
            (Frame::bulk("role"), Frame::bulk("master")),
            (Frame::bulk("modules"), Frame::Array(vec![])),
        ]);
        self.reply(&info, out);
    }

    fn reset(&mut self, engine: &Engine, out: &mut Output) {
        self.close(engine);
        self.tx = None;
        self.proto = Protocol::Resp2;
        self.name = None;
        self.authed = engine.config().requirepass.is_none();
        self.reply(&Frame::simple("RESET"), out);
    }

    fn client(&mut self, args: &[Bytes], out: &mut Output) {
        let sub = args[1].to_ascii_uppercase();
        match (sub.as_slice(), args.len()) {
            (b"ID", 2) => self.reply(&Frame::Integer(self.id as i64), out),
            (b"GETNAME", 2) => {
                let reply = self.name.clone().map_or(Frame::Null, Frame::Bulk);
                self.reply(&reply, out);
            }
            (b"SETNAME", 3) => {
                self.name = Some(args[2].clone());
                self.reply(&Frame::OK, out);
            }
            // Client libraries announce themselves with CLIENT SETINFO; accepted and ignored.
            (b"SETINFO", 4) => self.reply(&Frame::OK, out),
            _ => self.error(
                CmdError::other("unsupported CLIENT subcommand (ID, GETNAME, SETNAME, SETINFO)"),
                out,
            ),
        }
    }

    fn multi(&mut self, args: &[Bytes], out: &mut Output) {
        if args.len() != 1 {
            return self.error(CmdError::Arity("multi".into()), out);
        }
        if self.tx.is_some() {
            return self.error(CmdError::other("MULTI calls can not be nested"), out);
        }
        self.tx = Some(Tx {
            queue: Vec::new(),
            dirty: false,
        });
        self.reply(&Frame::OK, out);
    }

    fn discard(&mut self, engine: &Engine, args: &[Bytes], out: &mut Output) {
        if args.len() != 1 {
            return self.error(CmdError::Arity("discard".into()), out);
        }
        if self.tx.take().is_none() {
            return self.error(CmdError::other("DISCARD without MULTI"), out);
        }
        engine.unwatch(&std::mem::take(&mut self.watched));
        self.reply(&Frame::OK, out);
    }

    fn exec(&mut self, engine: &Engine, args: &[Bytes], out: &mut Output) {
        if args.len() != 1 {
            return self.error(CmdError::Arity("exec".into()), out);
        }
        let Some(tx) = self.tx.take() else {
            return self.error(CmdError::other("EXEC without MULTI"), out);
        };
        let watched = std::mem::take(&mut self.watched);
        if tx.dirty {
            engine.unwatch(&watched);
            return self.error(
                CmdError::Raw("EXECABORT Transaction discarded because of previous errors.".into()),
                out,
            );
        }
        let (frame, seq) = engine.execute_transaction(&tx.queue, &watched, self.proto);
        self.reply(&frame, out);
        out.sync_seq = out.sync_seq.max(seq);
    }

    fn watch(&mut self, engine: &Engine, args: &[Bytes], out: &mut Output) {
        if args.len() < 2 {
            return self.error(CmdError::Arity("watch".into()), out);
        }
        if self.tx.is_some() {
            return self.error(CmdError::other("WATCH inside MULTI is not allowed"), out);
        }
        for key in &args[1..] {
            let version = engine.watch(key);
            self.watched.push((key.clone(), version));
        }
        self.reply(&Frame::OK, out);
    }

    fn subscribe(&mut self, engine: &Engine, pattern: bool, args: &[Bytes], out: &mut Output) {
        let kind = if pattern { "psubscribe" } else { "subscribe" };
        for target in &args[1..] {
            if pattern {
                self.patterns.insert(target.clone());
                engine.hub().psubscribe(&self.subscriber, target);
            } else {
                self.channels.insert(target.clone());
                engine.hub().subscribe(&self.subscriber, target);
            }
            let ack = Frame::Push(vec![
                Frame::bulk(kind),
                Frame::Bulk(target.clone()),
                Frame::Integer(self.subscription_count()),
            ]);
            self.reply(&ack, out);
        }
    }

    fn unsubscribe(&mut self, engine: &Engine, pattern: bool, args: &[Bytes], out: &mut Output) {
        let kind = if pattern {
            "punsubscribe"
        } else {
            "unsubscribe"
        };
        let targets: Vec<Bytes> = if args.len() > 1 {
            args[1..].to_vec()
        } else if pattern {
            self.patterns.iter().cloned().collect()
        } else {
            self.channels.iter().cloned().collect()
        };
        if targets.is_empty() {
            let ack = Frame::Push(vec![
                Frame::bulk(kind),
                Frame::Null,
                Frame::Integer(self.subscription_count()),
            ]);
            return self.reply(&ack, out);
        }
        for target in targets {
            if pattern {
                self.patterns.remove(&target);
                engine.hub().punsubscribe(&self.subscriber, &target);
            } else {
                self.channels.remove(&target);
                engine.hub().unsubscribe(&self.subscriber, &target);
            }
            let ack = Frame::Push(vec![
                Frame::bulk(kind),
                Frame::Bulk(target),
                Frame::Integer(self.subscription_count()),
            ]);
            self.reply(&ack, out);
        }
    }
}

/// Compares secrets without an early exit on the first differing byte.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let mut diff = a.len() ^ b.len();
    for i in 0..a.len().max(b.len()) {
        diff |= usize::from(a.get(i).copied().unwrap_or(0) ^ b.get(i).copied().unwrap_or(0));
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::constant_time_eq;

    #[test]
    fn secret_comparison() {
        assert!(constant_time_eq(b"hunter2", b"hunter2"));
        assert!(!constant_time_eq(b"hunter2", b"hunter3"));
        assert!(!constant_time_eq(b"hunter2", b"hunter"));
        assert!(!constant_time_eq(b"", b"x"));
        assert!(constant_time_eq(b"", b""));
    }
}
