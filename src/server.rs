//! TCP front end: accepts connections, drives the decoder and the session, and
//! applies backpressure and graceful shutdown.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinSet;

use crate::engine::Engine;
use crate::pubsub::Subscriber;
use crate::resp::{Decoder, Frame, Limits, Protocol, encode};
use crate::session::{Output, Session};

/// Replies are flushed mid-pipeline once this many bytes are buffered.
const FLUSH_THRESHOLD: usize = 256 * 1024;
/// How long connections get to finish after shutdown is requested.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// A bound listener plus the engine it serves.
pub struct Server {
    engine: Arc<Engine>,
    listener: TcpListener,
}

impl Server {
    /// Binds the address from the engine's configuration.
    pub async fn bind(engine: Arc<Engine>) -> io::Result<Server> {
        let addr = format!("{}:{}", engine.config().bind, engine.config().port);
        let listener = TcpListener::bind(addr).await?;
        Ok(Server { engine, listener })
    }

    /// The address actually bound (useful with port 0).
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Serves until `shutdown` resolves, then drains connections and flushes the AOF.
    pub async fn run(self, shutdown: impl Future<Output = ()>) {
        let (stop_tx, stop_rx) = watch::channel(false);
        let mut connections = JoinSet::new();
        let expiry = tokio::spawn(expiry_loop(self.engine.clone(), stop_rx.clone()));
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                () = &mut shutdown => break,
                accepted = self.listener.accept() => match accepted {
                    Ok((stream, peer)) => self.admit(&mut connections, stream, peer, &stop_rx),
                    Err(e) => {
                        // Typically EMFILE; back off instead of spinning.
                        tracing::warn!(error = %e, "accept failed");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                },
                Some(_) = connections.join_next() => {}
            }
        }
        tracing::info!("shutting down: draining connections");
        let _ = stop_tx.send(true);
        let drain = async { while connections.join_next().await.is_some() {} };
        if tokio::time::timeout(DRAIN_TIMEOUT, drain).await.is_err() {
            tracing::warn!("connections did not drain in time; aborting them");
            connections.abort_all();
        }
        let _ = expiry.await;
        self.engine.shutdown();
        tracing::info!("shutdown complete");
    }

    fn admit(
        &self,
        connections: &mut JoinSet<()>,
        stream: TcpStream,
        peer: SocketAddr,
        stop: &watch::Receiver<bool>,
    ) {
        let stats = self.engine.stats();
        if stats.clients.load(Ordering::Relaxed) >= self.engine.config().max_clients {
            stats.rejected_connections.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(%peer, "connection refused: max clients reached");
            tokio::spawn(async move {
                let mut stream = stream;
                let _ = stream
                    .write_all(b"-ERR max number of clients reached\r\n")
                    .await;
            });
            return;
        }
        stats.clients.fetch_add(1, Ordering::Relaxed);
        stats.connections.fetch_add(1, Ordering::Relaxed);
        let (engine, stop) = (self.engine.clone(), stop.clone());
        connections.spawn(async move {
            tracing::debug!(%peer, "client connected");
            serve_connection(stream, &engine, stop).await;
            engine.stats().clients.fetch_sub(1, Ordering::Relaxed);
            tracing::debug!(%peer, "client disconnected");
        });
    }
}

async fn expiry_loop(engine: Arc<Engine>, mut stop: watch::Receiver<bool>) {
    let period = Duration::from_millis(1000 / u64::from(engine.config().hz.clamp(1, 1000)));
    let mut tick = tokio::time::interval(period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = tick.tick() => {
                // A pass that hit its budget probably left more work: go again, but bounded.
                for _ in 0..10 {
                    if engine.expire_cycle() < 100 {
                        break;
                    }
                }
            }
            _ = stop.changed() => return,
        }
    }
}

enum Flow {
    Continue,
    Close,
}

async fn serve_connection(stream: TcpStream, engine: &Engine, mut stop: watch::Receiver<bool>) {
    let _ = stream.set_nodelay(true);
    let (mut reader, mut writer) = stream.into_split();
    let mut session = Session::new(engine);
    let subscriber = session.subscriber().clone();
    let limits = Limits {
        max_bulk_len: engine.config().max_bulk_len,
        ..Limits::default()
    };
    let mut decoder = Decoder::new(limits);
    let mut inbuf = BytesMut::with_capacity(16 * 1024);

    'conn: loop {
        let flow = tokio::select! {
            biased;
            _ = stop.changed() => Flow::Close,
            () = subscriber.kicked() => {
                tracing::warn!(queued = subscriber.queued_bytes(), "subscriber too slow; disconnecting");
                break 'conn;
            }
            Some(push) = session.next_push(), if session.is_subscribed() => {
                let mut out = Output::default();
                session.encode_push(push, &mut out.buf);
                while out.buf.len() < FLUSH_THRESHOLD {
                    let Some(next) = session.try_next_push() else { break };
                    session.encode_push(next, &mut out.buf);
                }
                match flush(engine, &mut writer, &mut out, &subscriber).await {
                    Ok(()) => Flow::Continue,
                    Err(()) => break 'conn,
                }
            }
            read = reader.read_buf(&mut inbuf) => match read {
                Ok(0) | Err(_) => Flow::Close,
                Ok(_) => process(engine, &mut session, &mut decoder, &mut inbuf, &mut writer, &subscriber).await,
            },
        };
        if matches!(flow, Flow::Close) {
            break;
        }
    }
    drop(reader);
    session.close(engine);
}

/// Decodes and executes every complete command in `inbuf`, then flushes the replies.
async fn process(
    engine: &Engine,
    session: &mut Session,
    decoder: &mut Decoder,
    inbuf: &mut BytesMut,
    writer: &mut OwnedWriteHalf,
    subscriber: &Subscriber,
) -> Flow {
    let mut out = Output::default();
    loop {
        match decoder.decode(inbuf) {
            Ok(Some(frame)) => {
                match command_args(frame) {
                    Ok(Some(args)) => session.handle(engine, args, &mut out),
                    Ok(None) => {}
                    Err(msg) => {
                        encode(
                            &Frame::error(format!("ERR Protocol error: {msg}")),
                            Protocol::Resp2,
                            &mut out.buf,
                        );
                        out.close = true;
                    }
                }
                if out.close {
                    break;
                }
                if out.buf.len() >= FLUSH_THRESHOLD
                    && flush(engine, writer, &mut out, subscriber).await.is_err()
                {
                    return Flow::Close;
                }
            }
            Ok(None) => break,
            Err(e) => {
                encode(
                    &Frame::error(format!("ERR Protocol error: {e}")),
                    Protocol::Resp2,
                    &mut out.buf,
                );
                out.close = true;
                break;
            }
        }
    }
    let close = out.close;
    if flush(engine, writer, &mut out, subscriber).await.is_err() || close {
        return Flow::Close;
    }
    Flow::Continue
}

/// Converts a decoded frame into command arguments. `Ok(None)` means "ignore".
fn command_args(frame: Frame) -> Result<Option<Vec<Bytes>>, &'static str> {
    let Frame::Array(items) = frame else {
        return Err("expected an array of bulk strings");
    };
    if items.is_empty() {
        return Ok(None);
    }
    items
        .into_iter()
        .map(|f| match f {
            Frame::Bulk(b) => Ok(b),
            _ => Err("expected an array of bulk strings"),
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

/// Waits for AOF durability if required, then writes the buffered replies.
/// An error means the connection must be dropped without acknowledging anything.
async fn flush(
    engine: &Engine,
    writer: &mut OwnedWriteHalf,
    out: &mut Output,
    subscriber: &Subscriber,
) -> Result<(), ()> {
    if let Some(seq) = out.sync_seq.take()
        && let Err(e) = engine.wait_durable(seq).await
    {
        tracing::error!(error = %e, "durability not guaranteed; dropping connection unacknowledged");
        return Err(());
    }
    if out.buf.is_empty() {
        return Ok(());
    }
    let data = out.buf.split();
    // A subscriber that stops reading blocks this write; the hub's kick must still win.
    tokio::select! {
        result = writer.write_all(&data) => result.map_err(|_| ()),
        () = subscriber.kicked() => Err(()),
    }
}
