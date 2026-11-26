//! Shared helpers: an in-process server on an ephemeral port and a tiny RESP client.
#![allow(dead_code, missing_docs)]

use std::net::SocketAddr;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use ferrum_db::clock::{Clock, SystemClock};
use ferrum_db::resp::{Decoder, Frame};
use ferrum_db::{Config, Engine, Server};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

pub struct TestServer {
    pub addr: SocketAddr,
    pub engine: Arc<Engine>,
    pub dir: tempfile::TempDir,
    stop: Option<oneshot::Sender<()>>,
    handle: Option<JoinHandle<()>>,
}

impl TestServer {
    pub async fn start() -> TestServer {
        Self::start_with(|_| {}).await
    }

    pub async fn start_with(tweak: impl FnOnce(&mut Config)) -> TestServer {
        let dir = tempfile::tempdir().expect("tempdir");
        Self::start_in(dir, tweak).await
    }

    /// Starts a server whose data directory is `dir` (to restart over existing files).
    pub async fn start_in(dir: tempfile::TempDir, tweak: impl FnOnce(&mut Config)) -> TestServer {
        Self::start_full(dir, Arc::new(SystemClock), tweak).await
    }

    /// Starts a server driven by a caller-supplied clock (e.g. a manual one).
    pub async fn start_clocked(
        clock: Arc<dyn Clock>,
        tweak: impl FnOnce(&mut Config),
    ) -> TestServer {
        Self::start_full(tempfile::tempdir().expect("tempdir"), clock, tweak).await
    }

    async fn start_full(
        dir: tempfile::TempDir,
        clock: Arc<dyn Clock>,
        tweak: impl FnOnce(&mut Config),
    ) -> TestServer {
        let mut config = Config {
            port: 0,
            dir: dir.path().to_path_buf(),
            ..Config::default()
        };
        tweak(&mut config);
        let engine = Engine::open(config, clock).expect("open engine");
        let server = Server::bind(engine.clone()).await.expect("bind");
        let addr = server.local_addr().expect("addr");
        let (stop, rx) = oneshot::channel();
        let handle = tokio::spawn(server.run(async {
            let _ = rx.await;
        }));
        TestServer {
            addr,
            engine,
            dir,
            stop: Some(stop),
            handle: Some(handle),
        }
    }

    pub async fn client(&self) -> Client {
        Client::connect(self.addr).await
    }

    /// Graceful shutdown; returns the data directory so a test can restart on it.
    pub async fn stop(mut self) -> tempfile::TempDir {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(handle) = self.handle.take() {
            handle.await.expect("server task");
        }
        self.dir
    }
}

pub struct Client {
    stream: TcpStream,
    buf: BytesMut,
    decoder: Decoder,
}

pub fn b(s: &str) -> Frame {
    Frame::bulk(s)
}

pub fn ok() -> Frame {
    Frame::simple("OK")
}

pub fn int(n: i64) -> Frame {
    Frame::Integer(n)
}

pub fn arr(items: &[&str]) -> Frame {
    Frame::Array(items.iter().map(|s| b(s)).collect())
}

impl Client {
    pub async fn connect(addr: SocketAddr) -> Client {
        let stream = TcpStream::connect(addr).await.expect("connect");
        Client {
            stream,
            buf: BytesMut::new(),
            decoder: Decoder::default(),
        }
    }

    pub fn encode(args: &[&str]) -> Vec<u8> {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            out.extend_from_slice(format!("${}\r\n{}\r\n", a.len(), a).as_bytes());
        }
        out
    }

    pub async fn send(&mut self, args: &[&str]) {
        self.stream
            .write_all(&Self::encode(args))
            .await
            .expect("write");
    }

    pub async fn send_raw(&mut self, data: &[u8]) {
        self.stream.write_all(data).await.expect("write");
    }

    /// Reads the next frame; `None` on EOF or after `timeout`.
    pub async fn try_read(&mut self, timeout: Duration) -> Option<Frame> {
        tokio::time::timeout(timeout, async {
            loop {
                if let Some(frame) = self.decoder.decode(&mut self.buf).expect("valid reply") {
                    return Some(frame);
                }
                if self.stream.read_buf(&mut self.buf).await.ok()? == 0 {
                    return None;
                }
            }
        })
        .await
        .ok()
        .flatten()
    }

    pub async fn read(&mut self) -> Frame {
        self.try_read(Duration::from_secs(5))
            .await
            .expect("reply within 5s")
    }

    pub async fn cmd(&mut self, args: &[&str]) -> Frame {
        self.send(args).await;
        self.read().await
    }

    /// Discards incoming bytes until the server closes the connection.
    /// Returns false if that did not happen within `timeout`.
    pub async fn drain_to_eof(&mut self, timeout: Duration) -> bool {
        let mut scratch = vec![0u8; 64 * 1024];
        tokio::time::timeout(timeout, async {
            loop {
                match self.stream.read(&mut scratch).await {
                    Ok(0) | Err(_) => return,
                    Ok(_) => {}
                }
            }
        })
        .await
        .is_ok()
    }

    /// True if the server closed the connection.
    pub async fn is_closed(&mut self) -> bool {
        let mut scratch = [0u8; 64];
        match tokio::time::timeout(Duration::from_secs(5), self.stream.read(&mut scratch)).await {
            Ok(Ok(0)) | Ok(Err(_)) => true,
            Ok(Ok(_)) => false,
            Err(_) => false,
        }
    }
}

/// Polls `condition` until it holds, panicking after `timeout` (for background work).
pub async fn eventually(timeout: Duration, what: &str, mut condition: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + timeout;
    while !condition() {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for: {what}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

pub fn bytes(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

/// A real `ferrum-db` process, for tests that must survive or cause a crash.
pub struct Proc {
    child: Child,
    pub addr: SocketAddr,
}

impl Proc {
    pub fn spawn(dir: &Path, extra: &[&str]) -> Proc {
        Self::spawn_env(dir, extra, &[])
    }

    pub fn spawn_env(dir: &Path, extra: &[&str], envs: &[(&str, &str)]) -> Proc {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let child = Command::new(env!("CARGO_BIN_EXE_ferrum-db"))
            .args(["--port", &port.to_string(), "--dir"])
            .arg(dir)
            .args(extra)
            .envs(envs.iter().copied())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn ferrum-db");
        Proc {
            child,
            addr: SocketAddr::from(([127, 0, 0, 1], port)),
        }
    }

    pub async fn ready(&mut self) -> Client {
        for _ in 0..200 {
            if let Some(status) = self.child.try_wait().unwrap() {
                panic!("server exited early: {status}");
            }
            if let Ok(stream) = tokio::net::TcpStream::connect(self.addr).await {
                drop(stream);
                return Client::connect(self.addr).await;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("server did not become ready");
    }

    /// Sends SIGTERM through the `kill` utility (the exact child PID only).
    pub fn terminate(&self) {
        let status = Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()
            .unwrap();
        assert!(status.success());
    }

    /// Waits for the child to exit by itself.
    pub async fn wait_exit(&mut self) -> std::process::ExitStatus {
        for _ in 0..400 {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("server did not exit");
    }

    /// SIGKILL: no destructors, no flush, exactly like a crash.
    pub fn crash(mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
    }
}

impl Drop for Proc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
