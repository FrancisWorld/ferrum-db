//! Append-only file writer.
//!
//! Commands are appended to an in-memory buffer *while the caller still holds the
//! shard locks of the command*, so the file order is a valid serialisation of the
//! execution order. A dedicated writer thread drains the buffer, so request tasks
//! never perform file I/O. With `always`, a client is acknowledged only after the
//! chunk containing its command was fsynced (concurrent commands share one fsync:
//! group commit). With `everysec` at most about one second of writes can be lost
//! on power failure; with `no` the OS decides.
//!
//! Rewrites run against a point-in-time dump: from that instant every appended
//! command is mirrored into a *diff* buffer; once the compacted file is written, the
//! writer thread appends the diff, fsyncs and atomically renames it over the AOF.

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use bytes::Bytes;
use parking_lot::{Condvar, Mutex};
use tokio::sync::watch;

use super::rewrite;
use crate::db::DumpEntry;

/// When the writer thread calls `fsync`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncPolicy {
    /// After every batch, before acknowledging the clients in it.
    Always,
    /// At most once per second, in the background.
    EverySec,
    /// Never; leave it to the operating system.
    No,
}

impl FsyncPolicy {
    /// Parses `always`, `everysec` or `no`.
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "always" => Some(Self::Always),
            "everysec" => Some(Self::EverySec),
            "no" => Some(Self::No),
            _ => None,
        }
    }

    /// The configuration spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Always => "always",
            Self::EverySec => "everysec",
            Self::No => "no",
        }
    }
}

/// Serialises one command as a RESP array of bulk strings.
pub fn encode_command(args: &[Bytes], out: &mut Vec<u8>) {
    out.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for a in args {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
}

/// Failure reported to a client waiting for durability.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AofError {
    /// The writer thread could not write or fsync.
    #[error("MISCONF the append-only file cannot be written; writes are refused")]
    WriteFailed,
    /// A rewrite is already running.
    #[error("background append only file rewriting already in progress")]
    RewriteInProgress,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct SyncState {
    synced: u64,
    failed: bool,
}

struct RewriteJob {
    file: File,
    tmp_path: PathBuf,
    done: mpsc::Sender<io::Result<u64>>,
}

#[derive(Default)]
struct State {
    buf: Vec<u8>,
    appended: u64,
    diff: Option<Vec<u8>>,
    job: Option<RewriteJob>,
    shutdown: bool,
}

struct Shared {
    path: PathBuf,
    policy: FsyncPolicy,
    state: Mutex<State>,
    wake: Condvar,
    sync_tx: watch::Sender<SyncState>,
    size: AtomicU64,
    rewriting: AtomicBool,
    rewrites_done: AtomicU64,
}

/// Handle to the append-only file and its writer thread.
pub struct Aof {
    shared: Arc<Shared>,
    writer: Mutex<Option<JoinHandle<()>>>,
}

/// Held while a rewrite runs; releases the "one rewrite at a time" slot on drop.
pub struct RewriteSlot {
    shared: Arc<Shared>,
}

impl Drop for RewriteSlot {
    fn drop(&mut self) {
        self.shared.rewriting.store(false, Ordering::Release);
    }
}

impl Aof {
    /// Opens `path` for appending (creating it) and starts the writer thread.
    pub fn open(path: &Path, policy: FsyncPolicy) -> io::Result<Aof> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let size = file.metadata()?.len();
        let (sync_tx, _) = watch::channel(SyncState {
            synced: 0,
            failed: false,
        });
        let shared = Arc::new(Shared {
            path: path.to_path_buf(),
            policy,
            state: Mutex::new(State::default()),
            wake: Condvar::new(),
            sync_tx,
            size: AtomicU64::new(size),
            rewriting: AtomicBool::new(false),
            rewrites_done: AtomicU64::new(0),
        });
        let thread_shared = shared.clone();
        let writer = std::thread::Builder::new()
            .name("ferrum-aof".into())
            .spawn(move || writer_loop(&thread_shared, file))?;
        Ok(Aof {
            shared,
            writer: Mutex::new(Some(writer)),
        })
    }

    /// The configured fsync policy.
    pub fn policy(&self) -> FsyncPolicy {
        self.shared.policy
    }

    /// Appends commands to the log buffer and returns their sequence number.
    /// Never performs I/O, so it is safe to call under a shard lock.
    pub fn append(&self, commands: &[Vec<Bytes>]) -> u64 {
        let mut guard = self.shared.state.lock();
        let st = &mut *guard;
        let before = st.buf.len();
        for c in commands {
            encode_command(c, &mut st.buf);
        }
        if let Some(diff) = st.diff.as_mut() {
            diff.extend_from_slice(&st.buf[before..]);
        }
        st.appended += 1;
        let seq = st.appended;
        drop(guard);
        self.shared.wake.notify_one();
        seq
    }

    /// Resolves once everything up to `seq` is durable (only meaningful for `always`).
    pub async fn wait_synced(&self, seq: u64) -> Result<(), AofError> {
        let mut rx = self.shared.sync_tx.subscribe();
        let state = rx
            .wait_for(|s| s.failed || s.synced >= seq)
            .await
            .map_err(|_| AofError::WriteFailed)?;
        if state.failed {
            Err(AofError::WriteFailed)
        } else {
            Ok(())
        }
    }

    /// True once the writer thread hit an unrecoverable I/O error.
    pub fn failed(&self) -> bool {
        self.shared.sync_tx.borrow().failed
    }

    /// Current size of the AOF file in bytes.
    pub fn size(&self) -> u64 {
        self.shared.size.load(Ordering::Relaxed)
    }

    /// True while a rewrite is running.
    pub fn rewrite_in_progress(&self) -> bool {
        self.shared.rewriting.load(Ordering::Acquire)
    }

    /// Number of completed rewrites.
    pub fn rewrites_completed(&self) -> u64 {
        self.shared.rewrites_done.load(Ordering::Relaxed)
    }

    /// Claims the single rewrite slot.
    pub fn claim_rewrite(&self) -> Result<RewriteSlot, AofError> {
        self.shared
            .rewriting
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| RewriteSlot {
                shared: self.shared.clone(),
            })
            .map_err(|_| AofError::RewriteInProgress)
    }

    /// Starts mirroring appended commands into the diff buffer. The caller must hold
    /// every shard lock so the dump and this call describe the same instant.
    pub fn start_diff(&self) {
        self.shared.state.lock().diff = Some(Vec::new());
    }

    /// Writes `entries` to a temporary file, then has the writer thread append the
    /// diff and swap the file in. Blocks until finished; run it off the request path.
    pub fn finish_rewrite(&self, _slot: RewriteSlot, entries: &[DumpEntry]) -> io::Result<u64> {
        let result = self.rewrite_inner(entries);
        if result.is_err() {
            self.shared.state.lock().diff = None;
        }
        result
    }

    fn rewrite_inner(&self, entries: &[DumpEntry]) -> io::Result<u64> {
        let tmp_path = self.shared.path.with_extension("aof.rewrite");
        let mut file = File::create(&tmp_path)?;
        rewrite::write_aof(&mut file, entries)?;
        let (done, wait) = mpsc::channel();
        {
            let mut st = self.shared.state.lock();
            st.job = Some(RewriteJob {
                file,
                tmp_path,
                done,
            });
        }
        self.shared.wake.notify_one();
        wait.recv()
            .map_err(|_| io::Error::other("AOF writer stopped during rewrite"))?
    }

    /// Flushes, fsyncs and stops the writer thread.
    pub fn shutdown(&self) {
        self.shared.state.lock().shutdown = true;
        self.shared.wake.notify_one();
        if let Some(handle) = self.writer.lock().take() {
            let _ = handle.join();
        }
    }
}

impl Drop for Aof {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn publish(shared: &Shared, synced: u64, failed: bool) {
    shared.sync_tx.send_if_modified(|s| {
        let next = SyncState {
            synced: s.synced.max(synced),
            failed: s.failed || failed,
        };
        let changed = next != *s;
        *s = next;
        changed
    });
}

fn writer_loop(shared: &Shared, mut file: File) {
    let mut last_sync = Instant::now();
    let mut unsynced = false;
    let mut written_seq = 0u64;
    loop {
        let (chunk, seq, job, shutdown) = {
            let mut st = shared.state.lock();
            while st.buf.is_empty() && st.job.is_none() && !st.shutdown {
                if unsynced && shared.policy == FsyncPolicy::EverySec {
                    let elapsed = last_sync.elapsed();
                    if elapsed >= Duration::from_secs(1) {
                        break;
                    }
                    shared
                        .wake
                        .wait_for(&mut st, Duration::from_secs(1) - elapsed);
                } else {
                    shared.wake.wait(&mut st);
                }
            }
            (
                std::mem::take(&mut st.buf),
                st.appended,
                st.job.take(),
                st.shutdown,
            )
        };

        if !chunk.is_empty() {
            match file.write_all(&chunk) {
                Ok(()) => {
                    shared.size.fetch_add(chunk.len() as u64, Ordering::Relaxed);
                    written_seq = seq;
                    unsynced = true;
                }
                Err(e) => {
                    tracing::error!(error = %e, "AOF write failed; refusing further writes");
                    publish(shared, 0, true);
                }
            }
        }
        let due = match shared.policy {
            FsyncPolicy::Always => unsynced,
            FsyncPolicy::EverySec => unsynced && last_sync.elapsed() >= Duration::from_secs(1),
            FsyncPolicy::No => false,
        };
        if due || (shutdown && unsynced) {
            match file.sync_data() {
                Ok(()) => {
                    publish(shared, written_seq, false);
                    unsynced = false;
                    last_sync = Instant::now();
                }
                Err(e) => {
                    tracing::error!(error = %e, "AOF fsync failed; refusing further writes");
                    publish(shared, 0, true);
                }
            }
        } else if shared.policy != FsyncPolicy::Always {
            publish(shared, written_seq, false);
        }

        if let Some(job) = job {
            let outcome = swap_in_rewrite(shared, job.file, &job.tmp_path);
            match outcome {
                Ok((new_file, size)) => {
                    file = new_file;
                    shared.size.store(size, Ordering::Relaxed);
                    shared.rewrites_done.fetch_add(1, Ordering::Relaxed);
                    unsynced = false;
                    let _ = job.done.send(Ok(size));
                }
                Err(e) => {
                    let _ = std::fs::remove_file(&job.tmp_path);
                    let _ = job.done.send(Err(e));
                }
            }
        }
        if shutdown {
            return;
        }
    }
}

/// Appends the accumulated diff to the compacted file and renames it over the AOF.
/// The last drain happens under the state lock so no command can slip between the
/// final diff and the switch; earlier drains keep that final critical section short.
fn swap_in_rewrite(shared: &Shared, mut file: File, tmp_path: &Path) -> io::Result<(File, u64)> {
    const FINAL_THRESHOLD: usize = 64 * 1024;
    loop {
        let pending = {
            let mut st = shared.state.lock();
            match st.diff.as_mut() {
                Some(d) if d.len() > FINAL_THRESHOLD => std::mem::take(d),
                _ => break,
            }
        };
        file.write_all(&pending)?;
    }
    let mut st = shared.state.lock();
    let tail = st.diff.take().unwrap_or_default();
    // Everything buffered so far is also in the diff; the old file is being replaced.
    st.buf.clear();
    let synced_upto = st.appended;
    file.write_all(&tail)?;
    file.sync_all()?;
    std::fs::rename(tmp_path, &shared.path)?;
    super::sync_parent(&shared.path)?;
    let size = file.metadata()?.len();
    drop(st);
    publish(shared, synced_upto, false);
    Ok((file, size))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Value;

    fn cmd(parts: &[&str]) -> Vec<Bytes> {
        parts
            .iter()
            .map(|p| Bytes::copy_from_slice(p.as_bytes()))
            .collect()
    }

    #[test]
    fn encodes_resp_arrays() {
        let mut out = Vec::new();
        encode_command(&cmd(&["SET", "k", "v"]), &mut out);
        assert_eq!(out, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n");
    }

    #[test]
    fn policy_names_round_trip() {
        for p in [FsyncPolicy::Always, FsyncPolicy::EverySec, FsyncPolicy::No] {
            assert_eq!(FsyncPolicy::parse(p.as_str()), Some(p));
        }
        assert_eq!(FsyncPolicy::parse("sometimes"), None);
    }

    #[tokio::test]
    async fn always_policy_acknowledges_only_after_fsync() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.aof");
        let aof = Aof::open(&path, FsyncPolicy::Always).unwrap();
        let seq = aof.append(&[cmd(&["SET", "a", "1"])]);
        aof.wait_synced(seq).await.unwrap();
        assert_eq!(
            std::fs::read(&path).unwrap(),
            b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n"
        );
        assert_eq!(aof.size(), std::fs::metadata(&path).unwrap().len());
    }

    #[tokio::test]
    async fn concurrent_appenders_all_become_durable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.aof");
        let aof = Arc::new(Aof::open(&path, FsyncPolicy::Always).unwrap());
        let tasks: Vec<_> = (0..32)
            .map(|i| {
                let aof = aof.clone();
                tokio::spawn(async move {
                    let seq = aof.append(&[cmd(&["SET", &format!("k{i}"), "v"])]);
                    aof.wait_synced(seq).await.unwrap();
                })
            })
            .collect();
        for t in tasks {
            t.await.unwrap();
        }
        let text = String::from_utf8(std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(text.matches("SET").count(), 32);
    }

    #[test]
    fn everysec_flushes_in_background_and_shutdown_syncs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.aof");
        let aof = Aof::open(&path, FsyncPolicy::EverySec).unwrap();
        aof.append(&[cmd(&["SET", "a", "1"])]);
        aof.shutdown();
        assert!(!std::fs::read(&path).unwrap().is_empty());
    }

    #[test]
    fn rewrite_swaps_in_compacted_file_and_keeps_concurrent_writes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.aof");
        let aof = Aof::open(&path, FsyncPolicy::No).unwrap();
        for _ in 0..100 {
            aof.append(&[cmd(&["INCR", "counter"])]);
        }
        let slot = aof.claim_rewrite().unwrap();
        assert_eq!(aof.claim_rewrite().err(), Some(AofError::RewriteInProgress));
        assert!(aof.rewrite_in_progress());
        aof.start_diff();
        // Written after the dump point: must survive in the new file via the diff.
        aof.append(&[cmd(&["SET", "after", "dump"])]);
        let entries = vec![DumpEntry {
            key: Bytes::from_static(b"counter"),
            value: Value::Str(Bytes::from_static(b"100")),
            expire_at: None,
        }];
        aof.finish_rewrite(slot, &entries).unwrap();
        assert!(!aof.rewrite_in_progress());
        assert_eq!(aof.rewrites_completed(), 1);
        // Later appends go to the new file.
        aof.append(&[cmd(&["SET", "later", "x"])]);
        aof.shutdown();
        let text = String::from_utf8(std::fs::read(&path).unwrap()).unwrap();
        assert!(!text.contains("INCR"), "old history must be compacted away");
        assert!(text.contains("100"));
        let (a, b, c) = (
            text.find("100").unwrap(),
            text.find("after").unwrap(),
            text.find("later").unwrap(),
        );
        assert!(a < b && b < c, "order: snapshot, diff, new appends");
        assert!(!path.with_extension("aof.rewrite").exists());
    }
}
