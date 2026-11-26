//! Durability: crash recovery of the real binary, torn-tail handling at every byte
//! offset, AOF rewrite under load, snapshots, and expiry/eviction replay safety.
#![allow(missing_docs)]

mod common;

use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use common::*;
use ferrum_db::clock::SystemClock;
use ferrum_db::db::EvictionPolicy;
use ferrum_db::persist::PersistError;
use ferrum_db::persist::aof::{FsyncPolicy, encode_command};
use ferrum_db::resp::Frame;
use ferrum_db::{Config, Engine};

// ---- helpers ----------------------------------------------------------------

fn aof_config(dir: &Path, fsync: FsyncPolicy) -> Config {
    Config {
        port: 0,
        dir: dir.to_path_buf(),
        aof_enabled: true,
        aof_fsync: fsync,
        shards: 4,
        ..Config::default()
    }
}

fn bulk_text(f: Frame) -> String {
    match f {
        Frame::Bulk(b) => String::from_utf8_lossy(&b).to_string(),
        other => panic!("bulk expected, got {other:?}"),
    }
}

async fn info_field(c: &mut Client, section: &str, field: &str) -> String {
    let text = bulk_text(c.cmd(&["INFO", section]).await);
    text.lines()
        .find_map(|l| l.strip_prefix(&format!("{field}:")))
        .unwrap_or_else(|| panic!("{field} missing in {text}"))
        .trim()
        .to_string()
}

fn cmd(parts: &[&str]) -> Vec<Bytes> {
    parts
        .iter()
        .map(|p| Bytes::copy_from_slice(p.as_bytes()))
        .collect()
}

// ---- crash recovery of the real process ----------------------------------------

#[tokio::test]
async fn acknowledged_writes_survive_sigkill_with_fsync_always() {
    let dir = tempfile::tempdir().unwrap();
    let mut proc = Proc::spawn(dir.path(), &["--aof", "--aof-fsync", "always"]);
    let mut c = proc.ready().await;
    for i in 0..200 {
        assert_eq!(
            c.cmd(&["SET", &format!("key:{i}"), &format!("value:{i}")])
                .await,
            ok()
        );
    }
    c.cmd(&["RPUSH", "list", "a", "b", "c"]).await;
    c.cmd(&["HSET", "hash", "f", "v"]).await;
    c.cmd(&["ZADD", "zset", "1.5", "m"]).await;
    c.cmd(&["MULTI"]).await;
    c.cmd(&["SET", "tx:a", "1"]).await;
    c.cmd(&["SET", "tx:b", "2"]).await;
    c.cmd(&["EXEC"]).await;
    c.cmd(&["EXPIRE", "key:0", "3600"]).await;
    proc.crash();

    let mut proc = Proc::spawn(dir.path(), &["--aof", "--aof-fsync", "always"]);
    let mut c = proc.ready().await;
    assert_eq!(c.cmd(&["DBSIZE"]).await, int(200 + 3 + 2));
    for i in 0..200 {
        assert_eq!(
            c.cmd(&["GET", &format!("key:{i}")]).await,
            b(&format!("value:{i}"))
        );
    }
    assert_eq!(
        c.cmd(&["LRANGE", "list", "0", "-1"]).await,
        arr(&["a", "b", "c"])
    );
    assert_eq!(c.cmd(&["ZSCORE", "zset", "m"]).await, b("1.5"));
    assert_eq!(c.cmd(&["MGET", "tx:a", "tx:b"]).await, arr(&["1", "2"]));
    let Frame::Integer(ttl) = c.cmd(&["TTL", "key:0"]).await else {
        panic!()
    };
    assert!(
        (3590..=3600).contains(&ttl),
        "TTL must be restored from the absolute deadline, got {ttl}"
    );
    assert_eq!(
        info_field(&mut c, "persistence", "aof_truncated_bytes_at_load").await,
        "0"
    );
    proc.crash();
}

#[tokio::test]
async fn torn_tail_is_discarded_and_the_server_keeps_working() {
    let dir = tempfile::tempdir().unwrap();
    let aof_path = dir.path().join("appendonly.aof");
    let mut proc = Proc::spawn(dir.path(), &["--aof", "--aof-fsync", "always"]);
    let mut c = proc.ready().await;
    for i in 0..50 {
        c.cmd(&["SET", &format!("k{i}"), "v"]).await;
    }
    proc.crash();
    let good_len = std::fs::metadata(&aof_path).unwrap().len();

    // A crash in the middle of appending a command leaves a prefix of it behind.
    let partial = b"*3\r\n$3\r\nSET\r\n$4\r\ntorn\r\n$10\r\nabc";
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&aof_path)
        .unwrap();
    std::io::Write::write_all(&mut file, partial).unwrap();
    drop(file);

    let mut proc = Proc::spawn(dir.path(), &["--aof", "--aof-fsync", "always"]);
    let mut c = proc.ready().await;
    assert_eq!(c.cmd(&["DBSIZE"]).await, int(50));
    assert_eq!(c.cmd(&["GET", "torn"]).await, Frame::Null);
    assert_eq!(
        info_field(&mut c, "persistence", "aof_truncated_bytes_at_load").await,
        partial.len().to_string()
    );
    assert_eq!(
        std::fs::metadata(&aof_path).unwrap().len(),
        good_len,
        "the torn bytes must be physically removed"
    );

    // New writes land cleanly after the cut and survive another crash.
    assert_eq!(c.cmd(&["SET", "after", "recovery"]).await, ok());
    proc.crash();
    let mut proc = Proc::spawn(dir.path(), &["--aof", "--aof-fsync", "always"]);
    let mut c = proc.ready().await;
    assert_eq!(c.cmd(&["DBSIZE"]).await, int(51));
    assert_eq!(c.cmd(&["GET", "after"]).await, b("recovery"));
    proc.crash();
}

#[tokio::test]
async fn corrupt_aof_makes_the_binary_refuse_to_start() {
    let dir = tempfile::tempdir().unwrap();
    let mut data = Vec::new();
    encode_command(&cmd(&["SET", "a", "1"]), &mut data);
    data.extend_from_slice(b"*2\r\n$3\r\nGET\r\n:5\r\n"); // an integer where a bulk string must be
    encode_command(&cmd(&["SET", "b", "2"]), &mut data);
    std::fs::write(dir.path().join("appendonly.aof"), &data).unwrap();

    let mut child = Command::new(env!("CARGO_BIN_EXE_ferrum-db"))
        .args(["--port", "0", "--aof", "--dir"])
        .arg(dir.path())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let status = tokio::task::spawn_blocking(move || {
        let status = child.wait().unwrap();
        let mut stderr = String::new();
        std::io::Read::read_to_string(&mut child.stderr.take().unwrap(), &mut stderr).unwrap();
        (status, stderr)
    })
    .await
    .unwrap();
    assert!(
        !status.0.success(),
        "server must not start on a corrupt AOF"
    );
    assert!(
        status.1.contains("corrupt data at byte"),
        "stderr: {}",
        status.1
    );
    // The file is left untouched for the operator to inspect.
    assert_eq!(
        std::fs::read(dir.path().join("appendonly.aof")).unwrap(),
        data
    );
}

// ---- recovery at every possible truncation point ---------------------------------

/// Cuts a log containing plain commands and a MULTI/EXEC block at *every* byte
/// offset. Whatever the cut, recovery must yield exactly the state after the last
/// complete unit: never a partial command, never half a transaction.
#[test]
fn recovery_is_correct_for_a_cut_at_every_byte_offset() {
    let units: Vec<Vec<Vec<Bytes>>> = vec![
        vec![cmd(&["SET", "alpha", "1"])],
        vec![cmd(&["INCR", "counter"])],
        vec![
            cmd(&["MULTI"]),
            cmd(&["SET", "x", "1"]),
            cmd(&["SET", "y", "2"]),
            cmd(&["EXEC"]),
        ],
        vec![cmd(&["RPUSH", "list", "a", "b"])],
        vec![cmd(&["INCRBY", "counter", "41"])],
        vec![
            cmd(&["MULTI"]),
            cmd(&["DEL", "alpha"]),
            cmd(&["SET", "z", "binary\r\nvalue"]),
            cmd(&["EXEC"]),
        ],
        vec![cmd(&["HSET", "h", "f", "v"])],
    ];
    let mut wire = Vec::new();
    let mut unit_ends = Vec::new();
    for unit in &units {
        for c in unit {
            encode_command(c, &mut wire);
        }
        unit_ends.push(wire.len());
    }

    let render = |engine: &Engine| {
        let mut rows: Vec<String> = engine
            .db()
            .lock_all()
            .dump(0)
            .into_iter()
            .map(|e| format!("{}={:?}", String::from_utf8_lossy(&e.key), e.value))
            .collect();
        rows.sort();
        rows
    };
    let expected_after = |units_done: usize| {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(
            Config {
                port: 0,
                dir: dir.path().to_path_buf(),
                shards: 2,
                ..Config::default()
            },
            Arc::new(SystemClock),
        )
        .unwrap();
        for unit in &units[..units_done] {
            for c in unit
                .iter()
                .filter(|c| !matches!(c[0].as_ref(), b"MULTI" | b"EXEC"))
            {
                engine.replay(c).unwrap();
            }
        }
        render(&engine)
    };
    let expectations: Vec<Vec<String>> = (0..=units.len()).map(expected_after).collect();

    for cut in 0..=wire.len() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("appendonly.aof");
        std::fs::write(&path, &wire[..cut]).unwrap();
        let cfg = Config {
            port: 0,
            dir: dir.path().to_path_buf(),
            aof_enabled: true,
            aof_fsync: FsyncPolicy::No,
            shards: 2,
            ..Config::default()
        };
        let engine = Engine::open(cfg, Arc::new(SystemClock))
            .unwrap_or_else(|e| panic!("cut at {cut} failed to recover: {e}"));
        let done = unit_ends.iter().filter(|&&end| end <= cut).count();
        let committed = if done == 0 { 0 } else { unit_ends[done - 1] };
        assert_eq!(
            render(&engine),
            expectations[done],
            "state wrong after cutting at byte {cut}"
        );
        assert_eq!(
            engine.load_report().truncated_bytes,
            (cut - committed) as u64,
            "cut {cut}"
        );
        engine.shutdown();
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            committed as u64,
            "file must be cut back to the last commit (cut {cut})"
        );
    }
}

#[test]
fn damaged_records_that_are_not_a_torn_tail_are_reported_with_an_offset() {
    let cases: Vec<(&str, Vec<u8>)> = vec![
        ("bad length", b"*1\r\n$abc\r\n".to_vec()),
        ("missing CRLF after payload", b"*1\r\n$4\r\nPINGXX".to_vec()),
        ("unknown command", {
            let mut v = Vec::new();
            encode_command(&cmd(&["NOSUCHCOMMAND", "x"]), &mut v);
            v
        }),
        ("EXEC without MULTI", {
            let mut v = Vec::new();
            encode_command(&cmd(&["EXEC"]), &mut v);
            v
        }),
        ("not an array", b"+OK\r\n".to_vec()),
    ];
    for (name, bad) in cases {
        let dir = tempfile::tempdir().unwrap();
        let mut data = Vec::new();
        encode_command(&cmd(&["SET", "ok", "1"]), &mut data);
        let valid_len = data.len();
        data.extend_from_slice(&bad);
        std::fs::write(dir.path().join("appendonly.aof"), &data).unwrap();
        let cfg = aof_config(dir.path(), FsyncPolicy::No);
        match Engine::open(cfg, Arc::new(SystemClock)) {
            Err(PersistError::Corrupt { offset, .. } | PersistError::Protocol { offset, .. }) => {
                assert_eq!(
                    offset, valid_len as u64,
                    "{name}: offset must point at the bad record"
                );
            }
            Err(other) => panic!("{name}: unexpected error {other}"),
            Ok(_) => panic!("{name}: must not start"),
        }
    }
}

// ---- rewrite ---------------------------------------------------------------------

async fn wait_for_rewrite(c: &mut Client, completed: u32) {
    for _ in 0..400 {
        if info_field(c, "persistence", "aof_rewrites_completed")
            .await
            .parse::<u32>()
            .unwrap()
            >= completed
            && info_field(c, "persistence", "aof_rewrite_in_progress").await == "0"
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("rewrite did not finish");
}

#[tokio::test]
async fn rewrite_compacts_history() {
    let srv = TestServer::start_with(|c| {
        c.aof_enabled = true;
        c.aof_fsync = FsyncPolicy::EverySec;
    })
    .await;
    let mut c = srv.client().await;
    let mut wire = Vec::new();
    for _ in 0..2000 {
        wire.extend(Client::encode(&["INCR", "counter"]));
    }
    c.send_raw(&wire).await;
    for _ in 0..2000 {
        c.read().await;
    }
    c.cmd(&["SET", "ttl", "v", "EX", "1000"]).await;
    // The writer thread drains asynchronously; wait for the 2000 INCRs to reach the file.
    let mut before = 0;
    for _ in 0..200 {
        before = info_field(&mut c, "persistence", "aof_current_size")
            .await
            .parse::<u64>()
            .unwrap();
        if before > 20_000 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(before > 20_000, "AOF only reached {before} bytes");
    assert!(matches!(c.cmd(&["BGREWRITEAOF"]).await, Frame::Simple(_)));
    wait_for_rewrite(&mut c, 1).await;
    let after: u64 = info_field(&mut c, "persistence", "aof_current_size")
        .await
        .parse()
        .unwrap();
    assert!(
        after < 200,
        "2000 INCRs must collapse into a single SET (was {before}, now {after})"
    );
    let pause: u64 = info_field(&mut c, "persistence", "aof_last_rewrite_pause_us")
        .await
        .parse()
        .unwrap();
    assert!(pause < 1_000_000, "stop-the-world dump took {pause}us");

    let dir = srv.stop().await;
    let restarted = TestServer::start_in(dir, |c| {
        c.aof_enabled = true;
        c.aof_fsync = FsyncPolicy::EverySec;
    })
    .await;
    let mut c = restarted.client().await;
    assert_eq!(c.cmd(&["GET", "counter"]).await, b("2000"));
    let Frame::Integer(ttl) = c.cmd(&["TTL", "ttl"]).await else {
        panic!()
    };
    assert!(ttl > 990);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rewrite_under_concurrent_writes_loses_nothing() {
    let srv = TestServer::start_with(|c| {
        c.aof_enabled = true;
        c.aof_fsync = FsyncPolicy::EverySec;
    })
    .await;
    let mut writers = Vec::new();
    for w in 0..4 {
        let mut c = srv.client().await;
        writers.push(tokio::spawn(async move {
            for i in 0..1500 {
                c.send(&["INCR", "shared"]).await;
                c.send(&["SET", &format!("w{w}:{i}"), "v"]).await;
                c.read().await;
                c.read().await;
            }
        }));
    }
    let mut admin = srv.client().await;
    // Rewrite several times while the writers are in full flight.
    for round in 1..=3 {
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(
            matches!(admin.cmd(&["BGREWRITEAOF"]).await, Frame::Simple(_)),
            "round {round}"
        );
        wait_for_rewrite(&mut admin, round).await;
    }
    for w in writers {
        w.await.unwrap();
    }
    assert_eq!(admin.cmd(&["GET", "shared"]).await, b("6000"));
    let dbsize = admin.cmd(&["DBSIZE"]).await;
    let dir = srv.stop().await;

    let restarted = TestServer::start_in(dir, |c| {
        c.aof_enabled = true;
        c.aof_fsync = FsyncPolicy::EverySec;
    })
    .await;
    let mut c = restarted.client().await;
    assert_eq!(
        c.cmd(&["GET", "shared"]).await,
        b("6000"),
        "no INCR may be lost or doubled by a rewrite"
    );
    assert_eq!(c.cmd(&["DBSIZE"]).await, dbsize);
}

#[tokio::test]
async fn only_one_rewrite_runs_at_a_time_and_it_requires_the_aof() {
    let off = TestServer::start().await;
    assert!(
        matches!(off.client().await.cmd(&["BGREWRITEAOF"]).await, Frame::Error(e) if e.contains("not enabled"))
    );

    let srv = TestServer::start_with(|c| {
        c.aof_enabled = true;
        c.aof_fsync = FsyncPolicy::No;
    })
    .await;
    let mut c = srv.client().await;
    let mut big = Vec::new();
    for i in 0..30_000 {
        big.extend(Client::encode(&[
            "SET",
            &format!("key:{i}"),
            &"v".repeat(64),
        ]));
    }
    c.send_raw(&big).await;
    for _ in 0..30_000 {
        c.read().await;
    }
    let first = c.cmd(&["BGREWRITEAOF"]).await;
    let second = c.cmd(&["BGREWRITEAOF"]).await;
    assert!(matches!(first, Frame::Simple(_)));
    // The rewrite of 30k keys is still running when the second request lands, or it
    // already finished; in the former case it must be refused rather than doubled up.
    if let Frame::Error(e) = second {
        assert!(e.contains("already in progress"), "{e}");
    }
    wait_for_rewrite(&mut c, 1).await;
}

// ---- snapshots ---------------------------------------------------------------------

async fn populate_every_type(c: &mut Client) {
    c.cmd(&["SET", "str", "hello", "EX", "5000"]).await;
    c.cmd(&["RPUSH", "list", "a", "b", "c"]).await;
    c.cmd(&["HSET", "hash", "f1", "v1", "f2", "v2"]).await;
    c.cmd(&["SADD", "set", "m1", "m2", "m3"]).await;
    c.cmd(&["ZADD", "zset", "1", "a", "2", "b", "-3.5", "c"])
        .await;
    c.cmd(&["SET", "binary", "x"]).await;
}

async fn check_every_type(c: &mut Client) {
    assert_eq!(c.cmd(&["GET", "str"]).await, b("hello"));
    let Frame::Integer(ttl) = c.cmd(&["TTL", "str"]).await else {
        panic!()
    };
    assert!((4990..=5000).contains(&ttl));
    assert_eq!(
        c.cmd(&["LRANGE", "list", "0", "-1"]).await,
        arr(&["a", "b", "c"])
    );
    assert_eq!(c.cmd(&["HGET", "hash", "f2"]).await, b("v2"));
    assert_eq!(c.cmd(&["SCARD", "set"]).await, int(3));
    assert_eq!(
        c.cmd(&["ZRANGE", "zset", "0", "-1", "WITHSCORES"]).await,
        arr(&["c", "-3.5", "a", "1", "b", "2"])
    );
    assert_eq!(c.cmd(&["DBSIZE"]).await, int(6));
}

#[tokio::test]
async fn save_and_restart_without_aof_restores_every_type() {
    let srv = TestServer::start().await;
    let mut c = srv.client().await;
    populate_every_type(&mut c).await;
    assert_eq!(c.cmd(&["SAVE"]).await, ok());
    let Frame::Integer(last) = c.cmd(&["LASTSAVE"]).await else {
        panic!()
    };
    assert!(last > 1_700_000_000);
    let dir = srv.stop().await;
    assert!(dir.path().join("dump.fdb").exists());

    let restarted = TestServer::start_in(dir, |_| {}).await;
    check_every_type(&mut restarted.client().await).await;
}

#[tokio::test]
async fn bgsave_writes_a_consistent_snapshot_while_writes_continue() {
    let srv = TestServer::start().await;
    let mut c = srv.client().await;
    populate_every_type(&mut c).await;
    assert!(matches!(c.cmd(&["BGSAVE"]).await, Frame::Simple(_)));
    // Mutations after BGSAVE returns must not leak into the snapshot.
    c.cmd(&["SET", "late", "x"]).await;
    for _ in 0..200 {
        if info_field(&mut c, "persistence", "save_in_progress").await == "0" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let dir = srv.stop().await;
    let restarted = TestServer::start_in(dir, |_| {}).await;
    let mut c = restarted.client().await;
    check_every_type(&mut c).await;
    assert_eq!(c.cmd(&["EXISTS", "late"]).await, int(0));
}

#[tokio::test]
async fn a_corrupt_snapshot_is_refused() {
    let srv = TestServer::start().await;
    let mut c = srv.client().await;
    populate_every_type(&mut c).await;
    c.cmd(&["SAVE"]).await;
    let dir = srv.stop().await;
    let path = dir.path().join("dump.fdb");
    let mut bytes = std::fs::read(&path).unwrap();
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0xFF;
    std::fs::write(&path, bytes).unwrap();
    let cfg = Config {
        port: 0,
        dir: dir.path().to_path_buf(),
        ..Config::default()
    };
    assert!(matches!(
        Engine::open(cfg, Arc::new(SystemClock)),
        Err(PersistError::Corrupt { .. })
    ));
}

#[tokio::test]
async fn enabling_aof_adopts_an_existing_snapshot() {
    let srv = TestServer::start().await;
    let mut c = srv.client().await;
    populate_every_type(&mut c).await;
    c.cmd(&["SAVE"]).await;
    let dir = srv.stop().await;

    let with_aof = TestServer::start_in(dir, |c| {
        c.aof_enabled = true;
        c.aof_fsync = FsyncPolicy::Always;
    })
    .await;
    let mut c = with_aof.client().await;
    check_every_type(&mut c).await;
    assert_eq!(c.cmd(&["SET", "new", "1"]).await, ok());
    let dir = with_aof.stop().await;
    std::fs::remove_file(dir.path().join("dump.fdb")).unwrap();

    // With the snapshot gone, the AOF alone must hold the full dataset.
    let again = TestServer::start_in(dir, |c| c.aof_enabled = true).await;
    let mut c = again.client().await;
    assert_eq!(c.cmd(&["DBSIZE"]).await, int(7));
    assert_eq!(c.cmd(&["GET", "new"]).await, b("1"));
    assert_eq!(c.cmd(&["ZSCORE", "zset", "c"]).await, b("-3.5"));
}

// ---- expiry / eviction / shutdown interplay ------------------------------------------

#[tokio::test]
async fn expired_keys_are_logged_as_deletes_so_replay_cannot_resurrect_them() {
    let srv = TestServer::start_with(|c| {
        c.aof_enabled = true;
        c.aof_fsync = FsyncPolicy::Always;
    })
    .await;
    let mut c = srv.client().await;
    c.cmd(&["SET", "active", "v", "PX", "80"]).await;
    c.cmd(&["SET", "lazy", "v", "PX", "80"]).await;
    c.cmd(&["SET", "keep", "v"]).await;
    eventually(
        Duration::from_secs(5),
        "the background task to reclaim both expired keys",
        || srv.engine.db().key_count() == 1,
    )
    .await;
    assert_eq!(c.cmd(&["GET", "lazy"]).await, Frame::Null);
    let dir = srv.stop().await;
    let log = std::fs::read_to_string(dir.path().join("appendonly.aof")).unwrap();
    assert!(
        log.matches("DEL").count() >= 2,
        "both expirations must be in the log: {log:?}"
    );

    let restarted = TestServer::start_in(dir, |c| {
        c.aof_enabled = true;
        c.aof_fsync = FsyncPolicy::Always;
    })
    .await;
    let mut c = restarted.client().await;
    assert_eq!(c.cmd(&["DBSIZE"]).await, int(1));
    assert_eq!(c.cmd(&["GET", "keep"]).await, b("v"));
}

#[tokio::test]
async fn a_key_that_expired_while_the_server_was_down_stays_gone() {
    let srv = TestServer::start_with(|c| {
        c.aof_enabled = true;
        c.aof_fsync = FsyncPolicy::Always;
    })
    .await;
    let mut c = srv.client().await;
    c.cmd(&["SET", "short", "v", "PX", "150"]).await;
    c.cmd(&["SET", "long", "v", "EX", "1000"]).await;
    // Stop immediately: the TTL is still in the future at shutdown.
    let dir = srv.stop().await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    let restarted = TestServer::start_in(dir, |c| {
        c.aof_enabled = true;
        c.aof_fsync = FsyncPolicy::Always;
    })
    .await;
    let mut c = restarted.client().await;
    assert_eq!(
        c.cmd(&["GET", "short"]).await,
        Frame::Null,
        "deadline passed during downtime"
    );
    assert_eq!(c.cmd(&["GET", "long"]).await, b("v"));
}

#[tokio::test]
async fn evictions_are_logged_so_recovery_matches_the_live_dataset() {
    let cfg = |c: &mut Config| {
        c.aof_enabled = true;
        c.aof_fsync = FsyncPolicy::Always;
        c.maxmemory = 64 * 1024;
        c.maxmemory_policy = EvictionPolicy::AllKeysLru;
    };
    let srv = TestServer::start_with(cfg).await;
    let mut c = srv.client().await;
    for i in 0..1000 {
        assert_eq!(
            c.cmd(&["SET", &format!("k{i}"), &"v".repeat(100)]).await,
            ok()
        );
    }
    let Frame::Integer(live) = c.cmd(&["DBSIZE"]).await else {
        panic!()
    };
    assert!(live < 1000, "eviction should have happened");
    let dir = srv.stop().await;
    let restarted = TestServer::start_in(dir, cfg).await;
    assert_eq!(restarted.client().await.cmd(&["DBSIZE"]).await, int(live));
}

#[tokio::test]
async fn graceful_shutdown_flushes_everysec_writes() {
    let srv = TestServer::start_with(|c| {
        c.aof_enabled = true;
        c.aof_fsync = FsyncPolicy::EverySec;
    })
    .await;
    let mut c = srv.client().await;
    for i in 0..100 {
        c.cmd(&["SET", &format!("k{i}"), "v"]).await;
    }
    let dir = srv.stop().await;
    let restarted = TestServer::start_in(dir, |c| c.aof_enabled = true).await;
    assert_eq!(restarted.client().await.cmd(&["DBSIZE"]).await, int(100));
}

#[tokio::test]
async fn flushall_is_durable() {
    let srv = TestServer::start_with(|c| {
        c.aof_enabled = true;
        c.aof_fsync = FsyncPolicy::Always;
    })
    .await;
    let mut c = srv.client().await;
    c.cmd(&["SET", "a", "1"]).await;
    c.cmd(&["FLUSHALL"]).await;
    c.cmd(&["SET", "b", "2"]).await;
    let dir = srv.stop().await;
    let restarted = TestServer::start_in(dir, |c| c.aof_enabled = true).await;
    let mut c = restarted.client().await;
    assert_eq!(c.cmd(&["EXISTS", "a"]).await, int(0));
    assert_eq!(c.cmd(&["GET", "b"]).await, b("2"));
}
