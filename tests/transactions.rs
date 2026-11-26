//! MULTI / EXEC / WATCH semantics, including concurrent optimistic locking.
#![allow(missing_docs)]

mod common;

use common::*;
use ferrum_db::resp::Frame;

fn queued() -> Frame {
    Frame::simple("QUEUED")
}

#[tokio::test]
async fn exec_runs_queued_commands_atomically_and_returns_every_reply() {
    let srv = TestServer::start().await;
    let mut c = srv.client().await;
    assert_eq!(c.cmd(&["MULTI"]).await, ok());
    assert_eq!(c.cmd(&["SET", "a", "1"]).await, queued());
    assert_eq!(c.cmd(&["INCR", "a"]).await, queued());
    assert_eq!(c.cmd(&["LPUSH", "a", "x"]).await, queued());
    assert_eq!(c.cmd(&["GET", "a"]).await, queued());
    // A runtime error inside EXEC does not abort the other commands.
    let reply = c.cmd(&["EXEC"]).await;
    let Frame::Array(items) = reply else {
        panic!("array expected")
    };
    assert_eq!(items[0], ok());
    assert_eq!(items[1], int(2));
    assert!(matches!(&items[2], Frame::Error(e) if e.starts_with("WRONGTYPE")));
    assert_eq!(items[3], b("2"));
}

#[tokio::test]
async fn queueing_errors_abort_the_whole_transaction() {
    let srv = TestServer::start().await;
    let mut c = srv.client().await;
    c.cmd(&["MULTI"]).await;
    assert_eq!(c.cmd(&["SET", "k", "v"]).await, queued());
    assert!(matches!(c.cmd(&["NOSUCHCOMMAND"]).await, Frame::Error(_)));
    assert!(matches!(c.cmd(&["GET"]).await, Frame::Error(e) if e.contains("wrong number")));
    assert!(matches!(c.cmd(&["EXEC"]).await, Frame::Error(e) if e.starts_with("EXECABORT")));
    assert_eq!(
        c.cmd(&["GET", "k"]).await,
        Frame::Null,
        "nothing from the aborted block ran"
    );
}

#[tokio::test]
async fn control_command_misuse() {
    let srv = TestServer::start().await;
    let mut c = srv.client().await;
    assert!(matches!(c.cmd(&["EXEC"]).await, Frame::Error(e) if e.contains("without MULTI")));
    assert!(matches!(c.cmd(&["DISCARD"]).await, Frame::Error(e) if e.contains("without MULTI")));
    c.cmd(&["MULTI"]).await;
    assert!(matches!(c.cmd(&["MULTI"]).await, Frame::Error(e) if e.contains("nested")));
    assert!(matches!(c.cmd(&["WATCH", "k"]).await, Frame::Error(e) if e.contains("inside MULTI")));
    assert!(
        matches!(c.cmd(&["SAVE"]).await, Frame::Error(e) if e.contains("not allowed inside MULTI"))
    );
    assert!(matches!(c.cmd(&["EXEC"]).await, Frame::Error(e) if e.starts_with("EXECABORT")));
    c.cmd(&["MULTI"]).await;
    c.cmd(&["SET", "k", "v"]).await;
    assert_eq!(c.cmd(&["DISCARD"]).await, ok());
    assert_eq!(c.cmd(&["GET", "k"]).await, Frame::Null);
}

#[tokio::test]
async fn watch_aborts_when_another_client_touches_the_key() {
    let srv = TestServer::start().await;
    let mut a = srv.client().await;
    let mut other = srv.client().await;
    a.cmd(&["SET", "k", "1"]).await;
    assert_eq!(a.cmd(&["WATCH", "k"]).await, ok());
    assert_eq!(other.cmd(&["SET", "k", "2"]).await, ok());
    a.cmd(&["MULTI"]).await;
    a.cmd(&["SET", "k", "mine"]).await;
    assert_eq!(
        a.cmd(&["EXEC"]).await,
        Frame::NullArray,
        "RESP2 aborted EXEC is a null array"
    );
    assert_eq!(a.cmd(&["GET", "k"]).await, b("2"));

    // Watches are cleared by EXEC: the next transaction is unaffected.
    a.cmd(&["MULTI"]).await;
    a.cmd(&["SET", "k", "again"]).await;
    assert!(matches!(a.cmd(&["EXEC"]).await, Frame::Array(_)));
}

#[tokio::test]
async fn watch_detects_every_kind_of_modification() {
    let srv = TestServer::start().await;
    let mut a = srv.client().await;
    let mut other = srv.client().await;
    let cases: [&[&str]; 6] = [
        &["SET", "w", "x"],
        &["DEL", "w"],
        &["EXPIRE", "w", "100"],
        &["RENAME", "w", "w2"],
        &["LPUSH", "wl", "x"],
        &["FLUSHALL"],
    ];
    for modify in cases {
        other.cmd(&["SET", "w", "seed"]).await;
        other.cmd(&["RPUSH", "wl", "seed"]).await;
        let key = if modify[0] == "LPUSH" { "wl" } else { "w" };
        a.cmd(&["WATCH", key]).await;
        other.cmd(modify).await;
        a.cmd(&["MULTI"]).await;
        a.cmd(&["PING"]).await;
        assert_eq!(
            a.cmd(&["EXEC"]).await,
            Frame::NullArray,
            "{modify:?} must invalidate the watch"
        );
    }
}

#[tokio::test]
async fn watching_a_missing_key_then_creating_it_aborts() {
    let srv = TestServer::start().await;
    let mut a = srv.client().await;
    let mut other = srv.client().await;
    a.cmd(&["WATCH", "ghost"]).await;
    other.cmd(&["SET", "ghost", "boo"]).await;
    a.cmd(&["MULTI"]).await;
    assert_eq!(a.cmd(&["EXEC"]).await, Frame::NullArray);
}

#[tokio::test]
async fn unwatch_and_disconnect_release_watches() {
    let srv = TestServer::start().await;
    let mut a = srv.client().await;
    let mut other = srv.client().await;
    a.cmd(&["WATCH", "k"]).await;
    assert_eq!(a.cmd(&["UNWATCH"]).await, ok());
    other.cmd(&["SET", "k", "v"]).await;
    a.cmd(&["MULTI"]).await;
    assert!(matches!(a.cmd(&["EXEC"]).await, Frame::Array(_)));

    let mut watcher = srv.client().await;
    watcher.cmd(&["WATCH", "leak"]).await;
    let idx = srv.engine.db().shard_index(b"leak");
    watcher.cmd(&["PING"]).await;
    drop(watcher);
    // After the connection closes the registration must be gone (version resets to 0).
    for _ in 0..50 {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let mut locked = srv.engine.db().lock_shard(idx);
        locked.shard_by_index(idx).unwrap().insert(
            bytes("leak"),
            ferrum_db::db::Value::Str(bytes("v")),
            None,
            0,
        );
        if locked.shard_by_index(idx).unwrap().version(b"leak") == 0 {
            return;
        }
    }
    panic!("watch registration leaked after disconnect");
}

#[tokio::test]
async fn resp3_aborted_exec_is_a_null() {
    let srv = TestServer::start().await;
    let mut a = srv.client().await;
    let mut other = srv.client().await;
    a.cmd(&["HELLO", "3"]).await;
    a.cmd(&["WATCH", "k"]).await;
    other.cmd(&["SET", "k", "v"]).await;
    a.cmd(&["MULTI"]).await;
    assert_eq!(a.cmd(&["EXEC"]).await, Frame::Null);
}

/// Optimistic-locking counter: many clients retry WATCH/GET/MULTI/SET/EXEC until it
/// commits. Lost updates would make the final value smaller than the commit count.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_check_and_set_never_loses_an_update() {
    let srv = TestServer::start().await;
    const CLIENTS: usize = 8;
    const INCREMENTS: usize = 100;
    let mut tasks = Vec::new();
    for _ in 0..CLIENTS {
        let mut c = srv.client().await;
        tasks.push(tokio::spawn(async move {
            let mut retries = 0usize;
            for _ in 0..INCREMENTS {
                loop {
                    c.cmd(&["WATCH", "counter"]).await;
                    let current = match c.cmd(&["GET", "counter"]).await {
                        Frame::Bulk(v) => String::from_utf8(v.to_vec())
                            .unwrap()
                            .parse::<i64>()
                            .unwrap(),
                        _ => 0,
                    };
                    c.cmd(&["MULTI"]).await;
                    c.cmd(&["SET", "counter", &(current + 1).to_string()]).await;
                    if matches!(c.cmd(&["EXEC"]).await, Frame::Array(_)) {
                        break;
                    }
                    retries += 1;
                }
            }
            retries
        }));
    }
    let mut retries = 0;
    for t in tasks {
        retries += t.await.unwrap();
    }
    let mut c = srv.client().await;
    assert_eq!(
        c.cmd(&["GET", "counter"]).await,
        b(&(CLIENTS * INCREMENTS).to_string())
    );
    eprintln!("optimistic-lock retries under contention: {retries}");
}

/// A transaction must be isolated: concurrent readers running their own MULTI blocks
/// never observe the two halves of a transfer out of sync.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transactions_are_isolated_from_each_other() {
    let srv = TestServer::start().await;
    let mut setup = srv.client().await;
    // Keys chosen to land on different shards so isolation is not a single-mutex accident.
    let (from, to) = (0..64)
        .map(|i| (format!("acct:{i}"), format!("acct:{}", i + 100)))
        .find(|(a, b)| {
            srv.engine.db().shard_index(a.as_bytes()) != srv.engine.db().shard_index(b.as_bytes())
        })
        .unwrap();
    setup.cmd(&["SET", &from, "1000"]).await;
    setup.cmd(&["SET", &to, "0"]).await;

    let writer = {
        let mut c = srv.client().await;
        let (from, to) = (from.clone(), to.clone());
        tokio::spawn(async move {
            for _ in 0..300 {
                c.cmd(&["MULTI"]).await;
                c.cmd(&["DECRBY", &from, "1"]).await;
                c.cmd(&["INCRBY", &to, "1"]).await;
                assert!(matches!(c.cmd(&["EXEC"]).await, Frame::Array(_)));
            }
        })
    };
    let mut readers = Vec::new();
    for _ in 0..3 {
        let mut c = srv.client().await;
        let (from, to) = (from.clone(), to.clone());
        readers.push(tokio::spawn(async move {
            for _ in 0..300 {
                c.cmd(&["MULTI"]).await;
                c.cmd(&["GET", &from]).await;
                c.cmd(&["GET", &to]).await;
                let Frame::Array(items) = c.cmd(&["EXEC"]).await else {
                    panic!()
                };
                let n = |f: &Frame| match f {
                    Frame::Bulk(v) => String::from_utf8_lossy(v).parse::<i64>().unwrap(),
                    _ => panic!("value expected"),
                };
                assert_eq!(
                    n(&items[0]) + n(&items[1]),
                    1000,
                    "observed a torn transfer"
                );
            }
        }));
    }
    writer.await.unwrap();
    for r in readers {
        r.await.unwrap();
    }
}
