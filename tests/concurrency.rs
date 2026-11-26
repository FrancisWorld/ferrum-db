//! Cross-shard atomicity and lost-update checks under real parallelism.
#![allow(missing_docs)]

mod common;

use common::*;
use ferrum_db::resp::Frame;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_increments_are_never_lost() {
    let srv = TestServer::start().await;
    const CLIENTS: usize = 16;
    const OPS: usize = 250;
    let mut tasks = Vec::new();
    for _ in 0..CLIENTS {
        let mut c = srv.client().await;
        tasks.push(tokio::spawn(async move {
            for _ in 0..OPS {
                c.send(&["INCR", "shared"]).await;
            }
            for _ in 0..OPS {
                assert!(matches!(c.read().await, Frame::Integer(_)));
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    assert_eq!(
        srv.client().await.cmd(&["GET", "shared"]).await,
        b(&(CLIENTS * OPS).to_string())
    );
}

/// MSET touches several shards; MGET must never observe a half-applied MSET.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_key_commands_are_atomic_across_shards() {
    let srv = TestServer::start().await;
    let keys: Vec<String> = (0..8).map(|i| format!("mk:{i}")).collect();
    let shards: std::collections::HashSet<_> = keys
        .iter()
        .map(|k| srv.engine.db().shard_index(k.as_bytes()))
        .collect();
    assert!(shards.len() > 1, "test needs keys on several shards");

    let writer = {
        let mut c = srv.client().await;
        let keys = keys.clone();
        tokio::spawn(async move {
            for round in 0..400 {
                let mut args: Vec<String> = vec!["MSET".into()];
                for k in &keys {
                    args.push(k.clone());
                    args.push(round.to_string());
                }
                let refs: Vec<&str> = args.iter().map(String::as_str).collect();
                assert_eq!(c.cmd(&refs).await, ok());
            }
        })
    };
    let mut readers = Vec::new();
    for _ in 0..3 {
        let mut c = srv.client().await;
        let keys = keys.clone();
        readers.push(tokio::spawn(async move {
            let mut args = vec!["MGET"];
            args.extend(keys.iter().map(String::as_str));
            for _ in 0..400 {
                let Frame::Array(items) = c.cmd(&args).await else {
                    panic!()
                };
                let first = &items[0];
                assert!(items.iter().all(|i| i == first), "torn read: {items:?}");
            }
        }));
    }
    writer.await.unwrap();
    for r in readers {
        r.await.unwrap();
    }
}

/// LMOVE between lists that live on different shards must conserve elements even
/// when many clients shuffle them in both directions at once (this is also a
/// deadlock check: opposite lock orders would hang here).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_shard_moves_conserve_elements_and_never_deadlock() {
    let srv = TestServer::start().await;
    let (a, b2) = (0..64)
        .map(|i| (format!("la:{i}"), format!("lb:{i}")))
        .find(|(x, y)| {
            srv.engine.db().shard_index(x.as_bytes()) != srv.engine.db().shard_index(y.as_bytes())
        })
        .unwrap();
    let mut setup = srv.client().await;
    let mut push = vec!["RPUSH", a.as_str()];
    let items: Vec<String> = (0..100).map(|i| i.to_string()).collect();
    push.extend(items.iter().map(String::as_str));
    setup.cmd(&push).await;

    let mut tasks = Vec::new();
    for dir in 0..8 {
        let mut c = srv.client().await;
        let (src, dst) = if dir % 2 == 0 {
            (a.clone(), b2.clone())
        } else {
            (b2.clone(), a.clone())
        };
        tasks.push(tokio::spawn(async move {
            for _ in 0..300 {
                c.cmd(&["LMOVE", &src, &dst, "LEFT", "RIGHT"]).await;
            }
        }));
    }
    let all = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        for t in tasks {
            t.await.unwrap();
        }
    })
    .await;
    assert!(all.is_ok(), "deadlock between cross-shard commands");
    let Frame::Integer(la) = setup.cmd(&["LLEN", &a]).await else {
        panic!()
    };
    let Frame::Integer(lb) = setup.cmd(&["LLEN", &b2]).await else {
        panic!()
    };
    assert_eq!(la + lb, 100);
}

/// Active expiry, eviction-free writers and readers all hammer the same shards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expiry_running_alongside_writers_keeps_the_keyspace_consistent() {
    let srv = TestServer::start().await;
    let mut tasks = Vec::new();
    for t in 0..6 {
        let mut c = srv.client().await;
        tasks.push(tokio::spawn(async move {
            for i in 0..300 {
                let key = format!("t{t}:{i}");
                c.cmd(&["SET", &key, "v", "PX", "30"]).await;
                c.cmd(&["GET", &key]).await;
                c.cmd(&["SET", &format!("{key}:persist"), "v"]).await;
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    let mut c = srv.client().await;
    assert_eq!(
        c.cmd(&["DBSIZE"]).await,
        int(6 * 300),
        "every volatile key must be reclaimed exactly once"
    );
    let counters = srv.engine.db().counters();
    assert_eq!(
        counters
            .volatile_keys
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}
