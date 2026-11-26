//! maxmemory enforcement and approximated-LRU eviction.
#![allow(missing_docs)]

mod common;

use std::sync::Arc;

use common::*;
use ferrum_db::clock::ManualClock;
use ferrum_db::resp::Frame;

fn info_value(text: &str, field: &str) -> u64 {
    text.lines()
        .find_map(|l| l.strip_prefix(&format!("{field}:")))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or_else(|| panic!("{field} missing"))
}

async fn info(c: &mut Client, section: &str) -> String {
    let Frame::Bulk(text) = c.cmd(&["INFO", section]).await else {
        panic!("bulk expected")
    };
    String::from_utf8_lossy(&text).to_string()
}

#[tokio::test]
async fn noeviction_rejects_writes_but_still_serves_reads_and_deletes() {
    let srv = TestServer::start_with(|c| c.maxmemory = 64 * 1024).await;
    let mut c = srv.client().await;
    let value = "v".repeat(512);
    let mut stored = 0;
    loop {
        match c.cmd(&["SET", &format!("key:{stored}"), &value]).await {
            Frame::Simple(_) => stored += 1,
            Frame::Error(e) => {
                assert!(e.starts_with("OOM"), "{e}");
                break;
            }
            other => panic!("{other:?}"),
        }
        assert!(stored < 10_000, "limit was never enforced");
    }
    assert!(stored > 10, "should hold a decent amount before refusing");
    assert_eq!(
        c.cmd(&["GET", "key:0"]).await,
        b(&value),
        "reads keep working"
    );
    assert!(
        matches!(c.cmd(&["LPUSH", "l", &"x".repeat(4096)]).await, Frame::Error(e) if e.starts_with("OOM"))
    );
    assert_eq!(
        c.cmd(&["DEL", "key:0", "key:1", "key:2", "key:3"]).await,
        int(4),
        "deletes are allowed and free memory"
    );
    assert_eq!(c.cmd(&["SET", "after", "ok"]).await, ok());
}

#[tokio::test]
async fn allkeys_lru_keeps_memory_bounded_and_prefers_cold_keys() {
    let clock = Arc::new(ManualClock::new(1_000_000));
    let limit = 128 * 1024;
    let srv = TestServer::start_clocked(clock.clone(), |c| {
        c.maxmemory = limit;
        c.maxmemory_policy = ferrum_db::db::EvictionPolicy::AllKeysLru;
    })
    .await;
    let mut c = srv.client().await;
    let value = "v".repeat(200);
    assert_eq!(c.cmd(&["SET", "hot", &value]).await, ok());
    for i in 0..3000 {
        clock.advance(1);
        assert_eq!(c.cmd(&["SET", &format!("cold:{i}"), &value]).await, ok());
        if i % 5 == 0 {
            // Reading refreshes the LRU clock.
            assert_eq!(
                c.cmd(&["GET", "hot"]).await,
                b(&value),
                "hot key was evicted at write {i}"
            );
        }
    }
    let text = info(&mut c, "memory").await;
    // Eviction runs before a write, so the final write may overshoot by one entry.
    assert!(
        info_value(&text, "used_memory") <= limit as u64 + 1024,
        "{text}"
    );
    let stats = info(&mut c, "stats").await;
    let evicted = info_value(&stats, "evicted_keys");
    assert!(evicted > 1000, "expected heavy eviction, got {evicted}");
    // The oldest cold keys are the ones that went away; the newest are still there.
    assert_eq!(c.cmd(&["GET", "cold:2999"]).await, b(&value));
    assert_eq!(c.cmd(&["EXISTS", "cold:0"]).await, int(0));
}

#[tokio::test]
async fn volatile_lru_only_evicts_keys_with_a_ttl() {
    let srv = TestServer::start_with(|c| {
        c.maxmemory = 32 * 1024;
        c.maxmemory_policy = ferrum_db::db::EvictionPolicy::VolatileLru;
    })
    .await;
    let mut c = srv.client().await;
    let value = "v".repeat(300);
    for i in 0..20 {
        assert_eq!(c.cmd(&["SET", &format!("keep:{i}"), &value]).await, ok());
    }
    let mut volatile_ok = 0;
    for i in 0..500 {
        if c.cmd(&["SET", &format!("tmp:{i}"), &value, "EX", "3600"])
            .await
            == ok()
        {
            volatile_ok += 1;
        }
    }
    assert_eq!(
        volatile_ok, 500,
        "volatile keys can always make room for each other"
    );
    for i in 0..20 {
        assert_eq!(
            c.cmd(&["EXISTS", &format!("keep:{i}")]).await,
            int(1),
            "persistent keys must never be evicted"
        );
    }
    // With only persistent keys left to evict, writes fail instead.
    for i in 0..500 {
        let _ = c.cmd(&["DEL", &format!("tmp:{i}")]).await;
    }
    let mut refused = false;
    for i in 0..200 {
        if matches!(c.cmd(&["SET", &format!("more:{i}"), &value]).await, Frame::Error(e) if e.starts_with("OOM"))
        {
            refused = true;
            break;
        }
    }
    assert!(refused);
}

#[tokio::test]
async fn maxmemory_can_be_changed_at_runtime() {
    let srv = TestServer::start().await;
    let mut c = srv.client().await;
    let value = "v".repeat(1000);
    for i in 0..200 {
        c.cmd(&["SET", &format!("k{i}"), &value]).await;
    }
    assert_eq!(
        c.cmd(&["CONFIG", "SET", "maxmemory-policy", "allkeys-lru"])
            .await,
        ok()
    );
    assert_eq!(c.cmd(&["CONFIG", "SET", "maxmemory", "50kb"]).await, ok());
    assert_eq!(c.cmd(&["SET", "trigger", "x"]).await, ok());
    let text = info(&mut c, "memory").await;
    assert!(
        info_value(&text, "used_memory") <= 50 * 1024 + 1024,
        "{text}"
    );
    assert_eq!(
        c.cmd(&["CONFIG", "GET", "maxmemory"]).await,
        arr(&["maxmemory", "51200"])
    );
    assert!(matches!(
        c.cmd(&["CONFIG", "SET", "maxmemory-policy", "bogus"]).await,
        Frame::Error(_)
    ));
    assert!(
        matches!(c.cmd(&["CONFIG", "SET", "port", "1"]).await, Frame::Error(e) if e.contains("not settable"))
    );
}

#[tokio::test]
async fn memory_accounting_returns_to_baseline_after_deleting_everything() {
    let srv = TestServer::start().await;
    let mut c = srv.client().await;
    let baseline = info_value(&info(&mut c, "memory").await, "used_memory");
    c.cmd(&["RPUSH", "l", "a", "b", "c"]).await;
    c.cmd(&["HSET", "h", "f", "v"]).await;
    c.cmd(&["SADD", "s", "m"]).await;
    c.cmd(&["ZADD", "z", "1", "m", "2", "n"]).await;
    c.cmd(&["SET", "k", &"x".repeat(1000)]).await;
    c.cmd(&["APPEND", "k", &"y".repeat(1000)]).await;
    assert!(info_value(&info(&mut c, "memory").await, "used_memory") > baseline + 2000);
    c.cmd(&["LPOP", "l", "3"]).await;
    c.cmd(&["HDEL", "h", "f"]).await;
    c.cmd(&["SREM", "s", "m"]).await;
    c.cmd(&["ZPOPMAX", "z", "5"]).await;
    c.cmd(&["DEL", "k"]).await;
    assert_eq!(
        info_value(&info(&mut c, "memory").await, "used_memory"),
        baseline
    );
    assert_eq!(c.cmd(&["DBSIZE"]).await, int(0));
}

const LIMIT: usize = 64 * 1024;

fn used(srv: &TestServer) -> usize {
    srv.engine.db().used_memory()
}

/// maxmemory is enforced against the command's own size, not only the state before it:
/// a multi-key write that cannot fit is refused whole instead of overshooting.
#[tokio::test]
async fn mset_cannot_overshoot_maxmemory() {
    let srv = TestServer::start_with(|c| c.maxmemory = LIMIT).await;
    let mut c = srv.client().await;
    let chunk = "v".repeat(10_000);
    let mut refused = false;
    for i in 0..100 {
        let (a, b, d) = (format!("a{i}"), format!("b{i}"), format!("c{i}"));
        let reply = c.cmd(&["MSET", &a, &chunk, &b, &chunk, &d, &chunk]).await;
        assert!(
            used(&srv) <= LIMIT,
            "used {} exceeded the limit after write {i}",
            used(&srv)
        );
        if matches!(&reply, Frame::Error(e) if e.starts_with("OOM")) {
            refused = true;
            // Nothing from the refused MSET may be applied.
            assert_eq!(c.cmd(&["EXISTS", &a, &b, &d]).await, int(0));
            break;
        }
        assert_eq!(reply, ok());
    }
    assert!(
        refused,
        "a 30 KB MSET must eventually be refused under a 64 KB limit"
    );
}

#[tokio::test]
async fn exec_cannot_overshoot_maxmemory() {
    let srv = TestServer::start_with(|c| c.maxmemory = LIMIT).await;
    let mut c = srv.client().await;
    let chunk = "v".repeat(5_000);
    c.cmd(&["MULTI"]).await;
    for i in 0..30 {
        c.cmd(&["SET", &format!("k{i}"), &chunk]).await; // 150 KB queued
    }
    assert!(matches!(c.cmd(&["EXEC"]).await, Frame::Error(e) if e.starts_with("OOM")));
    assert!(used(&srv) <= LIMIT);
    assert_eq!(
        c.cmd(&["DBSIZE"]).await,
        int(0),
        "an OOM-rejected transaction applies nothing"
    );
    // The connection and its watches are usable afterwards.
    assert_eq!(c.cmd(&["SET", "small", "x"]).await, ok());
}

#[tokio::test]
async fn oversized_request_is_refused_without_evicting_everything() {
    let srv = TestServer::start_with(|c| {
        c.maxmemory = LIMIT;
        c.maxmemory_policy = ferrum_db::db::EvictionPolicy::AllKeysLru;
    })
    .await;
    let mut c = srv.client().await;
    for i in 0..20 {
        c.cmd(&["SET", &format!("k{i}"), "v"]).await;
    }
    let huge = "v".repeat(LIMIT + 1);
    assert!(matches!(c.cmd(&["SET", "big", &huge]).await, Frame::Error(e) if e.starts_with("OOM")));
    assert_eq!(
        c.cmd(&["DBSIZE"]).await,
        int(20),
        "a request that can never fit must not wipe the dataset"
    );

    // A request that fits only after eviction succeeds and stays under the limit.
    let big = "v".repeat(LIMIT / 2);
    for i in 0..10 {
        assert_eq!(
            c.cmd(&[
                "MSET",
                &format!("m{i}"),
                &big,
                &format!("n{i}"),
                &"w".repeat(100)
            ])
            .await,
            ok()
        );
        assert!(used(&srv) <= LIMIT, "used {} after write {i}", used(&srv));
    }
}
