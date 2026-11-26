//! End-to-end command behaviour over a real TCP connection.
#![allow(missing_docs)]

mod common;

use common::*;
use ferrum_db::resp::Frame;

#[tokio::test]
async fn strings_and_counters() {
    let srv = TestServer::start().await;
    let mut c = srv.client().await;
    assert_eq!(c.cmd(&["PING"]).await, Frame::simple("PONG"));
    assert_eq!(c.cmd(&["SET", "k", "v"]).await, ok());
    assert_eq!(c.cmd(&["GET", "k"]).await, b("v"));
    assert_eq!(c.cmd(&["GET", "missing"]).await, Frame::Null);
    assert_eq!(c.cmd(&["SET", "k", "w", "NX"]).await, Frame::Null);
    assert_eq!(c.cmd(&["SET", "k", "w", "XX", "GET"]).await, b("v"));
    assert_eq!(c.cmd(&["APPEND", "k", "!!"]).await, int(3));
    assert_eq!(c.cmd(&["GETRANGE", "k", "0", "1"]).await, b("w!"));
    assert_eq!(c.cmd(&["STRLEN", "k"]).await, int(3));
    assert_eq!(c.cmd(&["INCR", "n"]).await, int(1));
    assert_eq!(c.cmd(&["INCRBY", "n", "41"]).await, int(42));
    assert_eq!(c.cmd(&["DECRBY", "n", "2"]).await, int(40));
    assert_eq!(c.cmd(&["INCRBYFLOAT", "f", "1.5"]).await, b("1.5"));
    assert_eq!(c.cmd(&["MSET", "a", "1", "b", "2"]).await, ok());
    assert_eq!(
        c.cmd(&["MGET", "a", "nope", "b"]).await,
        Frame::Array(vec![b("1"), Frame::Null, b("2")])
    );
    assert_eq!(c.cmd(&["MSETNX", "a", "x", "z", "y"]).await, int(0));
    assert_eq!(c.cmd(&["EXISTS", "z"]).await, int(0));
    assert_eq!(c.cmd(&["GETDEL", "a"]).await, b("1"));
    assert_eq!(c.cmd(&["SETRANGE", "s", "3", "ab"]).await, int(5));
    assert_eq!(
        c.cmd(&["GET", "s"]).await,
        Frame::Bulk(bytes::Bytes::from_static(b"\0\0\0ab"))
    );
    assert_eq!(c.cmd(&["TYPE", "k"]).await, Frame::simple("string"));
    assert_eq!(c.cmd(&["TYPE", "none"]).await, Frame::simple("none"));
}

#[tokio::test]
async fn errors_are_structured() {
    let srv = TestServer::start().await;
    let mut c = srv.client().await;
    let err = |f: Frame| match f {
        Frame::Error(e) => e,
        other => panic!("expected error, got {other:?}"),
    };
    assert!(err(c.cmd(&["NOPE"]).await).starts_with("ERR unknown command 'NOPE'"));
    assert!(err(c.cmd(&["GET"]).await).starts_with("ERR wrong number of arguments for 'get'"));
    c.cmd(&["SET", "s", "abc"]).await;
    assert!(err(c.cmd(&["INCR", "s"]).await).contains("not an integer"));
    assert!(err(c.cmd(&["LPUSH", "s", "x"]).await).starts_with("WRONGTYPE"));
    c.cmd(&["SET", "big", "9223372036854775807"]).await;
    assert!(err(c.cmd(&["INCR", "big"]).await).contains("overflow"));
    assert!(err(c.cmd(&["SET", "k", "v", "EX", "0"]).await).contains("invalid expire time"));
    assert!(err(c.cmd(&["SET", "k", "v", "NX", "XX"]).await).contains("syntax"));
    assert!(err(c.cmd(&["SELECT", "3"]).await).contains("out of range"));
    // The connection survives all of the above.
    assert_eq!(c.cmd(&["PING"]).await, Frame::simple("PONG"));
}

#[tokio::test]
async fn lists() {
    let srv = TestServer::start().await;
    let mut c = srv.client().await;
    assert_eq!(c.cmd(&["RPUSH", "l", "a", "b", "c"]).await, int(3));
    assert_eq!(c.cmd(&["LPUSH", "l", "z"]).await, int(4));
    assert_eq!(
        c.cmd(&["LRANGE", "l", "0", "-1"]).await,
        arr(&["z", "a", "b", "c"])
    );
    assert_eq!(c.cmd(&["LINDEX", "l", "-1"]).await, b("c"));
    assert_eq!(c.cmd(&["LSET", "l", "1", "A"]).await, ok());
    assert_eq!(c.cmd(&["LPOP", "l"]).await, b("z"));
    assert_eq!(c.cmd(&["RPOP", "l", "2"]).await, arr(&["c", "b"]));
    assert_eq!(c.cmd(&["LLEN", "l"]).await, int(1));
    assert_eq!(
        c.cmd(&["RPUSH", "r", "x", "y", "x", "z", "x"]).await,
        int(5)
    );
    assert_eq!(c.cmd(&["LREM", "r", "-2", "x"]).await, int(2));
    assert_eq!(
        c.cmd(&["LRANGE", "r", "0", "-1"]).await,
        arr(&["x", "y", "z"])
    );
    assert_eq!(c.cmd(&["LTRIM", "r", "1", "1"]).await, ok());
    assert_eq!(c.cmd(&["LRANGE", "r", "0", "-1"]).await, arr(&["y"]));
    assert_eq!(c.cmd(&["LMOVE", "r", "dst", "LEFT", "RIGHT"]).await, b("y"));
    assert_eq!(
        c.cmd(&["EXISTS", "r"]).await,
        int(0),
        "empty lists are deleted"
    );
    assert_eq!(c.cmd(&["LPOP", "nope"]).await, Frame::Null);
    assert_eq!(c.cmd(&["LPOP", "nope", "2"]).await, Frame::NullArray);
}

#[tokio::test]
async fn hashes() {
    let srv = TestServer::start().await;
    let mut c = srv.client().await;
    assert_eq!(c.cmd(&["HSET", "h", "a", "1", "b", "2"]).await, int(2));
    assert_eq!(c.cmd(&["HSET", "h", "a", "9"]).await, int(0));
    assert_eq!(c.cmd(&["HGET", "h", "a"]).await, b("9"));
    assert_eq!(
        c.cmd(&["HMGET", "h", "a", "x"]).await,
        Frame::Array(vec![b("9"), Frame::Null])
    );
    assert_eq!(c.cmd(&["HINCRBY", "h", "b", "5"]).await, int(7));
    assert_eq!(c.cmd(&["HGETALL", "h"]).await, arr(&["a", "9", "b", "7"]));
    assert_eq!(c.cmd(&["HLEN", "h"]).await, int(2));
    assert_eq!(c.cmd(&["HDEL", "h", "a", "b"]).await, int(2));
    assert_eq!(c.cmd(&["EXISTS", "h"]).await, int(0));
}

#[tokio::test]
async fn sets() {
    let srv = TestServer::start().await;
    let mut c = srv.client().await;
    assert_eq!(c.cmd(&["SADD", "a", "1", "2", "3"]).await, int(3));
    assert_eq!(c.cmd(&["SADD", "b", "3", "4"]).await, int(1 + 1));
    assert_eq!(c.cmd(&["SISMEMBER", "a", "2"]).await, int(1));
    assert_eq!(c.cmd(&["SCARD", "a"]).await, int(3));
    let sorted = |f: Frame| {
        let Frame::Array(items) = f else {
            panic!("array")
        };
        let mut v: Vec<String> = items
            .into_iter()
            .map(|i| match i {
                Frame::Bulk(b) => String::from_utf8(b.to_vec()).unwrap(),
                _ => panic!(),
            })
            .collect();
        v.sort();
        v
    };
    assert_eq!(sorted(c.cmd(&["SINTER", "a", "b"]).await), vec!["3"]);
    assert_eq!(
        sorted(c.cmd(&["SUNION", "a", "b"]).await),
        vec!["1", "2", "3", "4"]
    );
    assert_eq!(sorted(c.cmd(&["SDIFF", "a", "b"]).await), vec!["1", "2"]);
    assert_eq!(c.cmd(&["SUNIONSTORE", "u", "a", "b"]).await, int(4));
    assert_eq!(c.cmd(&["SMOVE", "a", "b", "1"]).await, int(1));
    assert_eq!(c.cmd(&["SISMEMBER", "b", "1"]).await, int(1));
    assert_eq!(c.cmd(&["SREM", "a", "2", "3"]).await, int(2));
    assert_eq!(c.cmd(&["EXISTS", "a"]).await, int(0));
    let popped = c.cmd(&["SPOP", "u"]).await;
    assert!(matches!(popped, Frame::Bulk(_)));
    assert_eq!(c.cmd(&["SCARD", "u"]).await, int(3));
}

#[tokio::test]
async fn sorted_sets() {
    let srv = TestServer::start().await;
    let mut c = srv.client().await;
    assert_eq!(
        c.cmd(&["ZADD", "z", "1", "a", "2", "b", "3", "c"]).await,
        int(3)
    );
    assert_eq!(c.cmd(&["ZADD", "z", "CH", "5", "a"]).await, int(1));
    assert_eq!(c.cmd(&["ZADD", "z", "NX", "9", "a"]).await, int(0));
    assert_eq!(c.cmd(&["ZADD", "z", "GT", "1", "a"]).await, int(0));
    assert_eq!(c.cmd(&["ZSCORE", "z", "a"]).await, b("5"));
    assert_eq!(c.cmd(&["ZRANK", "z", "a"]).await, int(2));
    assert_eq!(c.cmd(&["ZREVRANK", "z", "a"]).await, int(0));
    assert_eq!(
        c.cmd(&["ZRANGE", "z", "0", "-1"]).await,
        arr(&["b", "c", "a"])
    );
    assert_eq!(
        c.cmd(&["ZRANGE", "z", "0", "1", "WITHSCORES"]).await,
        arr(&["b", "2", "c", "3"])
    );
    assert_eq!(c.cmd(&["ZREVRANGE", "z", "0", "0"]).await, arr(&["a"]));
    assert_eq!(
        c.cmd(&["ZRANGEBYSCORE", "z", "(2", "+inf"]).await,
        arr(&["c", "a"])
    );
    assert_eq!(
        c.cmd(&["ZRANGE", "z", "+inf", "(2", "BYSCORE", "REV"])
            .await,
        arr(&["a", "c"])
    );
    assert_eq!(c.cmd(&["ZCOUNT", "z", "-inf", "3"]).await, int(2));
    assert_eq!(c.cmd(&["ZINCRBY", "z", "10", "b"]).await, b("12"));
    assert_eq!(c.cmd(&["ZPOPMIN", "z"]).await, arr(&["c", "3"]));
    assert_eq!(c.cmd(&["ZREMRANGEBYSCORE", "z", "0", "6"]).await, int(1));
    assert_eq!(c.cmd(&["ZCARD", "z"]).await, int(1));
    assert_eq!(c.cmd(&["ZREM", "z", "b"]).await, int(1));
    assert_eq!(c.cmd(&["EXISTS", "z"]).await, int(0));
    assert!(matches!(
        c.cmd(&["ZADD", "z", "nan", "m"]).await,
        Frame::Error(_)
    ));
}

#[tokio::test]
async fn keyspace_commands() {
    let srv = TestServer::start().await;
    let mut c = srv.client().await;
    for i in 0..30 {
        c.cmd(&["SET", &format!("user:{i}"), "x"]).await;
    }
    c.cmd(&["SET", "other", "y"]).await;
    assert_eq!(c.cmd(&["DBSIZE"]).await, int(31));
    let Frame::Array(keys) = c.cmd(&["KEYS", "user:1*"]).await else {
        panic!()
    };
    assert_eq!(keys.len(), 11);
    // SCAN visits every key exactly once when the keyspace is stable.
    let mut seen = std::collections::HashSet::new();
    let mut cursor = "0".to_string();
    let mut rounds = 0;
    loop {
        let Frame::Array(reply) = c.cmd(&["SCAN", &cursor, "COUNT", "7"]).await else {
            panic!()
        };
        let Frame::Bulk(next) = &reply[0] else {
            panic!()
        };
        let Frame::Array(page) = &reply[1] else {
            panic!()
        };
        for k in page {
            assert!(
                seen.insert(format!("{k:?}")),
                "duplicate key in stable scan"
            );
        }
        cursor = String::from_utf8(next.to_vec()).unwrap();
        rounds += 1;
        if cursor == "0" {
            break;
        }
        assert!(rounds < 200);
    }
    assert_eq!(seen.len(), 31);
    assert_eq!(c.cmd(&["RENAME", "other", "renamed"]).await, ok());
    assert_eq!(c.cmd(&["GET", "renamed"]).await, b("y"));
    assert_eq!(c.cmd(&["RENAMENX", "renamed", "user:1"]).await, int(0));
    assert!(matches!(
        c.cmd(&["RENAME", "ghost", "x"]).await,
        Frame::Error(_)
    ));
    assert_eq!(c.cmd(&["DEL", "user:1", "user:2", "nope"]).await, int(2));
    assert_eq!(c.cmd(&["FLUSHALL"]).await, ok());
    assert_eq!(c.cmd(&["DBSIZE"]).await, int(0));
}

#[tokio::test]
async fn ttl_lazy_and_active_expiry() {
    let srv = TestServer::start().await;
    let mut c = srv.client().await;
    assert_eq!(c.cmd(&["SET", "lazy", "v", "PX", "60"]).await, ok());
    assert_eq!(c.cmd(&["SET", "active", "v", "PX", "60"]).await, ok());
    assert_eq!(c.cmd(&["SET", "forever", "v"]).await, ok());
    let Frame::Integer(pttl) = c.cmd(&["PTTL", "lazy"]).await else {
        panic!()
    };
    assert!((1..=60).contains(&pttl));
    assert_eq!(c.cmd(&["TTL", "forever"]).await, int(-1));
    assert_eq!(c.cmd(&["TTL", "nope"]).await, int(-2));
    assert_eq!(c.cmd(&["PERSIST", "forever"]).await, int(0));
    assert_eq!(c.cmd(&["EXPIRE", "forever", "100"]).await, int(1));
    assert_eq!(c.cmd(&["PERSIST", "forever"]).await, int(1));
    tokio::time::sleep(std::time::Duration::from_millis(120)).await;
    // `lazy` is observed by a read; `active` is never touched, only the background task can remove it.
    assert_eq!(c.cmd(&["GET", "lazy"]).await, Frame::Null);
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    assert_eq!(
        srv.engine.db().key_count(),
        1,
        "background expiry must reclaim untouched keys"
    );
    let info = c.cmd(&["INFO", "stats"]).await;
    let Frame::Bulk(text) = info else { panic!() };
    assert!(String::from_utf8_lossy(&text).contains("expired_keys:2"));
}

#[tokio::test]
async fn expire_options_and_past_deadlines() {
    let srv = TestServer::start().await;
    let mut c = srv.client().await;
    c.cmd(&["SET", "k", "v"]).await;
    assert_eq!(c.cmd(&["EXPIRE", "k", "100", "XX"]).await, int(0));
    assert_eq!(c.cmd(&["EXPIRE", "k", "100", "NX"]).await, int(1));
    assert_eq!(c.cmd(&["EXPIRE", "k", "50", "GT"]).await, int(0));
    assert_eq!(c.cmd(&["EXPIRE", "k", "50", "LT"]).await, int(1));
    assert_eq!(
        c.cmd(&["PEXPIREAT", "k", "1"]).await,
        int(1),
        "a deadline in the past deletes the key"
    );
    assert_eq!(c.cmd(&["EXISTS", "k"]).await, int(0));
}

#[tokio::test]
async fn pipelining_returns_replies_in_order() {
    let srv = TestServer::start().await;
    let mut c = srv.client().await;
    let mut wire = Vec::new();
    for i in 0..500 {
        wire.extend(Client::encode(&["INCR", "n"]));
        wire.extend(Client::encode(&["SET", &format!("k{i}"), "v"]));
    }
    c.send_raw(&wire).await;
    for i in 1..=500 {
        assert_eq!(c.read().await, int(i));
        assert_eq!(c.read().await, ok());
    }
}

#[tokio::test]
async fn fragmented_and_inline_input() {
    let srv = TestServer::start().await;
    let mut c = srv.client().await;
    let wire = Client::encode(&["SET", "key", "value"]);
    for byte in wire {
        c.send_raw(&[byte]).await;
        tokio::task::yield_now().await;
    }
    assert_eq!(c.read().await, ok());
    c.send_raw(b"GET key\r\n").await;
    assert_eq!(c.read().await, b("value"));
}

#[tokio::test]
async fn protocol_errors_close_the_connection() {
    let srv = TestServer::start().await;
    let mut c = srv.client().await;
    c.send_raw(b"*1\r\n$abc\r\n").await;
    let Frame::Error(e) = c.read().await else {
        panic!("error expected")
    };
    assert!(e.starts_with("ERR Protocol error"));
    assert!(c.is_closed().await);
    // A new connection is unaffected.
    assert_eq!(
        srv.client().await.cmd(&["PING"]).await,
        Frame::simple("PONG")
    );
}

#[tokio::test]
async fn oversized_bulk_is_rejected_before_buffering() {
    let srv = TestServer::start_with(|c| c.max_bulk_len = 1024).await;
    let mut c = srv.client().await;
    c.send_raw(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$2000\r\n").await;
    let Frame::Error(e) = c.read().await else {
        panic!("error expected")
    };
    assert!(e.contains("exceeds the configured limit"));
    assert!(c.is_closed().await);
}

#[tokio::test]
async fn resp3_negotiation_changes_reply_types() {
    let srv = TestServer::start().await;
    let mut c = srv.client().await;
    c.cmd(&["HSET", "h", "f", "v"]).await;
    c.cmd(&["ZADD", "z", "1.5", "m"]).await;
    assert_eq!(
        c.cmd(&["HGETALL", "h"]).await,
        arr(&["f", "v"]),
        "RESP2 flattens maps"
    );
    let Frame::Map(info) = c.cmd(&["HELLO", "3"]).await else {
        panic!("HELLO 3 returns a map")
    };
    assert!(info.contains(&(b("proto"), int(3))));
    assert_eq!(
        c.cmd(&["HGETALL", "h"]).await,
        Frame::Map(vec![(b("f"), b("v"))])
    );
    assert_eq!(c.cmd(&["GET", "nope"]).await, Frame::Null);
    assert_eq!(c.cmd(&["ZSCORE", "z", "m"]).await, Frame::Double(1.5));
    assert_eq!(
        c.cmd(&["ZRANGE", "z", "0", "-1", "WITHSCORES"]).await,
        Frame::Array(vec![Frame::Array(vec![b("m"), Frame::Double(1.5)])])
    );
    assert!(matches!(c.cmd(&["SMEMBERS", "none"]).await, Frame::Set(_)));
    assert!(matches!(c.cmd(&["HELLO", "9"]).await, Frame::Error(e) if e.starts_with("NOPROTO")));
}

#[tokio::test]
async fn auth_is_enforced_with_constant_time_compare() {
    let srv = TestServer::start_with(|c| c.requirepass = Some("s3cret".into())).await;
    let mut c = srv.client().await;
    assert!(matches!(c.cmd(&["GET", "k"]).await, Frame::Error(e) if e.starts_with("NOAUTH")));
    assert!(
        matches!(c.cmd(&["AUTH", "wrong"]).await, Frame::Error(e) if e.starts_with("WRONGPASS"))
    );
    assert!(
        matches!(c.cmd(&["AUTH", "other", "s3cret"]).await, Frame::Error(e) if e.starts_with("WRONGPASS"))
    );
    assert!(matches!(c.cmd(&["GET", "k"]).await, Frame::Error(e) if e.starts_with("NOAUTH")));
    assert_eq!(c.cmd(&["AUTH", "s3cret"]).await, ok());
    assert_eq!(c.cmd(&["SET", "k", "v"]).await, ok());
    // HELLO can authenticate and switch protocol in one step.
    let mut d = srv.client().await;
    assert!(matches!(
        d.cmd(&["HELLO", "3", "AUTH", "default", "s3cret"]).await,
        Frame::Map(_)
    ));
    assert_eq!(d.cmd(&["GET", "k"]).await, b("v"));
    // Open servers refuse AUTH instead of silently accepting any password.
    let open = TestServer::start().await;
    assert!(
        matches!(open.client().await.cmd(&["AUTH", "x"]).await, Frame::Error(e) if e.contains("without any password"))
    );
}

#[tokio::test]
async fn max_clients_is_enforced() {
    let srv = TestServer::start_with(|c| c.max_clients = 2).await;
    let mut a = srv.client().await;
    let mut b2 = srv.client().await;
    assert_eq!(a.cmd(&["PING"]).await, Frame::simple("PONG"));
    assert_eq!(b2.cmd(&["PING"]).await, Frame::simple("PONG"));
    let mut third = srv.client().await;
    let Frame::Error(e) = third.read().await else {
        panic!("rejection expected")
    };
    assert!(e.contains("max number of clients"));
    drop(a);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(
        srv.client().await.cmd(&["PING"]).await,
        Frame::simple("PONG")
    );
}
