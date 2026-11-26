//! Option parsing, error paths and cross-type behaviour for every command family.
#![allow(missing_docs)]

mod common;

use common::*;
use ferrum_db::resp::Frame;

fn err(f: Frame) -> String {
    match f {
        Frame::Error(e) => e,
        other => panic!("expected an error reply, got {other:?}"),
    }
}

fn strings(f: Frame) -> Vec<String> {
    let items = match f {
        Frame::Array(items) | Frame::Set(items) => items,
        other => panic!("expected an array, got {other:?}"),
    };
    items
        .into_iter()
        .map(|i| match i {
            Frame::Bulk(b) => String::from_utf8_lossy(&b).to_string(),
            other => panic!("expected bulk, got {other:?}"),
        })
        .collect()
}

fn sorted(f: Frame) -> Vec<String> {
    let mut v = strings(f);
    v.sort();
    v
}

#[tokio::test]
async fn string_expiry_options() {
    let srv = TestServer::start().await;
    let mut c = srv.client().await;
    assert_eq!(c.cmd(&["SETEX", "k", "100", "v"]).await, ok());
    let Frame::Integer(ttl) = c.cmd(&["TTL", "k"]).await else {
        panic!()
    };
    assert!((98..=100).contains(&ttl));
    assert!(err(c.cmd(&["SETEX", "k", "0", "v"]).await).contains("invalid expire time in 'setex'"));
    assert_eq!(c.cmd(&["PSETEX", "p", "5000", "v"]).await, ok());
    let Frame::Integer(pttl) = c.cmd(&["PTTL", "p"]).await else {
        panic!()
    };
    assert!((1..=5000).contains(&pttl));
    assert!(err(c.cmd(&["PSETEX", "p", "-1", "v"]).await).contains("invalid expire time"));

    // KEEPTTL keeps the deadline; a plain SET clears it.
    c.cmd(&["SET", "k", "v", "EX", "100"]).await;
    assert_eq!(c.cmd(&["SET", "k", "w", "KEEPTTL"]).await, ok());
    let Frame::Integer(ttl) = c.cmd(&["TTL", "k"]).await else {
        panic!()
    };
    assert!(ttl > 90);
    c.cmd(&["SET", "k", "plain"]).await;
    assert_eq!(c.cmd(&["TTL", "k"]).await, int(-1));

    // Absolute deadlines.
    assert_eq!(c.cmd(&["SET", "a", "v", "EXAT", "4102444800"]).await, ok());
    assert_eq!(
        c.cmd(&["SET", "b", "v", "PXAT", "4102444800000"]).await,
        ok()
    );
    assert!(matches!(c.cmd(&["TTL", "a"]).await, Frame::Integer(t) if t > 1_000_000));
    // A deadline already in the past leaves nothing behind.
    assert_eq!(c.cmd(&["SET", "gone", "v", "PXAT", "1"]).await, ok());
    assert_eq!(c.cmd(&["EXISTS", "gone"]).await, int(0));

    for bad in [
        &["SET", "k", "v", "EX", "10", "PX", "5"][..],
        &["SET", "k", "v", "EX"],
        &["SET", "k", "v", "KEEPTTL", "EX", "5"],
        &["SET", "k", "v", "WAT"],
    ] {
        assert!(err(c.cmd(bad).await).contains("syntax"), "{bad:?}");
    }
    assert!(err(c.cmd(&["SET", "k", "v", "EX", "abc"]).await).contains("not an integer"));
    assert!(
        err(c.cmd(&["SET", "k", "v", "EX", "9223372036854775807"]).await)
            .contains("invalid expire")
    );
}

#[tokio::test]
async fn getex_getset_setnx_and_friends() {
    let srv = TestServer::start().await;
    let mut c = srv.client().await;
    assert_eq!(c.cmd(&["SETNX", "k", "1"]).await, int(1));
    assert_eq!(c.cmd(&["SETNX", "k", "2"]).await, int(0));
    assert_eq!(c.cmd(&["GETSET", "k", "3"]).await, b("1"));
    assert_eq!(c.cmd(&["GETSET", "fresh", "x"]).await, Frame::Null);
    assert_eq!(c.cmd(&["GETEX", "k", "EX", "100"]).await, b("3"));
    assert!(matches!(c.cmd(&["TTL", "k"]).await, Frame::Integer(t) if t > 90));
    assert_eq!(c.cmd(&["GETEX", "k", "PERSIST"]).await, b("3"));
    assert_eq!(c.cmd(&["TTL", "k"]).await, int(-1));
    assert_eq!(c.cmd(&["GETEX", "k"]).await, b("3"));
    assert_eq!(c.cmd(&["GETEX", "k", "PX", "100000"]).await, b("3"));
    assert_eq!(c.cmd(&["GETEX", "missing", "EX", "10"]).await, Frame::Null);
    assert!(err(c.cmd(&["GETEX", "k", "EX"]).await).contains("syntax"));
    assert!(err(c.cmd(&["GETEX", "k", "BOGUS", "1"]).await).contains("syntax"));
    assert_eq!(c.cmd(&["GETDEL", "missing"]).await, Frame::Null);
    c.cmd(&["RPUSH", "list", "x"]).await;
    assert!(err(c.cmd(&["GETDEL", "list"]).await).starts_with("WRONGTYPE"));
}

#[tokio::test]
async fn numeric_string_commands() {
    let srv = TestServer::start().await;
    let mut c = srv.client().await;
    assert_eq!(c.cmd(&["DECR", "n"]).await, int(-1));
    assert_eq!(c.cmd(&["INCRBY", "n", "10"]).await, int(9));
    assert!(err(c.cmd(&["DECRBY", "n", "-9223372036854775808"]).await).contains("overflow"));
    assert!(err(c.cmd(&["INCRBY", "n", "x"]).await).contains("not an integer"));
    c.cmd(&["SET", "f", "10.5"]).await;
    assert_eq!(c.cmd(&["INCRBYFLOAT", "f", "0.25"]).await, b("10.75"));
    assert_eq!(c.cmd(&["INCRBYFLOAT", "f", "-0.75"]).await, b("10"));
    assert!(err(c.cmd(&["INCRBYFLOAT", "f", "nan"]).await).contains("not a valid float"));
    assert!(err(c.cmd(&["INCRBYFLOAT", "f", "inf"]).await).contains("not a valid float"));
    c.cmd(&["SET", "text", "abc"]).await;
    assert!(err(c.cmd(&["INCRBYFLOAT", "text", "1"]).await).contains("not a valid float"));
    c.cmd(&["SET", "huge", "1.7976931348623157e308"]).await;
    assert!(
        err(c
            .cmd(&["INCRBYFLOAT", "huge", "1.7976931348623157e308"])
            .await)
        .contains("NaN or Infinity")
    );
    assert_eq!(c.cmd(&["APPEND", "fresh", "abc"]).await, int(3));
    assert_eq!(c.cmd(&["STRLEN", "missing"]).await, int(0));
    assert_eq!(c.cmd(&["GETRANGE", "fresh", "-2", "-1"]).await, b("bc"));
    assert_eq!(c.cmd(&["GETRANGE", "fresh", "5", "9"]).await, b(""));
    assert_eq!(c.cmd(&["GETRANGE", "missing", "0", "-1"]).await, b(""));
    assert_eq!(c.cmd(&["SETRANGE", "fresh", "1", ""]).await, int(3));
    assert_eq!(c.cmd(&["SETRANGE", "fresh", "1", "XY"]).await, int(3));
    assert_eq!(c.cmd(&["GET", "fresh"]).await, b("aXY"));
    assert!(err(c.cmd(&["SETRANGE", "fresh", "-1", "x"]).await).contains("out of range"));
    assert!(
        err(c.cmd(&["SETRANGE", "fresh", "536870912", "x"]).await).contains("maximum allowed size")
    );
    assert!(err(c.cmd(&["MSET", "a", "1", "b"]).await).contains("wrong number"));
    assert!(err(c.cmd(&["MSETNX", "a", "1", "b"]).await).contains("wrong number"));
    assert_eq!(c.cmd(&["MSETNX", "m1", "1", "m2", "2"]).await, int(1));
    c.cmd(&["RPUSH", "list", "x"]).await;
    assert_eq!(
        c.cmd(&["MGET", "m1", "list", "none"]).await,
        Frame::Array(vec![b("1"), Frame::Null, Frame::Null])
    );
}

#[tokio::test]
async fn hash_extras() {
    let srv = TestServer::start().await;
    let mut c = srv.client().await;
    assert_eq!(c.cmd(&["HSETNX", "h", "f", "1"]).await, int(1));
    assert_eq!(c.cmd(&["HSETNX", "h", "f", "2"]).await, int(0));
    assert_eq!(c.cmd(&["HMSET", "h", "g", "2", "h", "3"]).await, ok());
    assert_eq!(sorted(c.cmd(&["HKEYS", "h"]).await), vec!["f", "g", "h"]);
    assert_eq!(sorted(c.cmd(&["HVALS", "h"]).await), vec!["1", "2", "3"]);
    assert_eq!(c.cmd(&["HEXISTS", "h", "g"]).await, int(1));
    assert_eq!(c.cmd(&["HEXISTS", "h", "zz"]).await, int(0));
    assert_eq!(c.cmd(&["HINCRBYFLOAT", "h", "fl", "1.5"]).await, b("1.5"));
    assert_eq!(c.cmd(&["HINCRBYFLOAT", "h", "fl", "2.25"]).await, b("3.75"));
    assert!(err(c.cmd(&["HINCRBYFLOAT", "h", "fl", "inf"]).await).contains("not a valid float"));
    c.cmd(&["HSET", "h", "text", "abc"]).await;
    assert!(err(c.cmd(&["HINCRBY", "h", "text", "1"]).await).contains("not an integer"));
    assert!(err(c.cmd(&["HINCRBYFLOAT", "h", "text", "1"]).await).contains("not a float"));
    assert!(err(c.cmd(&["HINCRBY", "h", "f", "x"]).await).contains("not an integer"));
    assert!(err(c.cmd(&["HSET", "h", "a", "1", "b"]).await).contains("wrong number"));
    assert!(err(c.cmd(&["HMSET", "h", "a"]).await).contains("wrong number"));
    c.cmd(&["HSET", "big", "n", "9223372036854775807"]).await;
    assert!(err(c.cmd(&["HINCRBY", "big", "n", "1"]).await).contains("overflow"));
    assert_eq!(c.cmd(&["HGETALL", "missing"]).await, Frame::Array(vec![]));
    assert_eq!(c.cmd(&["HLEN", "missing"]).await, int(0));
    assert_eq!(c.cmd(&["HDEL", "missing", "f"]).await, int(0));
    assert_eq!(c.cmd(&["HGET", "missing", "f"]).await, Frame::Null);
    assert_eq!(
        c.cmd(&["HMGET", "missing", "a", "b"]).await,
        Frame::Array(vec![Frame::Null, Frame::Null])
    );
    assert_eq!(c.cmd(&["HKEYS", "missing"]).await, Frame::Array(vec![]));
}

#[tokio::test]
async fn set_extras() {
    let srv = TestServer::start().await;
    let mut c = srv.client().await;
    c.cmd(&["SADD", "s", "a", "b", "c"]).await;
    assert_eq!(
        c.cmd(&["SMISMEMBER", "s", "a", "zz"]).await,
        Frame::Array(vec![int(1), int(0)])
    );
    assert!(matches!(c.cmd(&["SRANDMEMBER", "s"]).await, Frame::Bulk(_)));
    let two = strings(c.cmd(&["SRANDMEMBER", "s", "2"]).await);
    assert_eq!(two.len(), 2);
    assert_ne!(two[0], two[1], "positive counts never repeat");
    assert_eq!(
        strings(c.cmd(&["SRANDMEMBER", "s", "-9"]).await).len(),
        9,
        "negative counts allow repeats"
    );
    assert_eq!(strings(c.cmd(&["SRANDMEMBER", "s", "50"]).await).len(), 3);
    assert_eq!(c.cmd(&["SRANDMEMBER", "missing"]).await, Frame::Null);
    assert_eq!(
        c.cmd(&["SRANDMEMBER", "missing", "3"]).await,
        Frame::Array(vec![])
    );
    assert_eq!(strings(c.cmd(&["SPOP", "s", "2"]).await).len(), 2);
    assert_eq!(c.cmd(&["SCARD", "s"]).await, int(1));
    assert_eq!(c.cmd(&["SPOP", "missing"]).await, Frame::Null);
    assert!(err(c.cmd(&["SPOP", "s", "-1"]).await).contains("must be positive"));

    c.cmd(&["SADD", "x", "1", "2", "3"]).await;
    c.cmd(&["SADD", "y", "2", "3", "4"]).await;
    assert_eq!(c.cmd(&["SINTERSTORE", "i", "x", "y"]).await, int(2));
    assert_eq!(sorted(c.cmd(&["SMEMBERS", "i"]).await), vec!["2", "3"]);
    assert_eq!(c.cmd(&["SDIFFSTORE", "d", "x", "y"]).await, int(1));
    assert_eq!(c.cmd(&["SINTERSTORE", "i", "x", "nothing"]).await, int(0));
    assert_eq!(
        c.cmd(&["EXISTS", "i"]).await,
        int(0),
        "an empty result deletes the destination"
    );
    assert_eq!(c.cmd(&["SMOVE", "x", "y", "absent"]).await, int(0));
    c.cmd(&["SET", "str", "v"]).await;
    assert!(err(c.cmd(&["SMOVE", "x", "str", "1"]).await).starts_with("WRONGTYPE"));
    assert_eq!(
        c.cmd(&["SISMEMBER", "x", "1"]).await,
        int(1),
        "a failed SMOVE must not lose the member"
    );
    assert!(err(c.cmd(&["SUNION", "x", "str"]).await).starts_with("WRONGTYPE"));
    assert_eq!(c.cmd(&["SMEMBERS", "missing"]).await, Frame::Array(vec![]));
}

#[tokio::test]
async fn key_commands_extras() {
    let srv = TestServer::start().await;
    let mut c = srv.client().await;
    assert_eq!(c.cmd(&["RANDOMKEY"]).await, Frame::Null);
    c.cmd(&["SET", "a", "1"]).await;
    assert_eq!(c.cmd(&["RANDOMKEY"]).await, b("a"));
    assert_eq!(c.cmd(&["EXISTS", "a", "a", "nope"]).await, int(2));
    assert_eq!(c.cmd(&["EXPIREAT", "a", "4102444800"]).await, int(1));
    assert_eq!(c.cmd(&["PEXPIRE", "a", "100000"]).await, int(1));
    assert!(matches!(c.cmd(&["PTTL", "a"]).await, Frame::Integer(t) if t > 90_000));
    assert_eq!(c.cmd(&["EXPIRE", "nope", "10"]).await, int(0));
    assert!(err(c.cmd(&["EXPIRE", "a", "10", "BOGUS"]).await).contains("syntax"));
    assert!(err(c.cmd(&["EXPIRE", "a", "10", "NX", "XX"]).await).contains("not compatible"));
    assert!(err(c.cmd(&["EXPIRE", "a", "10", "GT", "LT"]).await).contains("not compatible"));
    assert!(err(c.cmd(&["EXPIRE", "a", "9223372036854775807"]).await).contains("invalid expire"));
    assert!(err(c.cmd(&["EXPIRE", "a", "x"]).await).contains("not an integer"));
    assert_eq!(c.cmd(&["UNLINK", "a"]).await, int(1));
    assert_eq!(
        c.cmd(&["RENAME", "nope", "x"]).await,
        Frame::Error("ERR no such key".into())
    );
    c.cmd(&["SET", "same", "1"]).await;
    assert_eq!(c.cmd(&["RENAME", "same", "same"]).await, ok());
    assert_eq!(c.cmd(&["RENAMENX", "same", "same"]).await, int(0));

    c.cmd(&["SET", "s1", "v"]).await;
    c.cmd(&["RPUSH", "l1", "v"]).await;
    c.cmd(&["HSET", "h1", "f", "v"]).await;
    c.cmd(&["SADD", "t1", "v"]).await;
    c.cmd(&["ZADD", "z1", "1", "v"]).await;
    for (key, ty) in [
        ("s1", "string"),
        ("l1", "list"),
        ("h1", "hash"),
        ("t1", "set"),
        ("z1", "zset"),
    ] {
        assert_eq!(c.cmd(&["TYPE", key]).await, Frame::simple(ty));
    }
    // SCAN filters (each call visits one shard, so follow the cursor to the end).
    async fn scan_all(c: &mut Client, filter: &[&str]) -> Vec<String> {
        let (mut cursor, mut found) = ("0".to_string(), Vec::new());
        loop {
            let mut args = vec!["SCAN", cursor.as_str(), "COUNT", "100"];
            args.extend_from_slice(filter);
            let Frame::Array(reply) = c.cmd(&args).await else {
                panic!()
            };
            found.extend(strings(reply[1].clone()));
            cursor = strings(Frame::Array(vec![reply[0].clone()])).remove(0);
            if cursor == "0" {
                found.sort();
                return found;
            }
        }
    }
    assert_eq!(scan_all(&mut c, &["TYPE", "hash"]).await, vec!["h1"]);
    assert_eq!(scan_all(&mut c, &["MATCH", "?1"]).await.len(), 5);
    assert_eq!(
        scan_all(&mut c, &[]).await.len(),
        6,
        "5 typed keys plus `same`"
    );
    assert!(err(c.cmd(&["SCAN", "abc"]).await).contains("invalid cursor"));
    assert!(err(c.cmd(&["SCAN", "99999999999"]).await).contains("invalid cursor"));
    assert!(err(c.cmd(&["SCAN", "0", "COUNT", "0"]).await).contains("syntax"));
    assert!(err(c.cmd(&["SCAN", "0", "BOGUS", "1"]).await).contains("syntax"));
    assert!(err(c.cmd(&["SCAN", "0", "MATCH"]).await).contains("syntax"));
    assert_eq!(c.cmd(&["FLUSHALL", "ASYNC"]).await, ok());
    assert!(err(c.cmd(&["FLUSHALL", "now"]).await).contains("syntax"));
    assert_eq!(c.cmd(&["KEYS", "*"]).await, Frame::Array(vec![]));
}

#[tokio::test]
async fn list_extras() {
    let srv = TestServer::start().await;
    let mut c = srv.client().await;
    assert_eq!(c.cmd(&["LPUSHX", "l", "x"]).await, int(0));
    assert_eq!(c.cmd(&["RPUSHX", "l", "x"]).await, int(0));
    assert_eq!(c.cmd(&["EXISTS", "l"]).await, int(0));
    c.cmd(&["RPUSH", "l", "a", "b", "c"]).await;
    assert_eq!(c.cmd(&["LPUSHX", "l", "z"]).await, int(4));
    assert_eq!(c.cmd(&["RPUSHX", "l", "y"]).await, int(5));
    assert_eq!(c.cmd(&["LPOP", "l", "0"]).await, Frame::Array(vec![]));
    assert!(err(c.cmd(&["LPOP", "l", "-1"]).await).contains("must be positive"));
    assert!(err(c.cmd(&["LPOP", "l", "1", "2"]).await).contains("wrong number"));
    assert_eq!(c.cmd(&["LINDEX", "l", "99"]).await, Frame::Null);
    assert_eq!(c.cmd(&["LINDEX", "missing", "0"]).await, Frame::Null);
    assert!(err(c.cmd(&["LSET", "l", "99", "x"]).await).contains("index out of range"));
    assert!(err(c.cmd(&["LSET", "missing", "0", "x"]).await).contains("no such key"));
    assert_eq!(c.cmd(&["LSET", "l", "-1", "Y"]).await, ok());
    assert_eq!(c.cmd(&["LRANGE", "l", "-2", "-1"]).await, arr(&["c", "Y"]));
    assert_eq!(
        c.cmd(&["LRANGE", "l", "10", "20"]).await,
        Frame::Array(vec![])
    );
    assert_eq!(
        c.cmd(&["LRANGE", "missing", "0", "-1"]).await,
        Frame::Array(vec![])
    );
    assert_eq!(c.cmd(&["LREM", "l", "0", "nothing"]).await, int(0));
    assert_eq!(c.cmd(&["LREM", "missing", "0", "x"]).await, int(0));
    assert_eq!(c.cmd(&["LTRIM", "l", "10", "20"]).await, ok());
    assert_eq!(
        c.cmd(&["EXISTS", "l"]).await,
        int(0),
        "trimming to nothing deletes the key"
    );
    assert_eq!(c.cmd(&["LTRIM", "missing", "0", "1"]).await, ok());
    assert_eq!(c.cmd(&["LLEN", "missing"]).await, int(0));

    c.cmd(&["RPUSH", "src", "1"]).await;
    c.cmd(&["SET", "str", "v"]).await;
    assert!(err(c.cmd(&["LMOVE", "src", "str", "LEFT", "LEFT"]).await).starts_with("WRONGTYPE"));
    assert_eq!(
        c.cmd(&["LLEN", "src"]).await,
        int(1),
        "a rejected LMOVE must not pop"
    );
    assert!(err(c.cmd(&["LMOVE", "src", "dst", "UP", "LEFT"]).await).contains("syntax"));
    assert_eq!(
        c.cmd(&["LMOVE", "missing", "dst", "LEFT", "LEFT"]).await,
        Frame::Null
    );
    assert_eq!(
        c.cmd(&["LMOVE", "src", "src", "LEFT", "RIGHT"]).await,
        b("1")
    );
    assert_eq!(c.cmd(&["LRANGE", "src", "0", "-1"]).await, arr(&["1"]));
}

#[tokio::test]
async fn zset_extras() {
    let srv = TestServer::start().await;
    let mut c = srv.client().await;
    assert_eq!(c.cmd(&["ZADD", "z", "XX", "1", "a"]).await, int(0));
    assert_eq!(
        c.cmd(&["EXISTS", "z"]).await,
        int(0),
        "an all-skipped ZADD must not leave an empty set"
    );
    assert_eq!(
        c.cmd(&["ZADD", "z", "1", "a", "2", "b", "3", "c"]).await,
        int(3)
    );
    assert_eq!(c.cmd(&["ZADD", "z", "INCR", "5", "a"]).await, b("6"));
    assert_eq!(
        c.cmd(&["ZADD", "z", "NX", "INCR", "5", "a"]).await,
        Frame::Null
    );
    assert_eq!(
        c.cmd(&["ZADD", "z", "LT", "CH", "1", "a", "9", "b"]).await,
        int(1)
    );
    assert_eq!(c.cmd(&["ZADD", "z", "GT", "CH", "100", "b"]).await, int(1));
    assert!(err(c.cmd(&["ZADD", "z", "GT", "LT", "1", "a"]).await).contains("not compatible"));
    assert!(err(c.cmd(&["ZADD", "z", "NX", "XX", "1", "a"]).await).contains("not compatible"));
    assert!(
        err(c.cmd(&["ZADD", "z", "INCR", "1", "a", "2", "b"]).await).contains("single increment")
    );
    assert!(err(c.cmd(&["ZADD", "z", "1", "a", "2"]).await).contains("syntax"));
    assert!(err(c.cmd(&["ZADD", "z", "abc", "a"]).await).contains("not a valid float"));
    assert_eq!(c.cmd(&["ZINCRBY", "new", "2.5", "m"]).await, b("2.5"));
    assert_eq!(c.cmd(&["ZSCORE", "z", "ghost"]).await, Frame::Null);
    assert_eq!(c.cmd(&["ZSCORE", "missing", "m"]).await, Frame::Null);
    assert_eq!(c.cmd(&["ZRANK", "z", "ghost"]).await, Frame::Null);
    assert_eq!(c.cmd(&["ZCARD", "missing"]).await, int(0));

    c.cmd(&["DEL", "z"]).await;
    c.cmd(&[
        "ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e",
    ])
    .await;
    assert_eq!(
        c.cmd(&["ZRANGE", "z", "0", "1", "REV"]).await,
        arr(&["e", "d"])
    );
    assert_eq!(
        c.cmd(&["ZRANGE", "z", "2", "3", "BYSCORE", "WITHSCORES"])
            .await,
        arr(&["b", "2", "c", "3"])
    );
    assert_eq!(
        c.cmd(&["ZRANGE", "z", "-inf", "+inf", "BYSCORE", "LIMIT", "1", "2"])
            .await,
        arr(&["b", "c"])
    );
    assert_eq!(
        c.cmd(&["ZRANGEBYSCORE", "z", "-inf", "+inf", "LIMIT", "3", "-1"])
            .await,
        arr(&["d", "e"])
    );
    assert_eq!(
        c.cmd(&[
            "ZREVRANGEBYSCORE",
            "z",
            "5",
            "(2",
            "LIMIT",
            "0",
            "2",
            "WITHSCORES"
        ])
        .await,
        arr(&["e", "5", "d", "4"])
    );
    assert_eq!(c.cmd(&["ZCOUNT", "z", "(1", "(5"]).await, int(3));
    assert_eq!(c.cmd(&["ZREMRANGEBYRANK", "z", "-2", "-1"]).await, int(2));
    assert_eq!(
        c.cmd(&["ZPOPMAX", "z", "2"]).await,
        arr(&["c", "3", "b", "2"])
    );
    assert_eq!(c.cmd(&["ZPOPMIN", "missing"]).await, Frame::Array(vec![]));
    assert_eq!(
        c.cmd(&["ZREMRANGEBYSCORE", "missing", "0", "1"]).await,
        int(0)
    );
    assert_eq!(
        c.cmd(&["ZREMRANGEBYRANK", "missing", "0", "1"]).await,
        int(0)
    );
    assert_eq!(c.cmd(&["ZREM", "missing", "a"]).await, int(0));
    assert_eq!(
        c.cmd(&["ZRANGE", "missing", "0", "-1"]).await,
        Frame::Array(vec![])
    );
    assert!(err(c.cmd(&["ZCOUNT", "z", "abc", "1"]).await).contains("min or max is not a float"));
    assert!(err(c.cmd(&["ZRANGE", "z", "0", "1", "BOGUS"]).await).contains("syntax"));
    assert!(
        err(c
            .cmd(&["ZRANGEBYSCORE", "z", "0", "1", "LIMIT", "x", "1"])
            .await)
        .contains("not an integer")
    );
    assert!(err(c.cmd(&["ZPOPMIN", "z", "-1"]).await).contains("must be positive"));
    assert!(err(c.cmd(&["ZPOPMIN", "z", "1", "2"]).await).contains("syntax"));
}

#[tokio::test]
async fn nan_scores_are_rejected() {
    let srv = TestServer::start().await;
    let mut c = srv.client().await;
    c.cmd(&["ZADD", "z", "inf", "m"]).await;
    assert!(err(c.cmd(&["ZADD", "z", "INCR", "-inf", "m"]).await).contains("NaN"));
    assert!(err(c.cmd(&["ZINCRBY", "z", "-inf", "m"]).await).contains("NaN"));
    assert_eq!(c.cmd(&["ZSCORE", "z", "m"]).await, b("inf"));
}

/// Every command must refuse a key of the wrong type without touching it.
#[tokio::test]
async fn wrong_type_matrix() {
    let srv = TestServer::start().await;
    let mut c = srv.client().await;
    c.cmd(&["SET", "str", "v"]).await;
    c.cmd(&["RPUSH", "list", "v"]).await;
    c.cmd(&["HSET", "hash", "f", "v"]).await;
    c.cmd(&["SADD", "set", "v"]).await;
    c.cmd(&["ZADD", "zset", "1", "v"]).await;

    let by_type: [(&str, Vec<&[&str]>); 5] = [
        (
            "string",
            vec![
                &["GET", "K"],
                &["APPEND", "K", "x"],
                &["STRLEN", "K"],
                &["GETRANGE", "K", "0", "1"],
                &["SETRANGE", "K", "0", "x"],
                &["INCR", "K"],
                &["INCRBYFLOAT", "K", "1"],
                &["GETDEL", "K"],
                &["GETEX", "K", "EX", "5"],
                &["GETSET", "K", "x"],
            ],
        ),
        (
            "list",
            vec![
                &["LPUSH", "K", "x"],
                &["RPUSH", "K", "x"],
                &["LPOP", "K"],
                &["RPOP", "K"],
                &["LLEN", "K"],
                &["LRANGE", "K", "0", "-1"],
                &["LINDEX", "K", "0"],
                &["LSET", "K", "0", "x"],
                &["LTRIM", "K", "0", "1"],
                &["LREM", "K", "0", "x"],
                &["LPUSHX", "K", "x"],
            ],
        ),
        (
            "hash",
            vec![
                &["HSET", "K", "f", "v"],
                &["HGET", "K", "f"],
                &["HDEL", "K", "f"],
                &["HLEN", "K"],
                &["HGETALL", "K"],
                &["HKEYS", "K"],
                &["HVALS", "K"],
                &["HEXISTS", "K", "f"],
                &["HINCRBY", "K", "f", "1"],
                &["HMGET", "K", "f"],
                &["HSETNX", "K", "f", "v"],
            ],
        ),
        (
            "set",
            vec![
                &["SADD", "K", "x"],
                &["SREM", "K", "x"],
                &["SISMEMBER", "K", "x"],
                &["SMEMBERS", "K"],
                &["SCARD", "K"],
                &["SPOP", "K"],
                &["SRANDMEMBER", "K"],
                &["SMISMEMBER", "K", "x"],
            ],
        ),
        (
            "zset",
            vec![
                &["ZADD", "K", "1", "x"],
                &["ZREM", "K", "x"],
                &["ZSCORE", "K", "x"],
                &["ZCARD", "K"],
                &["ZRANK", "K", "x"],
                &["ZRANGE", "K", "0", "-1"],
                &["ZINCRBY", "K", "1", "x"],
                &["ZCOUNT", "K", "0", "1"],
                &["ZPOPMIN", "K"],
                &["ZREMRANGEBYRANK", "K", "0", "1"],
                &["ZRANGEBYSCORE", "K", "0", "1"],
            ],
        ),
    ];
    for (family, commands) in by_type {
        for (key, key_type) in [
            ("str", "string"),
            ("list", "list"),
            ("hash", "hash"),
            ("set", "set"),
            ("zset", "zset"),
        ] {
            if key_type == family {
                continue;
            }
            for template in &commands {
                let args: Vec<&str> = template
                    .iter()
                    .map(|a| if *a == "K" { key } else { *a })
                    .collect();
                let e = err(c.cmd(&args).await);
                assert!(e.starts_with("WRONGTYPE"), "{args:?} on a {key_type}: {e}");
            }
            assert_eq!(
                c.cmd(&["TYPE", key]).await,
                Frame::simple(key_type),
                "{family} commands altered a {key_type}"
            );
        }
    }
}

#[tokio::test]
async fn server_and_session_commands() {
    let srv = TestServer::start().await;
    let mut c = srv.client().await;
    assert_eq!(c.cmd(&["ECHO", "hello"]).await, b("hello"));
    assert!(matches!(c.cmd(&["TIME"]).await, Frame::Array(items) if items.len() == 2));
    assert!(matches!(c.cmd(&["COMMAND", "COUNT"]).await, Frame::Integer(n) if n > 100));
    assert_eq!(c.cmd(&["COMMAND"]).await, Frame::Array(vec![]));
    assert_eq!(c.cmd(&["COMMAND", "DOCS"]).await, Frame::Array(vec![]));
    assert_eq!(c.cmd(&["SELECT", "0"]).await, ok());
    assert_eq!(c.cmd(&["PING", "msg"]).await, b("msg"));
    assert!(err(c.cmd(&["PING", "a", "b"]).await).contains("wrong number"));

    let Frame::Array(cfg) = c.cmd(&["CONFIG", "GET", "max*"]).await else {
        panic!()
    };
    assert!(strings(Frame::Array(cfg)).contains(&"maxmemory".to_string()));
    assert!(err(c.cmd(&["CONFIG", "REWRITE"]).await).contains("unknown or malformed"));
    assert!(
        err(c.cmd(&["CONFIG", "SET", "maxmemory", "bogus"]).await).contains("invalid maxmemory")
    );

    assert!(matches!(c.cmd(&["CLIENT", "ID"]).await, Frame::Integer(n) if n > 0));
    assert_eq!(c.cmd(&["CLIENT", "GETNAME"]).await, Frame::Null);
    assert_eq!(c.cmd(&["CLIENT", "SETNAME", "worker-1"]).await, ok());
    assert_eq!(c.cmd(&["CLIENT", "GETNAME"]).await, b("worker-1"));
    assert_eq!(
        c.cmd(&["CLIENT", "SETINFO", "LIB-NAME", "test"]).await,
        ok()
    );
    assert!(err(c.cmd(&["CLIENT", "KILL"]).await).contains("unsupported CLIENT"));

    for section in [
        "server",
        "clients",
        "memory",
        "persistence",
        "stats",
        "keyspace",
    ] {
        let Frame::Bulk(text) = c.cmd(&["INFO", section]).await else {
            panic!()
        };
        assert!(
            String::from_utf8_lossy(&text).starts_with(&format!(
                "# {}",
                section[..1].to_uppercase() + &section[1..]
            )),
            "{section}"
        );
    }
    let Frame::Bulk(all) = c.cmd(&["INFO"]).await else {
        panic!()
    };
    let all = String::from_utf8_lossy(&all);
    for field in [
        "ferrum_version:",
        "connected_clients:",
        "used_memory:",
        "aof_enabled:0",
        "total_commands_processed:",
        "evicted_keys:",
    ] {
        assert!(all.contains(field), "{field}");
    }
    c.cmd(&["SET", "k", "v"]).await;
    let Frame::Bulk(ks) = c.cmd(&["INFO", "keyspace"]).await else {
        panic!()
    };
    assert!(String::from_utf8_lossy(&ks).contains("db0:keys=1,expires=0"));

    assert!(err(c.cmd(&["PUBSUB", "BOGUS"]).await).contains("unknown PUBSUB"));
    assert!(err(c.cmd(&["SAVE", "x"]).await).contains("wrong number"));
    assert!(matches!(c.cmd(&["SUBSCRIBE"]).await, Frame::Error(e) if e.contains("wrong number")));
    assert_eq!(
        c.cmd(&["UNSUBSCRIBE"]).await,
        Frame::Array(vec![b("unsubscribe"), Frame::Null, int(0)])
    );
    assert_eq!(
        c.cmd(&["PUNSUBSCRIBE"]).await,
        Frame::Array(vec![b("punsubscribe"), Frame::Null, int(0)])
    );
}

#[tokio::test]
async fn hello_options_reset_and_quit() {
    let srv = TestServer::start().await;
    let mut c = srv.client().await;
    assert!(matches!(
        c.cmd(&["HELLO", "3", "SETNAME", "named"]).await,
        Frame::Map(_)
    ));
    assert_eq!(c.cmd(&["CLIENT", "GETNAME"]).await, b("named"));
    assert!(err(c.cmd(&["HELLO", "3", "WAT"]).await).contains("syntax"));
    assert!(
        matches!(c.cmd(&["HELLO"]).await, Frame::Map(_)),
        "HELLO without arguments keeps the protocol"
    );
    // RESET drops transaction state, protocol, name and subscriptions.
    c.cmd(&["MULTI"]).await;
    c.cmd(&["SET", "k", "v"]).await;
    assert_eq!(c.cmd(&["RESET"]).await, Frame::simple("RESET"));
    assert_eq!(
        c.cmd(&["GET", "k"]).await,
        Frame::Null,
        "the queued SET was discarded"
    );
    assert_eq!(c.cmd(&["CLIENT", "GETNAME"]).await, Frame::Null);
    assert_eq!(
        c.cmd(&["GET", "k"]).await,
        Frame::Null,
        "back to RESP2 nulls"
    );
    assert_eq!(c.cmd(&["QUIT"]).await, ok());
    assert!(c.is_closed().await);
}

#[tokio::test]
async fn auth_variants_and_client_gating() {
    let srv = TestServer::start_with(|cfg| cfg.requirepass = Some("pw".into())).await;
    let mut c = srv.client().await;
    assert!(err(c.cmd(&["AUTH"]).await).contains("wrong number"));
    assert!(err(c.cmd(&["HELLO", "3"]).await).starts_with("NOAUTH"));
    assert!(err(c.cmd(&["HELLO", "3", "AUTH", "default", "bad"]).await).starts_with("WRONGPASS"));
    assert_eq!(c.cmd(&["AUTH", "default", "pw"]).await, ok());
    assert_eq!(c.cmd(&["PING"]).await, Frame::simple("PONG"));
    // RESET logs the connection out again.
    c.cmd(&["RESET"]).await;
    assert!(err(c.cmd(&["PING"]).await).starts_with("NOAUTH"));
}

#[tokio::test]
async fn multi_edge_cases() {
    let srv = TestServer::start().await;
    let mut c = srv.client().await;
    c.cmd(&["MULTI"]).await;
    assert!(err(c.cmd(&["MULTI", "x"]).await).contains("wrong number"));
    c.cmd(&["DISCARD"]).await;
    assert!(err(c.cmd(&["EXEC", "x"]).await).contains("wrong number"));
    assert!(err(c.cmd(&["DISCARD", "x"]).await).contains("wrong number"));
    assert!(err(c.cmd(&["WATCH"]).await).contains("wrong number"));
    // Empty transaction.
    c.cmd(&["MULTI"]).await;
    assert_eq!(c.cmd(&["EXEC"]).await, Frame::Array(vec![]));
    // Commands that fail at runtime inside EXEC and a transaction touching expiry.
    c.cmd(&["SET", "short", "v", "PX", "30"]).await;
    c.cmd(&["MULTI"]).await;
    c.cmd(&["GET", "short"]).await;
    c.cmd(&["EXPIRE", "short", "100"]).await;
    let Frame::Array(replies) = c.cmd(&["EXEC"]).await else {
        panic!()
    };
    assert_eq!(replies, vec![b("v"), int(1)]);
}
