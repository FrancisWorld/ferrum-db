//! Pub/sub over real sockets, including pattern matching and slow-subscriber backpressure.
#![allow(missing_docs)]

mod common;

use std::time::{Duration, Instant};

use common::*;
use ferrum_db::resp::Frame;

fn sub_ack(kind: &str, target: &str, count: i64) -> Frame {
    Frame::Array(vec![b(kind), b(target), int(count)])
}

#[tokio::test]
async fn channel_and_pattern_delivery_resp2() {
    let srv = TestServer::start().await;
    let mut sub = srv.client().await;
    let mut publisher = srv.client().await;

    assert_eq!(
        sub.cmd(&["SUBSCRIBE", "news"]).await,
        sub_ack("subscribe", "news", 1)
    );
    assert_eq!(
        sub.cmd(&["PSUBSCRIBE", "log.*"]).await,
        sub_ack("psubscribe", "log.*", 2)
    );

    assert_eq!(publisher.cmd(&["PUBLISH", "news", "hello"]).await, int(1));
    assert_eq!(sub.read().await, arr(&["message", "news", "hello"]));
    assert_eq!(
        publisher.cmd(&["PUBLISH", "log.error", "boom"]).await,
        int(1)
    );
    assert_eq!(
        sub.read().await,
        arr(&["pmessage", "log.*", "log.error", "boom"])
    );
    assert_eq!(publisher.cmd(&["PUBLISH", "unrelated", "x"]).await, int(0));

    assert_eq!(
        publisher.cmd(&["PUBSUB", "NUMSUB", "news", "nobody"]).await,
        Frame::Array(vec![b("news"), int(1), b("nobody"), int(0)])
    );
    assert_eq!(publisher.cmd(&["PUBSUB", "NUMPAT"]).await, int(1));
    assert_eq!(publisher.cmd(&["PUBSUB", "CHANNELS"]).await, arr(&["news"]));

    assert_eq!(
        sub.cmd(&["UNSUBSCRIBE", "news"]).await,
        sub_ack("unsubscribe", "news", 1)
    );
    assert_eq!(publisher.cmd(&["PUBLISH", "news", "gone"]).await, int(0));
    assert_eq!(
        sub.cmd(&["PUNSUBSCRIBE"]).await,
        sub_ack("punsubscribe", "log.*", 0)
    );
    assert_eq!(
        sub.cmd(&["PING"]).await,
        Frame::simple("PONG"),
        "leaving the last subscription restores normal mode"
    );
}

#[tokio::test]
async fn a_client_matching_both_channel_and_pattern_gets_both_messages() {
    let srv = TestServer::start().await;
    let mut sub = srv.client().await;
    sub.cmd(&["SUBSCRIBE", "a"]).await;
    sub.cmd(&["PSUBSCRIBE", "a*"]).await;
    let mut p = srv.client().await;
    assert_eq!(p.cmd(&["PUBLISH", "a", "m"]).await, int(2));
    assert_eq!(sub.read().await, arr(&["message", "a", "m"]));
    assert_eq!(sub.read().await, arr(&["pmessage", "a*", "a", "m"]));
}

#[tokio::test]
async fn resp2_subscribed_connection_only_accepts_pubsub_commands() {
    let srv = TestServer::start().await;
    let mut sub = srv.client().await;
    sub.cmd(&["SUBSCRIBE", "c"]).await;
    assert!(
        matches!(sub.cmd(&["GET", "k"]).await, Frame::Error(e) if e.contains("only (P)SUBSCRIBE"))
    );
    assert_eq!(
        sub.cmd(&["PING"]).await,
        Frame::Array(vec![b("pong"), b("")])
    );
    assert_eq!(
        sub.cmd(&["PING", "hi"]).await,
        Frame::Array(vec![b("pong"), b("hi")])
    );
    assert_eq!(sub.cmd(&["RESET"]).await, Frame::simple("RESET"));
    assert_eq!(
        srv.client().await.cmd(&["PUBLISH", "c", "x"]).await,
        int(0),
        "RESET dropped the subscription"
    );
}

#[tokio::test]
async fn resp3_subscribers_get_push_frames_and_can_keep_issuing_commands() {
    let srv = TestServer::start().await;
    let mut sub = srv.client().await;
    sub.cmd(&["HELLO", "3"]).await;
    assert_eq!(
        sub.cmd(&["SUBSCRIBE", "c"]).await,
        Frame::Push(vec![b("subscribe"), b("c"), int(1)])
    );
    assert_eq!(
        sub.cmd(&["SET", "k", "v"]).await,
        ok(),
        "RESP3 allows normal commands while subscribed"
    );
    assert_eq!(
        srv.client().await.cmd(&["PUBLISH", "c", "payload"]).await,
        int(1)
    );
    assert_eq!(
        sub.read().await,
        Frame::Push(vec![b("message"), b("c"), b("payload")])
    );
}

#[tokio::test]
async fn disconnecting_removes_subscriptions() {
    let srv = TestServer::start().await;
    let mut sub = srv.client().await;
    sub.cmd(&["SUBSCRIBE", "c"]).await;
    sub.cmd(&["PSUBSCRIBE", "p*"]).await;
    drop(sub);
    let mut p = srv.client().await;
    for _ in 0..50 {
        if p.cmd(&["PUBLISH", "c", "x"]).await == int(0)
            && p.cmd(&["PUBSUB", "NUMPAT"]).await == int(0)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("subscriptions leaked after disconnect");
}

#[tokio::test]
async fn messages_keep_their_order_per_subscriber() {
    let srv = TestServer::start().await;
    let mut sub = srv.client().await;
    sub.cmd(&["SUBSCRIBE", "seq"]).await;
    let mut p = srv.client().await;
    for i in 0..200 {
        p.send(&["PUBLISH", "seq", &i.to_string()]).await;
    }
    for i in 0..200 {
        assert_eq!(sub.read().await, arr(&["message", "seq", &i.to_string()]));
    }
}

/// A subscriber that never reads must be cut loose without blocking publishers or
/// starving other subscribers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_subscriber_is_disconnected_and_does_not_block_the_publisher() {
    let srv = TestServer::start_with(|c| c.pubsub_buffer_limit = 256 * 1024).await;

    let mut slow = srv.client().await;
    slow.cmd(&["SUBSCRIBE", "firehose"]).await; // ...and then never reads again.

    let mut fast = srv.client().await;
    fast.cmd(&["SUBSCRIBE", "firehose"]).await;
    const MESSAGES: usize = 4000;
    let reader = tokio::spawn(async move {
        let mut seen = 0;
        while seen < MESSAGES {
            let Some(Frame::Array(_)) = fast.try_read(Duration::from_secs(20)).await else {
                break;
            };
            seen += 1;
        }
        // Hand the connection back so it stays subscribed while the test inspects the hub.
        (seen, fast)
    });

    let payload = "x".repeat(8 * 1024); // 4000 x 8 KiB = 32 MiB, far beyond socket buffers + the 256 KiB limit
    let mut publisher = srv.client().await;
    let started = Instant::now();
    for _ in 0..MESSAGES {
        publisher.send(&["PUBLISH", "firehose", &payload]).await;
        // Keep the publisher's own reply stream drained.
        publisher.read().await;
    }
    let publish_time = started.elapsed();

    let (seen, _fast_still_connected) = reader.await.unwrap();
    assert_eq!(
        seen, MESSAGES,
        "the healthy subscriber must receive everything"
    );
    assert!(
        publish_time < Duration::from_secs(20),
        "publisher was blocked for {publish_time:?}"
    );

    let info = publisher.cmd(&["INFO", "stats"]).await;
    let Frame::Bulk(text) = info else { panic!() };
    let text = String::from_utf8_lossy(&text).to_string();
    assert!(
        text.contains("pubsub_slow_subscribers_disconnected:1"),
        "{text}"
    );
    assert_eq!(
        publisher.cmd(&["PUBSUB", "NUMSUB", "firehose"]).await,
        Frame::Array(vec![b("firehose"), int(1)])
    );

    // Draining the dead connection ends in EOF rather than hanging forever.
    assert!(
        slow.drain_to_eof(Duration::from_secs(20)).await,
        "slow subscriber connection should have been closed"
    );
}
