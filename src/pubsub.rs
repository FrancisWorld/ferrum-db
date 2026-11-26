//! Publish/subscribe hub with per-subscriber backpressure.
//!
//! `PUBLISH` must never block on a slow consumer, so each subscriber owns an
//! unbounded queue whose *byte* size is tracked. When a subscriber falls more than
//! `limit` bytes behind, it is flagged and its connection is closed (the same policy
//! as Redis' `client-output-buffer-limit pubsub`), instead of letting one stuck
//! client grow server memory without bound or stall publishers.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use bytes::Bytes;
use parking_lot::RwLock;
use tokio::sync::{Notify, mpsc};

use crate::glob;

/// A message queued for delivery to one subscriber.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Push {
    /// Delivery through a direct channel subscription.
    Message {
        /// Channel the message was published on.
        channel: Bytes,
        /// Message body.
        payload: Bytes,
    },
    /// Delivery through a pattern subscription.
    PMessage {
        /// The matching pattern.
        pattern: Bytes,
        /// Channel the message was published on.
        channel: Bytes,
        /// Message body.
        payload: Bytes,
    },
}

impl Push {
    fn size(&self) -> usize {
        match self {
            Push::Message { channel, payload } => channel.len() + payload.len() + 32,
            Push::PMessage {
                pattern,
                channel,
                payload,
            } => pattern.len() + channel.len() + payload.len() + 48,
        }
    }
}

/// The delivery endpoint of one connection.
#[derive(Debug)]
pub struct Subscriber {
    id: u64,
    tx: mpsc::UnboundedSender<Push>,
    queued: AtomicUsize,
    limit: usize,
    kicked: AtomicBool,
    kick: Notify,
}

impl Subscriber {
    /// Creates a subscriber and the receiver its connection drains.
    pub fn new(id: u64, limit: usize) -> (Arc<Subscriber>, mpsc::UnboundedReceiver<Push>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let sub = Subscriber {
            id,
            tx,
            queued: AtomicUsize::new(0),
            limit,
            kicked: AtomicBool::new(false),
            kick: Notify::new(),
        };
        (Arc::new(sub), rx)
    }

    /// Queues `push`; returns false when the subscriber is (now) over its limit.
    fn deliver(&self, push: Push) -> bool {
        if self.kicked.load(Ordering::Acquire) {
            return false;
        }
        let size = push.size();
        let queued = self.queued.fetch_add(size, Ordering::AcqRel) + size;
        if queued > self.limit {
            self.kicked.store(true, Ordering::Release);
            self.kick.notify_one();
            return false;
        }
        self.tx.send(push).is_ok()
    }

    /// Called by the connection after it dequeued `push`.
    pub fn consumed(&self, push: &Push) {
        self.queued.fetch_sub(push.size(), Ordering::AcqRel);
    }

    /// Resolves when the hub gave up on this subscriber for being too slow.
    pub async fn kicked(&self) {
        if !self.kicked.load(Ordering::Acquire) {
            self.kick.notified().await;
        }
    }

    /// True if the subscriber was dropped for exceeding its queue limit.
    pub fn was_kicked(&self) -> bool {
        self.kicked.load(Ordering::Acquire)
    }

    /// Bytes currently queued and not yet consumed.
    pub fn queued_bytes(&self) -> usize {
        self.queued.load(Ordering::Acquire)
    }
}

type Registry = HashMap<Bytes, HashMap<u64, Arc<Subscriber>>>;

/// Channel and pattern registry.
#[derive(Default)]
pub struct Hub {
    inner: RwLock<(Registry, Registry)>,
    published: AtomicU64,
    slow_disconnects: AtomicU64,
}

impl Hub {
    /// Creates an empty hub.
    pub fn new() -> Self {
        Hub::default()
    }

    /// Subscribes `sub` to an exact channel.
    pub fn subscribe(&self, sub: &Arc<Subscriber>, channel: &Bytes) {
        self.inner
            .write()
            .0
            .entry(channel.clone())
            .or_default()
            .insert(sub.id, sub.clone());
    }

    /// Removes an exact-channel subscription.
    pub fn unsubscribe(&self, sub: &Subscriber, channel: &[u8]) {
        Self::remove(&mut self.inner.write().0, sub.id, channel);
    }

    /// Subscribes `sub` to a glob pattern.
    pub fn psubscribe(&self, sub: &Arc<Subscriber>, pattern: &Bytes) {
        self.inner
            .write()
            .1
            .entry(pattern.clone())
            .or_default()
            .insert(sub.id, sub.clone());
    }

    /// Removes a pattern subscription.
    pub fn punsubscribe(&self, sub: &Subscriber, pattern: &[u8]) {
        Self::remove(&mut self.inner.write().1, sub.id, pattern);
    }

    fn remove(registry: &mut Registry, id: u64, key: &[u8]) {
        if let Some(subs) = registry.get_mut(key) {
            subs.remove(&id);
            if subs.is_empty() {
                registry.remove(key);
            }
        }
    }

    /// Drops every subscription of a closing connection.
    pub fn remove_subscriber<'a>(
        &self,
        sub: &Subscriber,
        channels: impl IntoIterator<Item = &'a Bytes>,
        patterns: impl IntoIterator<Item = &'a Bytes>,
    ) {
        let mut inner = self.inner.write();
        channels
            .into_iter()
            .for_each(|c| Self::remove(&mut inner.0, sub.id, c));
        patterns
            .into_iter()
            .for_each(|p| Self::remove(&mut inner.1, sub.id, p));
        if sub.was_kicked() {
            self.slow_disconnects.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Delivers `payload` to every matching subscriber; returns the receiver count.
    pub fn publish(&self, channel: &Bytes, payload: &Bytes) -> usize {
        self.published.fetch_add(1, Ordering::Relaxed);
        let inner = self.inner.read();
        let mut receivers = 0;
        if let Some(subs) = inner.0.get(channel) {
            for sub in subs.values() {
                let push = Push::Message {
                    channel: channel.clone(),
                    payload: payload.clone(),
                };
                receivers += usize::from(sub.deliver(push));
            }
        }
        for (pattern, subs) in &inner.1 {
            if !glob::matches(pattern, channel) {
                continue;
            }
            for sub in subs.values() {
                let push = Push::PMessage {
                    pattern: pattern.clone(),
                    channel: channel.clone(),
                    payload: payload.clone(),
                };
                receivers += usize::from(sub.deliver(push));
            }
        }
        receivers
    }

    /// Active channels, optionally filtered by a glob pattern.
    pub fn channels(&self, pattern: Option<&[u8]>) -> Vec<Bytes> {
        let inner = self.inner.read();
        inner
            .0
            .keys()
            .filter(|c| pattern.is_none_or(|p| glob::matches(p, c)))
            .cloned()
            .collect()
    }

    /// Number of direct subscribers of `channel`.
    pub fn numsub(&self, channel: &[u8]) -> usize {
        self.inner.read().0.get(channel).map_or(0, HashMap::len)
    }

    /// Number of distinct patterns with at least one subscriber.
    pub fn numpat(&self) -> usize {
        self.inner.read().1.len()
    }

    /// Total messages published.
    pub fn published(&self) -> u64 {
        self.published.load(Ordering::Relaxed)
    }

    /// Subscribers disconnected for being too slow.
    pub fn slow_disconnects(&self) -> u64 {
        self.slow_disconnects.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[test]
    fn channel_and_pattern_delivery() {
        let hub = Hub::new();
        let (a, mut rx_a) = Subscriber::new(1, 1 << 20);
        let (p, mut rx_p) = Subscriber::new(2, 1 << 20);
        hub.subscribe(&a, &b("news"));
        hub.psubscribe(&p, &b("n*"));
        hub.psubscribe(&a, &b("n?ws"));
        assert_eq!(hub.publish(&b("news"), &b("hi")), 3);
        assert_eq!(hub.publish(&b("other"), &b("x")), 0);
        assert!(matches!(rx_a.try_recv().unwrap(), Push::Message { .. }));
        assert!(matches!(rx_a.try_recv().unwrap(), Push::PMessage { .. }));
        assert!(matches!(rx_p.try_recv().unwrap(), Push::PMessage { .. }));
        assert_eq!(hub.numsub(b"news"), 1);
        assert_eq!(hub.numpat(), 2);
        assert_eq!(hub.channels(Some(b"ne*")), vec![b("news")]);
        hub.unsubscribe(&a, b"news");
        assert_eq!(hub.publish(&b("news"), &b("hi")), 2);
    }

    #[test]
    fn slow_subscriber_is_kicked_instead_of_growing_without_bound() {
        let hub = Hub::new();
        let (slow, mut rx) = Subscriber::new(1, 1024);
        hub.subscribe(&slow, &b("c"));
        let payload = Bytes::from(vec![b'x'; 300]);
        let mut delivered = 0;
        for _ in 0..50 {
            delivered += hub.publish(&b("c"), &payload);
        }
        assert!(slow.was_kicked());
        assert!(delivered < 50, "delivery must stop once the limit is hit");
        assert!(
            slow.queued_bytes() < 2048,
            "queue must not keep growing after the kick"
        );
        // Draining what was queued before the kick still works.
        while let Ok(push) = rx.try_recv() {
            slow.consumed(&push);
        }
        let channels = [b("c")];
        hub.remove_subscriber(&slow, channels.iter(), std::iter::empty());
        assert_eq!(hub.slow_disconnects(), 1);
        assert_eq!(hub.numsub(b"c"), 0);
    }

    #[test]
    fn consuming_frees_budget() {
        let hub = Hub::new();
        let (sub, mut rx) = Subscriber::new(1, 1024);
        hub.subscribe(&sub, &b("c"));
        let payload = Bytes::from(vec![b'x'; 300]);
        for _ in 0..100 {
            assert_eq!(hub.publish(&b("c"), &payload), 1);
            let push = rx.try_recv().unwrap();
            sub.consumed(&push);
        }
        assert!(!sub.was_kicked());
        assert_eq!(sub.queued_bytes(), 0);
    }
}
