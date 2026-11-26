//! Time source abstraction so expiry logic can be tested deterministically.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// A source of wall-clock time in milliseconds since the Unix epoch.
pub trait Clock: Send + Sync {
    /// Current time in milliseconds.
    fn now_ms(&self) -> u64;
}

/// The real system clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as u64)
    }
}

/// A clock that only moves when told to; used by tests.
#[derive(Debug, Default)]
pub struct ManualClock(AtomicU64);

impl ManualClock {
    /// Creates a clock frozen at `start_ms`.
    pub fn new(start_ms: u64) -> Self {
        ManualClock(AtomicU64::new(start_ms))
    }

    /// Moves the clock forward by `ms`.
    pub fn advance(&self, ms: u64) {
        self.0.fetch_add(ms, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}
