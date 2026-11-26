//! ferrum-db: an in-memory data store speaking RESP2 and RESP3.
//!
//! The crate is organised bottom-up:
//!
//! * [`resp`]: wire protocol (incremental decoder, encoder)
//! * [`db`]: sharded keyspace, value types, TTL index, eviction
//! * [`cmd`]: command table and handlers
//! * [`persist`]: append-only file, rewrite, snapshots, recovery
//! * [`pubsub`]: channel and pattern hub with slow-subscriber protection
//! * [`engine`], [`session`], [`server`]: execution, per-connection state, TCP

pub mod clock;
pub mod cmd;
pub mod config;
pub mod db;
pub mod engine;
pub mod error;
pub mod glob;
pub mod persist;
pub mod pubsub;
pub mod resp;
pub mod server;
pub mod session;

pub use config::Config;
pub use engine::Engine;
pub use server::Server;
