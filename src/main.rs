//! `ferrum-db` server binary.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use clap::Parser;
use ferrum_db::clock::SystemClock;
use ferrum_db::config::parse_bytes;
use ferrum_db::db::EvictionPolicy;
use ferrum_db::persist::aof::FsyncPolicy;
use ferrum_db::{Config, Engine, Server};
use tracing_subscriber::EnvFilter;

/// An in-memory data store speaking RESP2 and RESP3.
#[derive(Parser, Debug)]
#[command(name = "ferrum-db", version, about)]
struct Args {
    /// Address to listen on.
    #[arg(long, env = "FERRUM_BIND", default_value = "127.0.0.1")]
    bind: String,
    /// TCP port.
    #[arg(long, env = "FERRUM_PORT", default_value_t = 6380)]
    port: u16,
    /// Number of keyspace shards.
    #[arg(long, env = "FERRUM_SHARDS", default_value_t = 16)]
    shards: usize,
    /// Directory for the AOF and snapshot files.
    #[arg(long, env = "FERRUM_DIR", default_value = ".")]
    dir: PathBuf,
    /// Enable the append-only file.
    #[arg(long, env = "FERRUM_AOF")]
    aof: bool,
    /// AOF fsync policy: always, everysec or no.
    #[arg(long, env = "FERRUM_AOF_FSYNC", default_value = "everysec", value_parser = parse_fsync)]
    aof_fsync: FsyncPolicy,
    /// Memory limit (for example 256mb); 0 disables the limit.
    #[arg(long, env = "FERRUM_MAXMEMORY", default_value = "0", value_parser = parse_size)]
    maxmemory: usize,
    /// Eviction policy: noeviction, allkeys-lru or volatile-lru.
    #[arg(long, env = "FERRUM_MAXMEMORY_POLICY", default_value = "noeviction", value_parser = parse_policy)]
    maxmemory_policy: EvictionPolicy,
    /// Require clients to AUTH with this password.
    #[arg(long, env = "FERRUM_REQUIREPASS", hide_env_values = true)]
    requirepass: Option<String>,
    /// Maximum simultaneous clients.
    #[arg(long, env = "FERRUM_MAXCLIENTS", default_value_t = 10_000)]
    max_clients: usize,
    /// Bytes a pub/sub subscriber may lag before being disconnected.
    #[arg(long, env = "FERRUM_PUBSUB_BUFFER", default_value = "8mb", value_parser = parse_size)]
    pubsub_buffer_limit: usize,
}

fn parse_fsync(s: &str) -> Result<FsyncPolicy, String> {
    FsyncPolicy::parse(s).ok_or_else(|| "expected always, everysec or no".into())
}

fn parse_policy(s: &str) -> Result<EvictionPolicy, String> {
    EvictionPolicy::parse(s)
        .ok_or_else(|| "expected noeviction, allkeys-lru or volatile-lru".into())
}

fn parse_size(s: &str) -> Result<usize, String> {
    parse_bytes(s).ok_or_else(|| "expected a size such as 4096, 64mb or 2g".into())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let args = Args::parse();
    let config = Config {
        bind: args.bind,
        port: args.port,
        shards: args.shards.max(1),
        dir: args.dir,
        aof_enabled: args.aof,
        aof_fsync: args.aof_fsync,
        maxmemory: args.maxmemory,
        maxmemory_policy: args.maxmemory_policy,
        requirepass: args.requirepass,
        max_clients: args.max_clients,
        pubsub_buffer_limit: args.pubsub_buffer_limit,
        ..Config::default()
    };
    let engine = match Engine::open(config, Arc::new(SystemClock)) {
        Ok(engine) => engine,
        Err(e) => {
            tracing::error!(error = %e, "cannot recover persistent state; refusing to start");
            return ExitCode::FAILURE;
        }
    };
    let server = match Server::bind(engine).await {
        Ok(server) => server,
        Err(e) => {
            tracing::error!(error = %e, "cannot bind listener");
            return ExitCode::FAILURE;
        }
    };
    if let Ok(addr) = server.local_addr() {
        tracing::info!(%addr, "ferrum-db ready");
    }
    server.run(shutdown_signal()).await;
    ExitCode::SUCCESS
}
