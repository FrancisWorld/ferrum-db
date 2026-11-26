//! `ferrum-load`: a small RESP load generator for measuring a running server.
//!
//! It drives N connections, each sending batches of `--pipeline` commands and
//! waiting for all replies, and reports throughput plus batch round-trip latency
//! percentiles. It speaks plain RESP2, so it can also be pointed at other
//! RESP-compatible servers for a like-for-like comparison.

use std::net::SocketAddr;
use std::process::ExitCode;
use std::time::Instant;

use bytes::BytesMut;
use clap::{Parser, ValueEnum};
use ferrum_db::resp::{Decoder, Frame};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Copy, Clone, Debug, ValueEnum)]
enum Workload {
    /// `SET key value` on random keys.
    Set,
    /// `GET key` on random keys (pre-populated first).
    Get,
    /// 80% `GET`, 20% `SET`.
    Mixed,
    /// `INCR` on random counters.
    Incr,
    /// `LPUSH` onto a small set of lists.
    Lpush,
    /// `ZADD` random scores into one sorted set.
    Zadd,
}

/// Load generator for RESP servers.
#[derive(Parser, Debug)]
#[command(name = "ferrum-load", version)]
struct Args {
    /// Server address.
    #[arg(long, default_value = "127.0.0.1:6380")]
    addr: SocketAddr,
    /// Concurrent connections.
    #[arg(long, short = 'c', default_value_t = 50)]
    clients: usize,
    /// Total requests across all connections.
    #[arg(long, short = 'n', default_value_t = 200_000)]
    requests: usize,
    /// Commands sent back-to-back before reading replies.
    #[arg(long, short = 'P', default_value_t = 1)]
    pipeline: usize,
    /// What to run.
    #[arg(long, value_enum, default_value_t = Workload::Mixed)]
    workload: Workload,
    /// Distinct keys.
    #[arg(long, default_value_t = 10_000)]
    keyspace: usize,
    /// Value size in bytes.
    #[arg(long, default_value_t = 64)]
    value_size: usize,
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

fn encode(parts: &[&[u8]], out: &mut Vec<u8>) {
    out.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
    for p in parts {
        out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        out.extend_from_slice(p);
        out.extend_from_slice(b"\r\n");
    }
}

fn build_command(workload: Workload, rng: &mut Rng, args: &Args, value: &[u8], out: &mut Vec<u8>) {
    let id = rng.next() as usize % args.keyspace;
    let key = format!("key:{id}");
    match workload {
        Workload::Set => encode(&[b"SET", key.as_bytes(), value], out),
        Workload::Get => encode(&[b"GET", key.as_bytes()], out),
        Workload::Mixed => {
            if rng.next() % 10 < 8 {
                build_command(Workload::Get, rng, args, value, out);
            } else {
                build_command(Workload::Set, rng, args, value, out);
            }
        }
        Workload::Incr => encode(&[b"INCR", format!("counter:{id}").as_bytes()], out),
        Workload::Lpush => encode(
            &[b"LPUSH", format!("list:{}", id % 64).as_bytes(), value],
            out,
        ),
        Workload::Zadd => {
            let score = (rng.next() % 1_000_000).to_string();
            encode(
                &[b"ZADD", b"leaderboard", score.as_bytes(), key.as_bytes()],
                out,
            );
        }
    }
}

struct Outcome {
    latencies_us: Vec<u32>,
    errors: usize,
}

async fn read_replies(
    stream: &mut TcpStream,
    decoder: &mut Decoder,
    buf: &mut BytesMut,
    n: usize,
) -> std::io::Result<usize> {
    let mut errors = 0;
    let mut got = 0;
    while got < n {
        match decoder.decode(buf).map_err(std::io::Error::other)? {
            Some(Frame::Error(_)) => {
                errors += 1;
                got += 1;
            }
            Some(_) => got += 1,
            None => {
                if stream.read_buf(buf).await? == 0 {
                    return Err(std::io::ErrorKind::UnexpectedEof.into());
                }
            }
        }
    }
    Ok(errors)
}

async fn run_client(
    args: &Args,
    workload: Workload,
    requests: usize,
    seed: u64,
) -> std::io::Result<Outcome> {
    let mut stream = TcpStream::connect(args.addr).await?;
    stream.set_nodelay(true)?;
    let mut rng = Rng(seed | 1);
    let value = vec![b'x'; args.value_size];
    let mut decoder = Decoder::default();
    let mut buf = BytesMut::with_capacity(64 * 1024);
    let mut wire = Vec::with_capacity(args.pipeline * (args.value_size + 64));
    let mut outcome = Outcome {
        latencies_us: Vec::with_capacity(requests / args.pipeline + 1),
        errors: 0,
    };
    let mut remaining = requests;
    while remaining > 0 {
        let batch = remaining.min(args.pipeline);
        wire.clear();
        for _ in 0..batch {
            build_command(workload, &mut rng, args, &value, &mut wire);
        }
        let started = Instant::now();
        stream.write_all(&wire).await?;
        outcome.errors += read_replies(&mut stream, &mut decoder, &mut buf, batch).await?;
        outcome
            .latencies_us
            .push(started.elapsed().as_micros().min(u128::from(u32::MAX)) as u32);
        remaining -= batch;
    }
    Ok(outcome)
}

async fn prefill(args: &Args) -> std::io::Result<()> {
    let mut stream = TcpStream::connect(args.addr).await?;
    let value = vec![b'x'; args.value_size];
    let mut decoder = Decoder::default();
    let mut buf = BytesMut::new();
    for chunk_start in (0..args.keyspace).step_by(1000) {
        let mut wire = Vec::new();
        let end = (chunk_start + 1000).min(args.keyspace);
        for id in chunk_start..end {
            encode(&[b"SET", format!("key:{id}").as_bytes(), &value], &mut wire);
        }
        stream.write_all(&wire).await?;
        read_replies(&mut stream, &mut decoder, &mut buf, end - chunk_start).await?;
    }
    Ok(())
}

fn percentile(sorted: &[u32], p: f64) -> u32 {
    if sorted.is_empty() {
        return 0;
    }
    sorted[(((sorted.len() - 1) as f64) * p).round() as usize]
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    if args.clients == 0 || args.requests == 0 || args.pipeline == 0 || args.keyspace == 0 {
        eprintln!("clients, requests, pipeline and keyspace must be positive");
        return ExitCode::from(2);
    }
    if matches!(args.workload, Workload::Get | Workload::Mixed)
        && let Err(e) = prefill(&args).await
    {
        eprintln!("prefill failed: {e}");
        return ExitCode::FAILURE;
    }
    let per_client = args.requests / args.clients;
    let args = std::sync::Arc::new(args);
    let started = Instant::now();
    let tasks: Vec<_> = (0..args.clients)
        .map(|i| {
            let args = args.clone();
            tokio::spawn(async move {
                run_client(
                    &args,
                    args.workload,
                    per_client.max(1),
                    0x9E37_79B9 ^ ((i as u64 + 1) * 0x2545_F491),
                )
                .await
            })
        })
        .collect();
    let mut latencies = Vec::new();
    let mut errors = 0;
    for task in tasks {
        match task.await {
            Ok(Ok(o)) => {
                errors += o.errors;
                latencies.extend(o.latencies_us);
            }
            Ok(Err(e)) => {
                eprintln!("client failed: {e}");
                return ExitCode::FAILURE;
            }
            Err(e) => {
                eprintln!("client panicked: {e}");
                return ExitCode::FAILURE;
            }
        }
    }
    let elapsed = started.elapsed();
    latencies.sort_unstable();
    let total = per_client.max(1) * args.clients;
    println!(
        "workload={:?} clients={} pipeline={} value_size={}B keyspace={}",
        args.workload, args.clients, args.pipeline, args.value_size, args.keyspace
    );
    println!(
        "requests:   {total} in {:.3}s ({errors} error replies)",
        elapsed.as_secs_f64()
    );
    println!(
        "throughput: {:.0} requests/s",
        total as f64 / elapsed.as_secs_f64().max(f64::EPSILON)
    );
    println!(
        "batch round-trip latency (us): p50={} p95={} p99={} max={}",
        percentile(&latencies, 0.50),
        percentile(&latencies, 0.95),
        percentile(&latencies, 0.99),
        latencies.last().copied().unwrap_or(0)
    );
    ExitCode::SUCCESS
}
