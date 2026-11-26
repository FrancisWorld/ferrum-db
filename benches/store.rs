//! In-process store benchmarks: command execution without the network, the sorted
//! set data structure, and how shard count affects multi-threaded throughput.

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use ferrum_db::clock::SystemClock;
use ferrum_db::db::ZSet;
use ferrum_db::session::{Output, Session};
use ferrum_db::{Config, Engine};

fn engine(shards: usize) -> (Arc<Engine>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        port: 0,
        dir: dir.path().to_path_buf(),
        shards,
        ..Config::default()
    };
    (Engine::open(config, Arc::new(SystemClock)).unwrap(), dir)
}

fn args(parts: &[&str]) -> Vec<Bytes> {
    parts
        .iter()
        .map(|p| Bytes::copy_from_slice(p.as_bytes()))
        .collect()
}

fn bench_commands(c: &mut Criterion) {
    let (engine, _dir) = engine(16);
    let mut session = Session::new(&engine);
    let mut out = Output::default();
    for i in 0..10_000 {
        session.handle(
            &engine,
            args(&["SET", &format!("key:{i}"), "value"]),
            &mut out,
        );
    }
    for i in 0..10_000 {
        session.handle(
            &engine,
            args(&[
                "ZADD",
                "board",
                &(i * 7 % 10_000).to_string(),
                &format!("m{i}"),
            ]),
            &mut out,
        );
    }
    out.buf.clear();

    let mut group = c.benchmark_group("command");
    let mut i = 0usize;
    group.bench_function("GET (hit)", |b| {
        b.iter_batched(
            || {
                i = (i + 7919) % 10_000;
                args(&["GET", &format!("key:{i}")])
            },
            |a| {
                session.handle(&engine, a, &mut out);
                out.buf.clear();
            },
            BatchSize::SmallInput,
        );
    });
    group.bench_function("SET", |b| {
        b.iter_batched(
            || {
                i = (i + 7919) % 10_000;
                args(&["SET", &format!("key:{i}"), "value"])
            },
            |a| {
                session.handle(&engine, a, &mut out);
                out.buf.clear();
            },
            BatchSize::SmallInput,
        );
    });
    group.bench_function("INCR", |b| {
        b.iter_batched(
            || args(&["INCR", "counter"]),
            |a| {
                session.handle(&engine, a, &mut out);
                out.buf.clear();
            },
            BatchSize::SmallInput,
        );
    });
    group.bench_function("ZRANK in a 10k-member sorted set", |b| {
        b.iter_batched(
            || {
                i = (i + 7919) % 10_000;
                args(&["ZRANK", "board", &format!("m{i}")])
            },
            |a| {
                session.handle(&engine, a, &mut out);
                out.buf.clear();
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn bench_zset(c: &mut Criterion) {
    let mut group = c.benchmark_group("zset");
    for n in [1_000usize, 100_000] {
        let mut z = ZSet::new();
        for i in 0..n {
            z.insert(
                Bytes::from(format!("member:{i}")),
                ((i * 2_654_435_761) % 1_000_003) as f64,
            );
        }
        let mut i = 0usize;
        group.bench_function(format!("rank, n={n}"), |b| {
            b.iter(|| {
                i = (i + 7919) % n;
                z.rank(format!("member:{i}").as_bytes(), false)
            });
        });
        group.bench_function(format!("insert+remove, n={n}"), |b| {
            b.iter(|| {
                z.insert(Bytes::from_static(b"churn"), 123.0);
                z.remove(b"churn");
            });
        });
        group.bench_function(format!("range of 100 by rank, n={n}"), |b| {
            b.iter(|| z.range(n / 2, n / 2 + 99, false))
        });
    }
    group.finish();
}

/// Same workload, N threads, differing shard counts: shows what sharding buys.
fn bench_sharding(c: &mut Criterion) {
    let mut group = c.benchmark_group("sharding (4 threads, SET/GET mix)");
    for shards in [1usize, 16] {
        group.bench_function(format!("{shards} shard(s)"), |b| {
            b.iter_custom(|iters| {
                let (engine, _dir) = engine(shards);
                let per_thread = (iters / 4).max(1);
                let started = Instant::now();
                std::thread::scope(|scope| {
                    for t in 0..4 {
                        let engine = engine.clone();
                        scope.spawn(move || {
                            let mut session = Session::new(&engine);
                            let mut out = Output::default();
                            for i in 0..per_thread {
                                let key = format!("t{t}:{}", i % 512);
                                let cmd = if i % 4 == 0 {
                                    args(&["SET", &key, "value"])
                                } else {
                                    args(&["GET", &key])
                                };
                                session.handle(&engine, cmd, &mut out);
                                out.buf.clear();
                            }
                        });
                    }
                });
                started.elapsed()
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_commands, bench_zset, bench_sharding);
criterion_main!(benches);
