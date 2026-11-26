//! Decoder and encoder throughput.

use bytes::{Bytes, BytesMut};
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use ferrum_db::resp::{Decoder, Frame, Protocol, encode};

fn set_command(i: usize) -> Vec<u8> {
    let (key, value) = (format!("key:{i}"), "v".repeat(64));
    format!(
        "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
        key.len(),
        key,
        value.len(),
        value
    )
    .into_bytes()
}

fn bench_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("decode");

    let pipeline: Vec<u8> = (0..1000).flat_map(set_command).collect();
    group.throughput(Throughput::Bytes(pipeline.len() as u64));
    group.bench_function("1000 pipelined SET commands", |b| {
        b.iter_batched(
            || BytesMut::from(&pipeline[..]),
            |mut buf| {
                let mut decoder = Decoder::default();
                let mut n = 0;
                while decoder.decode(&mut buf).unwrap().is_some() {
                    n += 1;
                }
                assert_eq!(n, 1000);
            },
            BatchSize::LargeInput,
        );
    });

    let mut big = b"$1048576\r\n".to_vec();
    big.extend(std::iter::repeat_n(b'x', 1 << 20));
    big.extend_from_slice(b"\r\n");
    group.throughput(Throughput::Bytes(big.len() as u64));
    group.bench_function("one 1 MiB bulk string (zero-copy split)", |b| {
        b.iter_batched(
            || BytesMut::from(&big[..]),
            |mut buf| Decoder::default().decode(&mut buf).unwrap().unwrap(),
            BatchSize::LargeInput,
        );
    });

    let wire = set_command(7);
    group.throughput(Throughput::Bytes(wire.len() as u64));
    group.bench_function("one SET fed byte by byte", |b| {
        b.iter(|| {
            let mut decoder = Decoder::default();
            let mut buf = BytesMut::with_capacity(wire.len());
            let mut frame = None;
            for byte in &wire {
                buf.extend_from_slice(std::slice::from_ref(byte));
                frame = decoder.decode(&mut buf).unwrap();
            }
            frame.unwrap()
        });
    });
    group.finish();
}

fn bench_encode(c: &mut Criterion) {
    let reply = Frame::Array(
        (0..100)
            .map(|i| Frame::Bulk(Bytes::from(format!("member-{i}"))))
            .collect(),
    );
    let mut group = c.benchmark_group("encode");
    group.bench_function("array of 100 bulk strings", |b| {
        b.iter(|| {
            let mut out = BytesMut::with_capacity(2048);
            encode(&reply, Protocol::Resp2, &mut out);
            out
        });
    });
    group.finish();
}

criterion_group!(benches, bench_decode, bench_encode);
criterion_main!(benches);
