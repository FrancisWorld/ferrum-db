//! Property tests (and fuzz-style robustness checks) for the RESP decoder/encoder.
//!
//! These play the role of a fuzz target that runs on every `cargo test`: the decoder
//! must never panic, never loop, and never allocate from an untrusted length.
#![allow(missing_docs)]

use bytes::{Bytes, BytesMut};
use ferrum_db::resp::{Decoder, Frame, Limits, Protocol, encode};
use proptest::prelude::*;

fn leaf() -> impl Strategy<Value = Frame> {
    prop_oneof![
        "[ -~]{0,16}".prop_map(|s| Frame::Simple(s.into())),
        "[ -~]{0,16}".prop_map(Frame::Error),
        any::<i64>().prop_map(Frame::Integer),
        proptest::collection::vec(any::<u8>(), 0..40).prop_map(|v| Frame::Bulk(Bytes::from(v))),
        Just(Frame::Null),
        any::<bool>().prop_map(Frame::Boolean),
        any::<f64>()
            .prop_filter("NaN != NaN", |d| !d.is_nan())
            .prop_map(Frame::Double),
    ]
}

fn frame() -> impl Strategy<Value = Frame> {
    leaf().prop_recursive(4, 48, 6, |inner| {
        prop_oneof![
            proptest::collection::vec(inner.clone(), 0..6).prop_map(Frame::Array),
            proptest::collection::vec(inner.clone(), 0..6).prop_map(Frame::Set),
            proptest::collection::vec(inner.clone(), 0..6).prop_map(Frame::Push),
            proptest::collection::vec((inner.clone(), inner), 0..4).prop_map(Frame::Map),
        ]
    })
}

fn decode_chunks(
    wire: &[u8],
    cuts: &[usize],
    limits: Limits,
) -> Result<Vec<Frame>, ferrum_db::resp::ProtocolError> {
    let mut decoder = Decoder::new(limits);
    let mut buf = BytesMut::new();
    let mut frames = Vec::new();
    let mut at = 0;
    let mut points: Vec<usize> = cuts.iter().map(|c| c % (wire.len() + 1)).collect();
    points.push(wire.len());
    points.sort_unstable();
    for p in points {
        buf.extend_from_slice(&wire[at..p]);
        at = p;
        while let Some(f) = decoder.decode(&mut buf)? {
            frames.push(f);
        }
    }
    Ok(frames)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn resp3_round_trip_under_arbitrary_chunking(frames in proptest::collection::vec(frame(), 1..5), cuts in proptest::collection::vec(any::<usize>(), 0..12)) {
        let mut wire = BytesMut::new();
        for f in &frames {
            encode(f, Protocol::Resp3, &mut wire);
        }
        let decoded = decode_chunks(&wire, &cuts, Limits::default()).unwrap();
        prop_assert_eq!(decoded, frames);
    }

    #[test]
    fn commands_round_trip(args in proptest::collection::vec(proptest::collection::vec(any::<u8>(), 0..64), 1..8), cuts in proptest::collection::vec(any::<usize>(), 0..8)) {
        let frame = Frame::Array(args.iter().map(|a| Frame::Bulk(Bytes::from(a.clone()))).collect());
        let mut wire = BytesMut::new();
        encode(&frame, Protocol::Resp2, &mut wire);
        let decoded = decode_chunks(&wire, &cuts, Limits::default()).unwrap();
        prop_assert_eq!(decoded, vec![frame]);
    }

    #[test]
    fn arbitrary_bytes_never_panic_and_never_over_consume(data in proptest::collection::vec(any::<u8>(), 0..512), cuts in proptest::collection::vec(any::<usize>(), 0..8)) {
        let limits = Limits { max_bulk_len: 1024, max_aggregate_len: 64, max_line_len: 128, max_depth: 8 };
        // Either a clean list of frames or a protocol error; both are acceptable.
        let _ = decode_chunks(&data, &cuts, limits);
        let mut buf = BytesMut::from(&data[..]);
        let before = buf.len();
        let mut decoder = Decoder::new(limits);
        let _ = decoder.decode(&mut buf);
        prop_assert!(buf.len() <= before);
    }

    #[test]
    fn chunking_does_not_change_the_outcome(data in proptest::collection::vec(prop_oneof![Just(b'*'), Just(b'$'), Just(b':'), Just(b'+'), Just(b'\r'), Just(b'\n'), Just(b'1'), Just(b'a'), any::<u8>()], 0..200), cuts in proptest::collection::vec(any::<usize>(), 0..10)) {
        let limits = Limits { max_bulk_len: 4096, max_aggregate_len: 64, max_line_len: 128, max_depth: 8 };
        let whole = decode_chunks(&data, &[], limits);
        let split = decode_chunks(&data, &cuts, limits);
        match (whole, split) {
            (Ok(a), Ok(b)) => prop_assert_eq!(a, b),
            (Err(a), Err(b)) => prop_assert_eq!(a, b),
            (a, b) => prop_assert!(false, "outcomes differ: {a:?} vs {b:?}"),
        }
    }

    #[test]
    fn declared_lengths_are_bounded_by_limits(len in 0u64..u64::MAX) {
        let limits = Limits { max_bulk_len: 1 << 20, max_aggregate_len: 1 << 10, max_line_len: 64, max_depth: 4 };
        let mut decoder = Decoder::new(limits);
        let mut buf = BytesMut::from(format!("${len}\r\n").as_bytes());
        let result = decoder.decode(&mut buf);
        if len > (1 << 20) {
            prop_assert!(result.is_err());
        }
        // Reserved capacity must track the limit, not the attacker-chosen number.
        prop_assert!(buf.capacity() < (2 << 20));
    }
}

#[test]
fn deeply_nested_input_is_rejected_not_recursed() {
    let wire = "*1\r\n".repeat(100_000);
    let mut buf = BytesMut::from(wire.as_bytes());
    assert!(Decoder::default().decode(&mut buf).is_err());
}

#[test]
fn a_million_blank_lines_do_not_overflow_the_stack() {
    let mut buf = BytesMut::from("\r\n".repeat(1_000_000).as_bytes());
    assert_eq!(Decoder::default().decode(&mut buf).unwrap(), None);
    assert!(buf.is_empty());
}
