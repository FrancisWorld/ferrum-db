use std::borrow::Cow;

use bytes::{Buf, Bytes, BytesMut};
use thiserror::Error;

use super::Frame;

/// Resource limits enforced while decoding untrusted input.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// Largest accepted bulk string payload, in bytes.
    pub max_bulk_len: usize,
    /// Largest accepted element count for an aggregate (array, map, set, push).
    pub max_aggregate_len: usize,
    /// Longest accepted header or inline line, in bytes.
    pub max_line_len: usize,
    /// Deepest accepted aggregate nesting.
    pub max_depth: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_bulk_len: 512 * 1024 * 1024,
            max_aggregate_len: 1024 * 1024,
            max_line_len: 64 * 1024,
            max_depth: 32,
        }
    }
}

/// Malformed input. The connection cannot be resynchronised after one of these.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ProtocolError {
    /// A length prefix was not a valid non-negative integer.
    #[error("invalid {0} length")]
    InvalidLength(&'static str),
    /// A length prefix exceeded the configured limit.
    #[error("{0} length exceeds the configured limit")]
    TooLarge(&'static str),
    /// A header or inline line exceeded `max_line_len` without a terminator.
    #[error("too big inline request or header line")]
    LineTooLong,
    /// A bulk payload was not followed by CRLF, or a line ended with a bare LF.
    #[error("expected CRLF terminator")]
    BadTerminator,
    /// The first byte of a frame is not a known RESP type marker.
    #[error("unknown type byte '{}'", .0.escape_ascii())]
    UnknownType(u8),
    /// An integer field could not be parsed.
    #[error("invalid integer")]
    InvalidInteger,
    /// A double field could not be parsed.
    #[error("invalid double")]
    InvalidDouble,
    /// A boolean field was neither `t` nor `f`.
    #[error("invalid boolean")]
    InvalidBoolean,
    /// Aggregates were nested deeper than `max_depth`.
    #[error("aggregate nesting too deep")]
    TooDeep,
}

#[derive(Clone, Copy)]
enum Kind {
    Array,
    Set,
    Push,
    Map,
}

struct Partial {
    kind: Kind,
    remaining: usize,
    items: Vec<Frame>,
}

impl Partial {
    fn finish(self) -> Frame {
        match self.kind {
            Kind::Array => Frame::Array(self.items),
            Kind::Set => Frame::Set(self.items),
            Kind::Push => Frame::Push(self.items),
            Kind::Map => {
                let mut pairs = Vec::with_capacity(self.items.len() / 2);
                let mut it = self.items.into_iter();
                while let (Some(k), Some(v)) = (it.next(), it.next()) {
                    pairs.push((k, v));
                }
                Frame::Map(pairs)
            }
        }
    }
}

enum Step {
    NeedMore,
    Frame(Frame),
    Opened,
}

/// Resumable RESP decoder.
///
/// Bytes are consumed from the caller's buffer exactly once: a half-received
/// array keeps its already-decoded elements on an explicit stack, and a half
/// received bulk string waits for its known length, so a frame that arrives in
/// many small reads never triggers a rescan. Bulk payloads are split off the
/// read buffer as [`Bytes`] without copying.
///
/// Besides RESP frames, a top-level line that does not start with a type marker
/// is decoded as an *inline command* (whitespace separated words), so `telnet`
/// and `nc` work.
pub struct Decoder {
    limits: Limits,
    stack: Vec<Partial>,
    pending_bulk: Option<usize>,
}

impl Default for Decoder {
    fn default() -> Self {
        Decoder::new(Limits::default())
    }
}

impl Decoder {
    /// Creates a decoder enforcing `limits`.
    pub fn new(limits: Limits) -> Self {
        Decoder {
            limits,
            stack: Vec::new(),
            pending_bulk: None,
        }
    }

    /// True when no partially decoded frame is buffered inside the decoder.
    pub fn is_idle(&self) -> bool {
        self.stack.is_empty() && self.pending_bulk.is_none()
    }

    /// Attempts to decode one complete frame from the front of `buf`.
    ///
    /// Returns `Ok(None)` when more input is required; consumed bytes are
    /// removed from `buf` either way.
    pub fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Frame>, ProtocolError> {
        loop {
            let mut done = match self.step(buf)? {
                Step::NeedMore => return Ok(None),
                Step::Opened => continue,
                Step::Frame(frame) => frame,
            };
            loop {
                let Some(parent) = self.stack.last_mut() else {
                    return Ok(Some(done));
                };
                parent.items.push(done);
                parent.remaining -= 1;
                if parent.remaining > 0 {
                    break;
                }
                done = self.stack.pop().map(Partial::finish).unwrap_or(Frame::Null);
            }
        }
    }

    fn step(&mut self, buf: &mut BytesMut) -> Result<Step, ProtocolError> {
        if let Some(len) = self.pending_bulk {
            return self.read_bulk_payload(buf, len);
        }
        let Some(&marker) = buf.first() else {
            return Ok(Step::NeedMore);
        };
        if !is_marker(marker) && !self.stack.is_empty() {
            return Err(ProtocolError::UnknownType(marker));
        }
        let Some(line_end) = self.find_line(buf)? else {
            return Ok(Step::NeedMore);
        };
        // `line_end` is the index of the LF; the header excludes the CR before it.
        let is_inline = self.stack.is_empty() && !is_marker(marker);
        if is_inline {
            let words = split_inline(&buf[..line_end]);
            buf.advance(line_end + 1);
            if words.is_empty() {
                // Blank line: nothing to deliver, let the caller loop again.
                return Ok(Step::Opened);
            }
            return Ok(Step::Frame(Frame::Array(
                words.into_iter().map(Frame::Bulk).collect(),
            )));
        }
        if buf[line_end - 1] != b'\r' {
            return Err(ProtocolError::BadTerminator);
        }
        let header = &buf[1..line_end - 1];
        let step = match marker {
            b'+' => Step::Frame(Frame::Simple(Cow::Owned(lossy(header)))),
            b'-' => Step::Frame(Frame::Error(lossy(header))),
            b':' => Step::Frame(Frame::Integer(parse_int(header)?)),
            b'_' => Step::Frame(Frame::Null),
            b'#' => match header {
                b"t" => Step::Frame(Frame::Boolean(true)),
                b"f" => Step::Frame(Frame::Boolean(false)),
                _ => return Err(ProtocolError::InvalidBoolean),
            },
            b',' => Step::Frame(Frame::Double(parse_double(header)?)),
            b'$' => {
                let len = parse_int(header)?;
                if len == -1 {
                    Step::Frame(Frame::Null)
                } else {
                    let len = self.checked_len(len, "bulk", self.limits.max_bulk_len)?;
                    self.pending_bulk = Some(len);
                    buf.advance(line_end + 1);
                    // No `reserve(len)`: the declared length is attacker-controlled, so the
                    // buffer must only grow as bytes actually arrive.
                    return Ok(Step::Opened);
                }
            }
            b'*' | b'~' | b'>' | b'%' => {
                let n = parse_int(header)?;
                if n == -1 && marker == b'*' {
                    Step::Frame(Frame::NullArray)
                } else {
                    let n = self.checked_len(n, "aggregate", self.limits.max_aggregate_len)?;
                    let kind = match marker {
                        b'*' => Kind::Array,
                        b'~' => Kind::Set,
                        b'>' => Kind::Push,
                        _ => Kind::Map,
                    };
                    let remaining = if matches!(kind, Kind::Map) { n * 2 } else { n };
                    if remaining == 0 {
                        Step::Frame(
                            Partial {
                                kind,
                                remaining,
                                items: Vec::new(),
                            }
                            .finish(),
                        )
                    } else {
                        if self.stack.len() >= self.limits.max_depth {
                            return Err(ProtocolError::TooDeep);
                        }
                        // Never trust a declared length for allocation.
                        let items = Vec::with_capacity(remaining.min(1024));
                        self.stack.push(Partial {
                            kind,
                            remaining,
                            items,
                        });
                        buf.advance(line_end + 1);
                        return Ok(Step::Opened);
                    }
                }
            }
            other => return Err(ProtocolError::UnknownType(other)),
        };
        buf.advance(line_end + 1);
        Ok(step)
    }

    fn read_bulk_payload(&mut self, buf: &mut BytesMut, len: usize) -> Result<Step, ProtocolError> {
        if buf.len() < len + 2 {
            return Ok(Step::NeedMore);
        }
        if &buf[len..len + 2] != b"\r\n" {
            return Err(ProtocolError::BadTerminator);
        }
        let payload: Bytes = buf.split_to(len).freeze();
        buf.advance(2);
        self.pending_bulk = None;
        Ok(Step::Frame(Frame::Bulk(payload)))
    }

    /// Index of the next LF, or `None` if the line is still incomplete.
    fn find_line(&self, buf: &BytesMut) -> Result<Option<usize>, ProtocolError> {
        match memchr::memchr(b'\n', buf) {
            Some(i) if i > self.limits.max_line_len => Err(ProtocolError::LineTooLong),
            Some(i) => Ok(Some(i)),
            None if buf.len() > self.limits.max_line_len => Err(ProtocolError::LineTooLong),
            None => Ok(None),
        }
    }

    fn checked_len(
        &self,
        raw: i64,
        what: &'static str,
        max: usize,
    ) -> Result<usize, ProtocolError> {
        let len = usize::try_from(raw).map_err(|_| ProtocolError::InvalidLength(what))?;
        if len > max {
            return Err(ProtocolError::TooLarge(what));
        }
        Ok(len)
    }
}

fn is_marker(b: u8) -> bool {
    matches!(
        b,
        b'+' | b'-' | b':' | b'$' | b'*' | b'_' | b'#' | b',' | b'%' | b'~' | b'>'
    )
}

fn lossy(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn parse_int(bytes: &[u8]) -> Result<i64, ProtocolError> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .ok_or(ProtocolError::InvalidInteger)
}

fn parse_double(bytes: &[u8]) -> Result<f64, ProtocolError> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .ok_or(ProtocolError::InvalidDouble)
}

fn split_inline(line: &[u8]) -> Vec<Bytes> {
    line.split(|b| b.is_ascii_whitespace())
        .filter(|w| !w.is_empty())
        .map(Bytes::copy_from_slice)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_all(input: &[u8]) -> Result<Vec<Frame>, ProtocolError> {
        let mut dec = Decoder::default();
        let mut buf = BytesMut::from(input);
        let mut out = Vec::new();
        while let Some(f) = dec.decode(&mut buf)? {
            out.push(f);
        }
        Ok(out)
    }

    #[test]
    fn scalars() {
        let frames =
            decode_all(b"+OK\r\n-ERR boom\r\n:-42\r\n$5\r\nhello\r\n$-1\r\n_\r\n").unwrap();
        assert_eq!(
            frames,
            vec![
                Frame::simple("OK"),
                Frame::error("ERR boom"),
                Frame::Integer(-42),
                Frame::bulk("hello"),
                Frame::Null,
                Frame::Null,
            ]
        );
    }

    #[test]
    fn resp3_types() {
        let frames = decode_all(
            b"#t\r\n#f\r\n,3.5\r\n,inf\r\n%1\r\n+k\r\n:1\r\n~2\r\n:1\r\n:2\r\n>1\r\n+x\r\n",
        )
        .unwrap();
        assert_eq!(frames[0], Frame::Boolean(true));
        assert_eq!(frames[1], Frame::Boolean(false));
        assert_eq!(frames[2], Frame::Double(3.5));
        assert_eq!(frames[3], Frame::Double(f64::INFINITY));
        assert_eq!(
            frames[4],
            Frame::Map(vec![(Frame::simple("k"), Frame::Integer(1))])
        );
        assert_eq!(
            frames[5],
            Frame::Set(vec![Frame::Integer(1), Frame::Integer(2)])
        );
        assert_eq!(frames[6], Frame::Push(vec![Frame::simple("x")]));
    }

    #[test]
    fn nested_and_empty_aggregates() {
        let frames = decode_all(b"*2\r\n*0\r\n*-1\r\n%0\r\n").unwrap();
        assert_eq!(
            frames[0],
            Frame::Array(vec![Frame::Array(vec![]), Frame::NullArray])
        );
        assert_eq!(frames[1], Frame::Map(vec![]));
    }

    #[test]
    fn binary_safe_bulk() {
        let frames = decode_all(b"$6\r\na\r\nb\0c\r\n").unwrap();
        assert_eq!(frames, vec![Frame::bulk(b"a\r\nb\0c")]);
    }

    #[test]
    fn byte_at_a_time_matches_whole_buffer() {
        let wire: &[u8] = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\n*1\r\n$4\r\nPING\r\n";
        let whole = decode_all(wire).unwrap();
        let mut dec = Decoder::default();
        let mut buf = BytesMut::new();
        let mut got = Vec::new();
        for b in wire {
            buf.extend_from_slice(&[*b]);
            while let Some(f) = dec.decode(&mut buf).unwrap() {
                got.push(f);
            }
        }
        assert_eq!(got, whole);
        assert!(dec.is_idle());
    }

    #[test]
    fn bulk_is_zero_copy_slice_of_read_buffer() {
        let mut buf = BytesMut::with_capacity(1024);
        buf.extend_from_slice(b"$5\r\nhello\r\n");
        let before = buf.as_ptr() as usize;
        let frame = Decoder::default().decode(&mut buf).unwrap().unwrap();
        let Frame::Bulk(b) = frame else {
            panic!("expected bulk")
        };
        assert_eq!(
            b.as_ptr() as usize,
            before + 4,
            "payload must alias the input buffer"
        );
    }

    #[test]
    fn inline_commands() {
        let frames = decode_all(b"PING\r\nset  k   v\n\r\n  \r\nGET k\r\n").unwrap();
        assert_eq!(
            frames,
            vec![
                Frame::Array(vec![Frame::bulk("PING")]),
                Frame::Array(vec![Frame::bulk("set"), Frame::bulk("k"), Frame::bulk("v")]),
                Frame::Array(vec![Frame::bulk("GET"), Frame::bulk("k")]),
            ]
        );
    }

    #[test]
    fn rejects_malformed_input() {
        assert_eq!(
            decode_all(b":12x\r\n").unwrap_err(),
            ProtocolError::InvalidInteger
        );
        assert_eq!(
            decode_all(b"$-5\r\n").unwrap_err(),
            ProtocolError::InvalidLength("bulk")
        );
        assert_eq!(
            decode_all(b"$3\r\nabcde\r\n").unwrap_err(),
            ProtocolError::BadTerminator
        );
        assert_eq!(
            decode_all(b"+OK\n").unwrap_err(),
            ProtocolError::BadTerminator
        );
        assert_eq!(
            decode_all(b"#x\r\n").unwrap_err(),
            ProtocolError::InvalidBoolean
        );
        assert_eq!(
            decode_all(b",abc\r\n").unwrap_err(),
            ProtocolError::InvalidDouble
        );
        assert_eq!(
            decode_all(b"*-3\r\n").unwrap_err(),
            ProtocolError::InvalidLength("aggregate")
        );
    }

    #[test]
    fn enforces_limits() {
        let limits = Limits {
            max_bulk_len: 8,
            max_aggregate_len: 2,
            max_line_len: 16,
            max_depth: 2,
        };
        let run = |input: &[u8]| {
            let mut dec = Decoder::new(limits);
            dec.decode(&mut BytesMut::from(input))
        };
        assert_eq!(run(b"$9\r\n").unwrap_err(), ProtocolError::TooLarge("bulk"));
        assert_eq!(
            run(b"*3\r\n").unwrap_err(),
            ProtocolError::TooLarge("aggregate")
        );
        assert_eq!(
            run(b"*1\r\n*1\r\n*1\r\n:1\r\n").unwrap_err(),
            ProtocolError::TooDeep
        );
        assert_eq!(run(&[b'x'; 40]).unwrap_err(), ProtocolError::LineTooLong);
    }

    #[test]
    fn declared_bulk_length_does_not_reserve_capacity() {
        // 512 MiB declared, a few bytes delivered, then the peer stalls.
        let mut dec = Decoder::new(Limits {
            max_bulk_len: 512 << 20,
            ..Limits::default()
        });
        let mut buf = BytesMut::with_capacity(64);
        buf.extend_from_slice(b"$536870912\r\nabc");
        assert_eq!(dec.decode(&mut buf).unwrap(), None);
        assert!(
            buf.capacity() < 1 << 20,
            "capacity {} tracks the declared length",
            buf.capacity()
        );
        // More data arrives: capacity follows the bytes received, not the declaration.
        buf.extend_from_slice(&[b'x'; 4096]);
        assert_eq!(dec.decode(&mut buf).unwrap(), None);
        assert!(buf.capacity() < 1 << 20);
    }

    #[test]
    fn huge_declared_length_does_not_preallocate() {
        let mut dec = Decoder::default();
        let mut buf = BytesMut::from(&b"*1000000\r\n:1\r\n"[..]);
        assert_eq!(dec.decode(&mut buf).unwrap(), None);
        assert!(!dec.is_idle());
    }
}
