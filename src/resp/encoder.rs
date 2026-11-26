use bytes::{BufMut, BytesMut};

use super::{Frame, Protocol};

/// Serialises `frame` into `out` using the dialect `proto`.
///
/// RESP3-only types degrade the way real clients expect under RESP2: maps
/// flatten to arrays, sets and pushes become arrays, doubles become bulk
/// strings and booleans become integers.
pub fn encode(frame: &Frame, proto: Protocol, out: &mut BytesMut) {
    match frame {
        Frame::Simple(s) => line(out, b'+', s),
        Frame::Error(s) => line(out, b'-', s),
        Frame::Integer(n) => {
            out.put_u8(b':');
            put_int(out, *n);
            out.put_slice(b"\r\n");
        }
        Frame::Bulk(b) => {
            out.put_u8(b'$');
            put_int(out, b.len() as i64);
            out.put_slice(b"\r\n");
            out.put_slice(b);
            out.put_slice(b"\r\n");
        }
        Frame::Null => match proto {
            Protocol::Resp2 => out.put_slice(b"$-1\r\n"),
            Protocol::Resp3 => out.put_slice(b"_\r\n"),
        },
        Frame::NullArray => match proto {
            Protocol::Resp2 => out.put_slice(b"*-1\r\n"),
            Protocol::Resp3 => out.put_slice(b"_\r\n"),
        },
        Frame::Array(items) => aggregate(out, proto, b'*', items),
        Frame::Set(items) => {
            let marker = if proto == Protocol::Resp3 { b'~' } else { b'*' };
            aggregate(out, proto, marker, items);
        }
        Frame::Push(items) => {
            let marker = if proto == Protocol::Resp3 { b'>' } else { b'*' };
            aggregate(out, proto, marker, items);
        }
        Frame::Map(pairs) => {
            let resp3 = proto == Protocol::Resp3;
            out.put_u8(if resp3 { b'%' } else { b'*' });
            put_int(
                out,
                if resp3 { pairs.len() } else { pairs.len() * 2 } as i64,
            );
            out.put_slice(b"\r\n");
            for (k, v) in pairs {
                encode(k, proto, out);
                encode(v, proto, out);
            }
        }
        Frame::Double(d) => match proto {
            Protocol::Resp3 => line(out, b',', &format_double(*d)),
            Protocol::Resp2 => encode(&Frame::bulk(format_double(*d)), proto, out),
        },
        Frame::Boolean(b) => match proto {
            Protocol::Resp3 => out.put_slice(if *b { b"#t\r\n" } else { b"#f\r\n" }),
            Protocol::Resp2 => encode(&Frame::Integer(i64::from(*b)), proto, out),
        },
    }
}

/// Formats a double the way RESP expects: `inf`, `-inf`, `nan`, otherwise the
/// shortest representation that round-trips.
pub fn format_double(d: f64) -> String {
    if d.is_nan() {
        "nan".into()
    } else if d.is_infinite() {
        if d > 0.0 { "inf".into() } else { "-inf".into() }
    } else {
        format!("{d}")
    }
}

fn aggregate(out: &mut BytesMut, proto: Protocol, marker: u8, items: &[Frame]) {
    out.put_u8(marker);
    put_int(out, items.len() as i64);
    out.put_slice(b"\r\n");
    for item in items {
        encode(item, proto, out);
    }
}

/// Writes a status/error line, replacing CR/LF so a message can never inject frames.
fn line(out: &mut BytesMut, marker: u8, text: &str) {
    out.put_u8(marker);
    out.extend(
        text.bytes()
            .map(|b| if b == b'\r' || b == b'\n' { b' ' } else { b }),
    );
    out.put_slice(b"\r\n");
}

fn put_int(out: &mut BytesMut, n: i64) {
    let mut buf = [0u8; 20];
    let mut pos = buf.len();
    let mut v = n.unsigned_abs();
    loop {
        pos -= 1;
        buf[pos] = b'0' + (v % 10) as u8;
        v /= 10;
        if v == 0 {
            break;
        }
    }
    if n < 0 {
        out.put_u8(b'-');
    }
    out.put_slice(&buf[pos..]);
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    fn enc(frame: &Frame, proto: Protocol) -> Vec<u8> {
        let mut out = BytesMut::new();
        encode(frame, proto, &mut out);
        out.to_vec()
    }

    #[test]
    fn scalars() {
        assert_eq!(enc(&Frame::OK, Protocol::Resp2), b"+OK\r\n");
        assert_eq!(
            enc(&Frame::Integer(i64::MIN), Protocol::Resp2),
            b":-9223372036854775808\r\n"
        );
        assert_eq!(enc(&Frame::Integer(0), Protocol::Resp2), b":0\r\n");
        assert_eq!(
            enc(&Frame::Bulk(Bytes::from_static(b"hi")), Protocol::Resp2),
            b"$2\r\nhi\r\n"
        );
    }

    #[test]
    fn nulls_depend_on_protocol() {
        assert_eq!(enc(&Frame::Null, Protocol::Resp2), b"$-1\r\n");
        assert_eq!(enc(&Frame::Null, Protocol::Resp3), b"_\r\n");
        assert_eq!(enc(&Frame::NullArray, Protocol::Resp2), b"*-1\r\n");
        assert_eq!(enc(&Frame::NullArray, Protocol::Resp3), b"_\r\n");
    }

    #[test]
    fn resp3_types_degrade_under_resp2() {
        let map = Frame::Map(vec![(Frame::bulk("a"), Frame::Double(1.5))]);
        assert_eq!(enc(&map, Protocol::Resp3), b"%1\r\n$1\r\na\r\n,1.5\r\n");
        assert_eq!(
            enc(&map, Protocol::Resp2),
            b"*2\r\n$1\r\na\r\n$3\r\n1.5\r\n"
        );
        assert_eq!(enc(&Frame::Boolean(true), Protocol::Resp3), b"#t\r\n");
        assert_eq!(enc(&Frame::Boolean(true), Protocol::Resp2), b":1\r\n");
        let set = Frame::Set(vec![Frame::Integer(1)]);
        assert_eq!(enc(&set, Protocol::Resp3), b"~1\r\n:1\r\n");
        assert_eq!(enc(&set, Protocol::Resp2), b"*1\r\n:1\r\n");
        let push = Frame::Push(vec![Frame::Integer(1)]);
        assert_eq!(enc(&push, Protocol::Resp3), b">1\r\n:1\r\n");
        assert_eq!(enc(&push, Protocol::Resp2), b"*1\r\n:1\r\n");
    }

    #[test]
    fn status_lines_cannot_inject_frames() {
        assert_eq!(
            enc(&Frame::error("bad\r\n+OK"), Protocol::Resp2),
            b"-bad  +OK\r\n"
        );
    }

    #[test]
    fn doubles() {
        assert_eq!(format_double(1.0), "1");
        assert_eq!(format_double(-0.25), "-0.25");
        assert_eq!(format_double(f64::INFINITY), "inf");
        assert_eq!(format_double(f64::NEG_INFINITY), "-inf");
        assert_eq!(format_double(f64::NAN), "nan");
    }
}
