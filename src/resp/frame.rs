use std::borrow::Cow;

use bytes::Bytes;

/// Protocol version negotiated by a connection (`HELLO 2` / `HELLO 3`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Protocol {
    /// Classic RESP2: arrays, bulk strings, integers.
    #[default]
    Resp2,
    /// RESP3: adds maps, sets, doubles, booleans, nulls and push messages.
    Resp3,
}

/// A single RESP value, shared by requests and replies.
///
/// Bulk payloads are [`Bytes`], so frames decoded from the socket borrow the read
/// buffer instead of copying it.
#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    /// `+OK\r\n`
    Simple(Cow<'static, str>),
    /// `-ERR message\r\n`
    Error(String),
    /// `:42\r\n`
    Integer(i64),
    /// `$5\r\nhello\r\n`
    Bulk(Bytes),
    /// Null bulk string (`$-1`) in RESP2, `_` in RESP3.
    Null,
    /// Null array (`*-1`) in RESP2, `_` in RESP3.
    NullArray,
    /// `*N\r\n...`
    Array(Vec<Frame>),
    /// RESP3 map (`%`); flattened to an array under RESP2.
    Map(Vec<(Frame, Frame)>),
    /// RESP3 set (`~`); an array under RESP2.
    Set(Vec<Frame>),
    /// RESP3 double (`,`); a bulk string under RESP2.
    Double(f64),
    /// RESP3 boolean (`#`); an integer under RESP2.
    Boolean(bool),
    /// RESP3 out-of-band push (`>`); an array under RESP2.
    Push(Vec<Frame>),
}

impl Frame {
    /// The canonical `+OK` reply.
    pub const OK: Frame = Frame::Simple(Cow::Borrowed("OK"));

    /// Builds a simple string from a static literal.
    pub fn simple(s: &'static str) -> Frame {
        Frame::Simple(Cow::Borrowed(s))
    }

    /// Builds a bulk string by copying `data`.
    pub fn bulk(data: impl AsRef<[u8]>) -> Frame {
        Frame::Bulk(Bytes::copy_from_slice(data.as_ref()))
    }

    /// Builds an error frame.
    pub fn error(msg: impl Into<String>) -> Frame {
        Frame::Error(msg.into())
    }

    /// Builds an array of bulk strings.
    pub fn bulk_array<I>(items: I) -> Frame
    where
        I: IntoIterator<Item = Bytes>,
    {
        Frame::Array(items.into_iter().map(Frame::Bulk).collect())
    }
}
