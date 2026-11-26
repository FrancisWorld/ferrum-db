//! Command-level errors with the exact wire messages clients expect.

use thiserror::Error;

use crate::resp::Frame;

/// A command failure that is reported to the client as a RESP error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CmdError {
    /// Operation against a key holding another type.
    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,
    /// Unparseable or contradictory options.
    #[error("ERR syntax error")]
    Syntax,
    /// An argument that must be an integer is not one.
    #[error("ERR value is not an integer or out of range")]
    NotInteger,
    /// An argument that must be a float is not one.
    #[error("ERR value is not a valid float")]
    NotFloat,
    /// Integer arithmetic would overflow.
    #[error("ERR increment or decrement would overflow")]
    Overflow,
    /// A computed score is NaN.
    #[error("ERR resulting score is not a number (NaN)")]
    NanScore,
    /// List index outside the list.
    #[error("ERR index out of range")]
    IndexOutOfRange,
    /// `RENAME` of a missing key.
    #[error("ERR no such key")]
    NoSuchKey,
    /// A numeric argument is negative where a positive one is required.
    #[error("ERR value is out of range, must be positive")]
    MustBePositive,
    /// An expiry argument is zero, negative or overflows.
    #[error("ERR invalid expire time in '{0}' command")]
    InvalidExpire(String),
    /// Wrong argument count.
    #[error("ERR wrong number of arguments for '{0}' command")]
    Arity(String),
    /// Unknown command name.
    #[error("ERR unknown command '{0}'")]
    UnknownCommand(String),
    /// `maxmemory` reached and nothing can be evicted.
    #[error("OOM command not allowed when used memory > 'maxmemory'.")]
    Oom,
    /// A score or range bound is not a valid float.
    #[error("ERR min or max is not a float")]
    BadRange,
    /// Free-form `ERR` message.
    #[error("ERR {0}")]
    Other(String),
    /// A message that already carries its own prefix (`NOAUTH`, `EXECABORT`, ...).
    #[error("{0}")]
    Raw(String),
}

impl CmdError {
    /// Convenience constructor for free-form errors.
    pub fn other(msg: impl Into<String>) -> Self {
        CmdError::Other(msg.into())
    }
}

impl From<CmdError> for Frame {
    fn from(e: CmdError) -> Frame {
        Frame::Error(e.to_string())
    }
}

/// Result of executing a command.
pub type Reply = Result<Frame, CmdError>;
