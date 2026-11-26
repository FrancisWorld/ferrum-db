//! Durability: append-only file, background rewrite, binary snapshots and recovery.

pub mod aof;
pub mod loader;
pub mod rewrite;
pub mod snapshot;

use std::io;

use thiserror::Error;

use crate::resp::ProtocolError;

/// Failure while reading or writing persistent state.
#[derive(Debug, Error)]
pub enum PersistError {
    /// Underlying I/O failure.
    #[error("i/o error: {0}")]
    Io(#[from] io::Error),
    /// The file is damaged in a way that is not a clean truncated tail.
    #[error("corrupt data at byte {offset}: {reason}")]
    Corrupt {
        /// Offset of the last fully valid record boundary.
        offset: u64,
        /// What was wrong.
        reason: String,
    },
    /// A malformed RESP record inside the AOF.
    #[error("malformed AOF record at byte {offset}: {source}")]
    Protocol {
        /// Offset of the last fully valid record boundary.
        offset: u64,
        /// Decoder error.
        source: ProtocolError,
    },
}

/// Flushes a directory entry so a preceding `rename` survives power loss.
pub(crate) fn sync_parent(path: &std::path::Path) -> io::Result<()> {
    match path.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => std::fs::File::open(dir)?.sync_all(),
        _ => std::fs::File::open(".")?.sync_all(),
    }
}
