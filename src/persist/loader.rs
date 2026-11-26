//! AOF recovery: replay a log into the engine, tolerating a torn final record.

use std::fs::OpenOptions;
use std::io::Read;
use std::path::Path;

use bytes::{Bytes, BytesMut};

use super::PersistError;
use crate::engine::Engine;
use crate::resp::{Decoder, Frame};

/// Outcome of replaying an AOF.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct LoadReport {
    /// Commands applied.
    pub commands: u64,
    /// Bytes cut from the end of the file (an incomplete record or transaction).
    pub truncated_bytes: u64,
}

fn corrupt(offset: u64, reason: &str) -> PersistError {
    PersistError::Corrupt {
        offset,
        reason: reason.into(),
    }
}

/// Replays `path` into `engine`.
///
/// A file that ends in the middle of a command, or in the middle of a
/// `MULTI ... EXEC` block, is the normal result of a crash during an append; the
/// incomplete tail is cut off and replay succeeds. Anything else that fails to
/// parse is genuine corruption and aborts startup with the offending offset.
pub fn load_aof(path: &Path, engine: &Engine) -> Result<LoadReport, PersistError> {
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    let file_len = file.metadata()?.len();
    let mut decoder = Decoder::default();
    let mut buf = BytesMut::with_capacity(1 << 20);
    let mut chunk = vec![0u8; 1 << 20];
    let mut read_total = 0u64;
    // Offset just past the last complete record that is outside any open transaction.
    let mut committed = 0u64;
    // Offset where the record currently being decoded starts.
    let mut record_start = 0u64;
    let mut tx: Option<Vec<Vec<Bytes>>> = None;
    let mut report = LoadReport::default();

    loop {
        // Drain every complete frame currently buffered.
        loop {
            let frame_start = record_start;
            let frame = match decoder.decode(&mut buf) {
                Ok(Some(f)) => f,
                Ok(None) => break,
                Err(source) => {
                    return Err(PersistError::Protocol {
                        offset: frame_start,
                        source,
                    });
                }
            };
            let end = read_total - buf.len() as u64;
            record_start = end;
            let Frame::Array(items) = frame else {
                return Err(corrupt(frame_start, "record is not a command array"));
            };
            let args = items
                .into_iter()
                .map(|f| match f {
                    Frame::Bulk(b) => Ok(b),
                    _ => Err(corrupt(
                        frame_start,
                        "command argument is not a bulk string",
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?;
            if args.is_empty() {
                return Err(corrupt(frame_start, "empty command"));
            }
            let name = args[0].to_ascii_uppercase();
            match (name.as_slice(), tx.as_mut()) {
                (b"MULTI", None) => tx = Some(Vec::new()),
                (b"MULTI", Some(_)) => return Err(corrupt(frame_start, "nested MULTI")),
                (b"EXEC", Some(_)) => {
                    let queued = tx.take().unwrap_or_default();
                    for command in queued {
                        apply(engine, &command, frame_start, &mut report)?;
                    }
                    committed = end;
                }
                (b"EXEC", None) => return Err(corrupt(frame_start, "EXEC without MULTI")),
                (_, Some(queued)) => queued.push(args),
                (_, None) => {
                    apply(engine, &args, frame_start, &mut report)?;
                    committed = end;
                }
            }
        }
        let n = file.read(&mut chunk)?;
        if n == 0 {
            break;
        }
        read_total += n as u64;
        buf.extend_from_slice(&chunk[..n]);
    }

    // Anything after `committed` is an incomplete record or an unfinished transaction.
    let cut_to = committed;
    if cut_to < file_len {
        report.truncated_bytes = file_len - cut_to;
        file.set_len(cut_to)?;
        file.sync_all()?;
        tracing::warn!(
            truncated_bytes = report.truncated_bytes,
            offset = cut_to,
            "AOF ended in an incomplete record (crash during append); tail discarded"
        );
    }
    Ok(report)
}

fn apply(
    engine: &Engine,
    args: &[Bytes],
    offset: u64,
    report: &mut LoadReport,
) -> Result<(), PersistError> {
    engine
        .replay(args)
        .map_err(|e| corrupt(offset, &format!("command failed during replay: {e}")))?;
    report.commands += 1;
    Ok(())
}
