//! Compacting an in-memory dataset into the minimal command stream that rebuilds it.

use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::Path;

use bytes::Bytes;

use super::aof::encode_command;
use crate::db::{DumpEntry, Value};

/// Elements per generated `RPUSH` / `HSET` / `SADD` / `ZADD` so no single record is huge.
const CHUNK: usize = 64;

/// Expands one entry into the commands that recreate it, including its TTL.
pub fn commands_for(entry: &DumpEntry) -> Vec<Vec<Bytes>> {
    let key = &entry.key;
    let cmd = |name: &'static str| vec![Bytes::from_static(name.as_bytes()), key.clone()];
    let mut out = Vec::new();
    match &entry.value {
        Value::Str(v) => {
            let mut c = cmd("SET");
            c.push(v.clone());
            out.push(c);
        }
        Value::List(l) => {
            let items: Vec<&Bytes> = l.items().iter().collect();
            for chunk in items.chunks(CHUNK) {
                let mut c = cmd("RPUSH");
                c.extend(chunk.iter().map(|b| (*b).clone()));
                out.push(c);
            }
        }
        Value::Hash(h) => {
            let pairs: Vec<_> = h.iter().collect();
            for chunk in pairs.chunks(CHUNK) {
                let mut c = cmd("HSET");
                for (f, v) in chunk {
                    c.push((*f).clone());
                    c.push((*v).clone());
                }
                out.push(c);
            }
        }
        Value::Set(s) => {
            let members: Vec<&Bytes> = s.iter().collect();
            for chunk in members.chunks(CHUNK) {
                let mut c = cmd("SADD");
                c.extend(chunk.iter().map(|b| (*b).clone()));
                out.push(c);
            }
        }
        Value::ZSet(z) => {
            let members: Vec<_> = z.iter().collect();
            for chunk in members.chunks(CHUNK) {
                let mut c = cmd("ZADD");
                for (m, s) in chunk {
                    c.push(Bytes::from(crate::resp::format_double(*s)));
                    c.push((*m).clone());
                }
                out.push(c);
            }
        }
    }
    if let Some(at) = entry.expire_at {
        out.push(vec![
            Bytes::from_static(b"PEXPIREAT"),
            key.clone(),
            Bytes::from(at.to_string()),
        ]);
    }
    out
}

/// Writes `entries` as an AOF to `file` (buffered) and fsyncs it.
pub fn write_aof(file: &mut File, entries: &[DumpEntry]) -> io::Result<u64> {
    let mut out = BufWriter::with_capacity(1 << 20, &mut *file);
    let mut scratch = Vec::new();
    let mut written = 0u64;
    for entry in entries {
        for command in commands_for(entry) {
            scratch.clear();
            encode_command(&command, &mut scratch);
            out.write_all(&scratch)?;
            written += scratch.len() as u64;
        }
    }
    out.flush()?;
    drop(out);
    file.sync_all()?;
    Ok(written)
}

/// Creates (or replaces) `path` with a rewritten AOF of `entries`, atomically.
pub fn write_aof_file(path: &Path, entries: &[DumpEntry]) -> io::Result<()> {
    let tmp = path.with_extension("aof.tmp");
    let mut file = File::create(&tmp)?;
    write_aof(&mut file, entries)?;
    std::fs::rename(&tmp, path)?;
    super::sync_parent(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{HashVal, ListVal, SetVal, ZSet};

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[test]
    fn large_collections_are_chunked_and_ttl_is_appended() {
        let mut l = ListVal::default();
        for i in 0..150 {
            l.push_back(b(&i.to_string()));
        }
        let cmds = commands_for(&DumpEntry {
            key: b("l"),
            value: Value::List(l),
            expire_at: Some(99),
        });
        assert_eq!(cmds.len(), 4, "3 RPUSH chunks + PEXPIREAT");
        assert_eq!(cmds[0].len(), 2 + 64);
        assert_eq!(cmds[3][0].as_ref(), b"PEXPIREAT");
    }

    #[test]
    fn every_type_produces_replayable_commands() {
        let mut h = HashVal::default();
        h.insert(b("f"), b("v"));
        let mut s = SetVal::default();
        s.insert(b("m"));
        let mut z = ZSet::new();
        z.insert(b("m"), 1.5);
        let names: Vec<_> = [
            Value::Str(b("x")),
            Value::Hash(h),
            Value::Set(s),
            Value::ZSet(z),
        ]
        .into_iter()
        .map(|v| {
            commands_for(&DumpEntry {
                key: b("k"),
                value: v,
                expire_at: None,
            })[0][0]
                .clone()
        })
        .collect();
        assert_eq!(names, vec![b("SET"), b("HSET"), b("SADD"), b("ZADD")]);
    }
}
