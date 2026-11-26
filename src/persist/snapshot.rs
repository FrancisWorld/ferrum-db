//! Point-in-time binary snapshot (`SAVE` / `BGSAVE`).
//!
//! Layout: `FDBSNAP1`, then one record per key (`tag`, `has_ttl`, optional `u64`
//! expiry, length-prefixed key, type-specific payload), an end tag `0xFF` and a
//! CRC-32 of everything before it. The file is written to a temporary name, fsynced
//! and renamed, so a crash never leaves a half-written snapshot in place.

use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::Path;

use bytes::Bytes;

use super::PersistError;
use crate::db::{DumpEntry, HashVal, ListVal, SetVal, Value, ZSet};

const MAGIC: &[u8; 8] = b"FDBSNAP1";
const END: u8 = 0xFF;

struct CrcWriter<W: Write> {
    inner: W,
    crc: crc32fast::Hasher,
}

impl<W: Write> Write for CrcWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.crc.update(&buf[..n]);
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

fn put_bytes(w: &mut impl Write, b: &[u8]) -> io::Result<()> {
    let len = u32::try_from(b.len()).map_err(|_| io::Error::other("value larger than 4 GiB"))?;
    w.write_all(&len.to_le_bytes())?;
    w.write_all(b)
}

fn put_len(w: &mut impl Write, n: usize) -> io::Result<()> {
    w.write_all(
        &u32::try_from(n)
            .map_err(|_| io::Error::other("collection too large"))?
            .to_le_bytes(),
    )
}

fn write_entry(w: &mut impl Write, e: &DumpEntry) -> io::Result<()> {
    let tag = match &e.value {
        Value::Str(_) => 0u8,
        Value::List(_) => 1,
        Value::Hash(_) => 2,
        Value::Set(_) => 3,
        Value::ZSet(_) => 4,
    };
    w.write_all(&[tag, u8::from(e.expire_at.is_some())])?;
    if let Some(at) = e.expire_at {
        w.write_all(&at.to_le_bytes())?;
    }
    put_bytes(w, &e.key)?;
    match &e.value {
        Value::Str(b) => put_bytes(w, b),
        Value::List(l) => {
            put_len(w, l.len())?;
            l.items().iter().try_for_each(|b| put_bytes(w, b))
        }
        Value::Hash(h) => {
            put_len(w, h.len())?;
            h.iter()
                .try_for_each(|(f, v)| put_bytes(w, f).and_then(|()| put_bytes(w, v)))
        }
        Value::Set(s) => {
            put_len(w, s.len())?;
            s.iter().try_for_each(|m| put_bytes(w, m))
        }
        Value::ZSet(z) => {
            put_len(w, z.len())?;
            z.iter().try_for_each(|(m, s)| {
                w.write_all(&s.to_bits().to_le_bytes())
                    .and_then(|()| put_bytes(w, m))
            })
        }
    }
}

/// Writes `entries` to `path` atomically. Returns the file size.
pub fn write_snapshot(path: &Path, entries: &[DumpEntry]) -> io::Result<u64> {
    let tmp = path.with_extension("fdb.tmp");
    let file = File::create(&tmp)?;
    let mut w = CrcWriter {
        inner: BufWriter::with_capacity(1 << 20, file),
        crc: crc32fast::Hasher::new(),
    };
    w.write_all(MAGIC)?;
    for e in entries {
        write_entry(&mut w, e)?;
    }
    w.write_all(&[END])?;
    let crc = w.crc.clone().finalize();
    w.inner.write_all(&crc.to_le_bytes())?;
    let file = w
        .inner
        .into_inner()
        .map_err(io::IntoInnerError::into_error)?;
    file.sync_all()?;
    let size = file.metadata()?.len();
    drop(file);
    std::fs::rename(&tmp, path)?;
    super::sync_parent(path)?;
    Ok(size)
}

struct Cursor {
    data: Bytes,
    pos: usize,
}

impl Cursor {
    fn err(&self, reason: &str) -> PersistError {
        PersistError::Corrupt {
            offset: self.pos as u64,
            reason: reason.into(),
        }
    }

    fn take(&mut self, n: usize) -> Result<Bytes, PersistError> {
        if self.data.len() - self.pos < n {
            return Err(self.err("unexpected end of snapshot"));
        }
        let out = self.data.slice(self.pos..self.pos + n);
        self.pos += n;
        Ok(out)
    }

    fn u8(&mut self) -> Result<u8, PersistError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, PersistError> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn u64(&mut self) -> Result<u64, PersistError> {
        let b = self.take(8)?;
        Ok(u64::from_le_bytes(b.as_ref().try_into().unwrap_or([0; 8])))
    }

    fn blob(&mut self) -> Result<Bytes, PersistError> {
        let n = self.u32()? as usize;
        self.take(n)
    }

    /// A collection length, rejected when it cannot possibly fit in the remaining bytes.
    fn count(&mut self, min_item: usize) -> Result<usize, PersistError> {
        let n = self.u32()? as usize;
        if n.saturating_mul(min_item) > self.data.len() - self.pos {
            return Err(self.err("collection length exceeds file size"));
        }
        Ok(n)
    }
}

/// Reads and verifies a snapshot.
pub fn read_snapshot(path: &Path) -> Result<Vec<DumpEntry>, PersistError> {
    let data = Bytes::from(std::fs::read(path)?);
    parse_snapshot(data)
}

fn parse_snapshot(data: Bytes) -> Result<Vec<DumpEntry>, PersistError> {
    if data.len() < MAGIC.len() + 1 + 4 || &data[..MAGIC.len()] != MAGIC {
        return Err(PersistError::Corrupt {
            offset: 0,
            reason: "not a ferrum-db snapshot".into(),
        });
    }
    let body_len = data.len() - 4;
    let stored = u32::from_le_bytes([
        data[body_len],
        data[body_len + 1],
        data[body_len + 2],
        data[body_len + 3],
    ]);
    if crc32fast::hash(&data[..body_len]) != stored {
        return Err(PersistError::Corrupt {
            offset: body_len as u64,
            reason: "checksum mismatch".into(),
        });
    }
    let mut c = Cursor {
        data: data.slice(..body_len),
        pos: MAGIC.len(),
    };
    let mut out = Vec::new();
    loop {
        let tag = c.u8()?;
        if tag == END {
            return if c.pos == body_len {
                Ok(out)
            } else {
                Err(c.err("trailing bytes after end marker"))
            };
        }
        let expire_at = match c.u8()? {
            0 => None,
            1 => Some(c.u64()?),
            _ => return Err(c.err("invalid TTL flag")),
        };
        let key = c.blob()?;
        let value = match tag {
            0 => Value::Str(c.blob()?),
            1 => {
                let mut l = ListVal::default();
                for _ in 0..c.count(4)? {
                    l.push_back(c.blob()?);
                }
                Value::List(l)
            }
            2 => {
                let mut h = HashVal::default();
                for _ in 0..c.count(8)? {
                    let f = c.blob()?;
                    h.insert(f, c.blob()?);
                }
                Value::Hash(h)
            }
            3 => {
                let mut s = SetVal::default();
                for _ in 0..c.count(4)? {
                    s.insert(c.blob()?);
                }
                Value::Set(s)
            }
            4 => {
                let mut z = ZSet::new();
                for _ in 0..c.count(12)? {
                    let score = f64::from_bits(c.u64()?);
                    if score.is_nan() {
                        return Err(c.err("NaN score"));
                    }
                    z.insert(c.blob()?, score);
                }
                Value::ZSet(z)
            }
            _ => return Err(c.err("unknown value type")),
        };
        out.push(DumpEntry {
            key,
            value,
            expire_at,
        });
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    fn sample() -> Vec<DumpEntry> {
        let mut l = ListVal::default();
        l.push_back(b("a"));
        l.push_back(b("b"));
        let mut h = HashVal::default();
        h.insert(b("f"), b("v"));
        let mut s = SetVal::default();
        s.insert(b("m1"));
        s.insert(b("m2"));
        let mut z = ZSet::new();
        z.insert(b("x"), 1.5);
        z.insert(b("y"), -2.0);
        vec![
            DumpEntry {
                key: b("str"),
                value: Value::Str(b("hello")),
                expire_at: Some(123),
            },
            DumpEntry {
                key: b("list"),
                value: Value::List(l),
                expire_at: None,
            },
            DumpEntry {
                key: b("hash"),
                value: Value::Hash(h),
                expire_at: None,
            },
            DumpEntry {
                key: b("set"),
                value: Value::Set(s),
                expire_at: Some(9),
            },
            DumpEntry {
                key: b("zset"),
                value: Value::ZSet(z),
                expire_at: None,
            },
        ]
    }

    #[test]
    fn round_trips_every_type() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s.fdb");
        let entries = sample();
        write_snapshot(&path, &entries).unwrap();
        assert!(!path.with_extension("fdb.tmp").exists());
        let back = read_snapshot(&path).unwrap();
        assert_eq!(back.len(), entries.len());
        for (a, e) in back.iter().zip(&entries) {
            assert_eq!(a.key, e.key);
            assert_eq!(a.expire_at, e.expire_at);
            assert_eq!(a.value.type_name(), e.value.type_name());
            assert_eq!(a.value.mem_size(), e.value.mem_size());
        }
        let Value::ZSet(z) = &back[4].value else {
            panic!()
        };
        assert_eq!(z.score(b"y"), Some(-2.0));
        assert_eq!(z.rank(b"x", false), Some(1));
    }

    #[test]
    fn detects_corruption_and_truncation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s.fdb");
        write_snapshot(&path, &sample()).unwrap();
        let good = std::fs::read(&path).unwrap();

        let mut flipped = good.clone();
        flipped[20] ^= 0xFF;
        assert!(matches!(
            parse_snapshot(Bytes::from(flipped)),
            Err(PersistError::Corrupt { .. })
        ));
        assert!(parse_snapshot(Bytes::from(good[..good.len() - 3].to_vec())).is_err());
        assert!(parse_snapshot(Bytes::from_static(b"nonsense")).is_err());
    }

    fn with_crc(mut body: Vec<u8>) -> Bytes {
        let crc = crc32fast::hash(&body);
        body.extend_from_slice(&crc.to_le_bytes());
        Bytes::from(body)
    }

    #[test]
    fn hostile_lengths_are_rejected_without_allocating() {
        // A list claiming 4 billion elements inside a tiny file, with a valid checksum.
        let mut body = MAGIC.to_vec();
        body.extend_from_slice(&[1, 0]);
        body.extend_from_slice(&1u32.to_le_bytes());
        body.push(b'k');
        body.extend_from_slice(&u32::MAX.to_le_bytes());
        body.push(END);
        assert!(matches!(
            parse_snapshot(with_crc(body)),
            Err(PersistError::Corrupt { .. })
        ));
    }

    proptest! {
        #[test]
        fn arbitrary_bytes_never_panic(data in proptest::collection::vec(any::<u8>(), 0..256)) {
            let mut body = MAGIC.to_vec();
            body.extend_from_slice(&data);
            let _ = parse_snapshot(with_crc(body));
        }

        #[test]
        fn string_entries_round_trip(entries in proptest::collection::vec((any::<Vec<u8>>(), any::<Vec<u8>>(), proptest::option::of(any::<u64>())), 0..20)) {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("p.fdb");
            let dump: Vec<_> = entries.iter().map(|(k, v, t)| DumpEntry { key: Bytes::from(k.clone()), value: Value::Str(Bytes::from(v.clone())), expire_at: *t }).collect();
            write_snapshot(&path, &dump).unwrap();
            let back = read_snapshot(&path).unwrap();
            prop_assert_eq!(back.len(), dump.len());
            for (a, e) in back.iter().zip(&dump) {
                prop_assert_eq!(&a.key, &e.key);
                prop_assert_eq!(a.expire_at, e.expire_at);
            }
        }
    }
}
