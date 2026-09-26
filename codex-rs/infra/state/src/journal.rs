use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;

const HEADER_LEN: usize = 52;
const MAGIC: &[u8; 4] = b"CXJ1";

/// A record's sequence is its stable offset in the machine writer's stream.
#[derive(Debug)]
pub struct JournalRecord {
    pub sequence: u64,
    pub payload: Vec<u8>,
}

/// Holds an exclusive file lock for its lifetime. Callers serialize append/replay
/// through this writer and publish acknowledged records to Git independently.
pub struct Journal {
    file: File,
    next_sequence: u64,
    end: u64,
    append_failed: bool,
}

impl Journal {
    /// Opens an existing parent directory's journal and replays complete records.
    /// A partially written final record is discarded before appends resume.
    pub fn open(
        path: &Path,
        mut replay: impl FnMut(JournalRecord) -> io::Result<()>,
    ) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        file.lock()?;
        let length = file.metadata()?.len();
        let mut end = 0;
        let mut next_sequence = 0_u64;
        while end < length {
            if length - end < HEADER_LEN as u64 {
                break;
            }
            let mut header = [0_u8; HEADER_LEN];
            file.read_exact(&mut header)?;
            if &header[..4] != MAGIC {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "journal frame magic",
                ));
            }
            let sequence = u64::from_le_bytes(header[4..12].try_into().map_err(io::Error::other)?);
            let payload_len =
                u64::from_le_bytes(header[12..20].try_into().map_err(io::Error::other)?);
            if sequence != next_sequence {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "journal sequence",
                ));
            }
            if payload_len > length - end - HEADER_LEN as u64 {
                break;
            }
            let mut payload = Vec::new();
            payload
                .try_reserve_exact(usize::try_from(payload_len).map_err(io::Error::other)?)
                .map_err(io::Error::other)?;
            Read::by_ref(&mut file)
                .take(payload_len)
                .read_to_end(&mut payload)?;
            if payload.len() as u64 != payload_len
                || blake3::hash(&payload).as_bytes() != &header[20..]
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "journal payload checksum",
                ));
            }
            replay(JournalRecord { sequence, payload })?;
            end += HEADER_LEN as u64 + payload_len;
            next_sequence = next_sequence
                .checked_add(1)
                .ok_or_else(|| io::Error::other("journal sequence exhausted"))?;
        }
        if end != length {
            file.set_len(end)?;
        }
        file.sync_all()?;
        // Persist a newly created directory entry before acknowledging any record.
        #[cfg(unix)]
        if let Some(parent) = path.parent() {
            let parent = if parent.as_os_str().is_empty() {
                Path::new(".")
            } else {
                parent
            };
            File::open(parent)?.sync_all()?;
        }
        file.seek(SeekFrom::Start(end))?;
        Ok(Self {
            file,
            next_sequence,
            end,
            append_failed: false,
        })
    }

    /// Returns the sequence only after the complete record is durable locally.
    pub fn append(&mut self, payload: &[u8]) -> io::Result<u64> {
        if self.append_failed {
            return Err(io::Error::other(
                "reopen journal after an incomplete append",
            ));
        }
        let following = self
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| io::Error::other("journal sequence exhausted"))?;
        let end = self
            .end
            .checked_add(HEADER_LEN as u64)
            .and_then(|end| end.checked_add(payload.len() as u64))
            .ok_or_else(|| io::Error::other("journal length exhausted"))?;
        let mut header = [0_u8; HEADER_LEN];
        header[..4].copy_from_slice(MAGIC);
        header[4..12].copy_from_slice(&self.next_sequence.to_le_bytes());
        header[12..20].copy_from_slice(&(payload.len() as u64).to_le_bytes());
        header[20..].copy_from_slice(blake3::hash(payload).as_bytes());
        self.append_failed = true;
        self.file.write_all(&header)?;
        self.file.write_all(payload)?;
        self.file.sync_all()?;
        let sequence = self.next_sequence;
        self.next_sequence = following;
        self.end = end;
        self.append_failed = false;
        Ok(sequence)
    }

    pub fn next_sequence(&self) -> u64 {
        self.next_sequence
    }
}
