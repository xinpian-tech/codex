use std::fs::File;
use std::io;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::Path;

use serde::Deserialize;
use serde::Serialize;

use crate::JournalRecord;
use crate::journal::HEADER_LEN;
use crate::journal::MAGIC;

/// Points to the next record, scoped to the immutable prefix of one journal.
/// A consumer persists this cursor only after completing its downstream work.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalPosition {
    pub next_sequence: u64,
    pub byte_offset: u64,
}

/// Follows complete frames without acquiring the writer's exclusive lock.
/// This is a read-only view; only the writer repairs incomplete trailing data.
pub struct JournalReader {
    file: File,
    position: JournalPosition,
}

impl JournalReader {
    pub fn open(path: &Path, position: JournalPosition) -> io::Result<Self> {
        Ok(Self {
            file: File::open(path)?,
            position,
        })
    }

    pub fn position(&self) -> JournalPosition {
        self.position
    }

    /// `None` means no complete record is available yet. The cursor is unchanged
    /// on incomplete data or errors, so the same reader can poll again later.
    pub fn next_record(&mut self) -> io::Result<Option<JournalRecord>> {
        self.file.seek(SeekFrom::Start(self.position.byte_offset))?;
        let remaining = self
            .file
            .metadata()?
            .len()
            .checked_sub(self.position.byte_offset)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "journal shorter than reader cursor",
                )
            })?;
        if remaining < HEADER_LEN as u64 {
            return Ok(None);
        }
        let mut header = [0_u8; HEADER_LEN];
        self.file.read_exact(&mut header)?;
        let sequence = u64::from_le_bytes(header[4..12].try_into().map_err(io::Error::other)?);
        if &header[..4] != MAGIC || sequence != self.position.next_sequence {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "journal reader cursor does not match frame",
            ));
        }
        let length = u64::from_le_bytes(header[12..20].try_into().map_err(io::Error::other)?);
        if length > remaining - HEADER_LEN as u64 {
            return Ok(None);
        }
        let mut payload = Vec::new();
        payload
            .try_reserve_exact(usize::try_from(length).map_err(io::Error::other)?)
            .map_err(io::Error::other)?;
        Read::by_ref(&mut self.file)
            .take(length)
            .read_to_end(&mut payload)?;
        if payload.len() as u64 != length || blake3::hash(&payload).as_bytes() != &header[20..] {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "journal reader payload checksum",
            ));
        }
        let next_sequence = sequence
            .checked_add(1)
            .ok_or_else(|| io::Error::other("journal sequence exhausted"))?;
        let position = self.position;
        self.position = JournalPosition {
            next_sequence,
            byte_offset: self.position.byte_offset + HEADER_LEN as u64 + length,
        };
        Ok(Some(JournalRecord {
            sequence,
            position,
            payload,
        }))
    }
}
