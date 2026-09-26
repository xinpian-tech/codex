use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use crate::ArchiveStream;
use crate::JournalPosition;
use crate::JournalReader;
use crate::JournalSegment;

/// Reconstructs one stream into a host-local journal. Reopening a partial
/// restoration replays segments from offset zero to verify existing bytes.
/// The file remains exclusively owned until finish verifies every frame.
pub struct JournalRestore {
    stream: ArchiveStream,
    path: PathBuf,
    file: File,
    verified: Option<u64>,
}

impl JournalRestore {
    pub fn open(path: &Path, stream: ArchiveStream) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        file.lock()?;
        file.sync_all()?;
        #[cfg(unix)]
        if let Some(parent) = path.parent() {
            File::open(parent)?.sync_all()?;
        }
        Ok(Self {
            stream,
            path: path.canonicalize()?,
            file,
            verified: None,
        })
    }

    /// Accepts adjacent or overlapping immutable ranges. Gaps wait for their
    /// missing segments; an overlap must contain byte-for-byte identical data.
    pub fn apply(&mut self, segment: &JournalSegment) -> io::Result<()> {
        let length = segment
            .end
            .checked_sub(segment.start)
            .ok_or_else(|| io::Error::other("archive segment range is reversed"))?;
        if segment.stream != self.stream
            || length != segment.bytes.len() as u64
            || segment.end > segment.durable.byte_offset
            || segment.start > self.verified.unwrap_or(0)
            || (length == 0
                && (segment.start != 0 || segment.durable != JournalPosition::default()))
        {
            return Err(io::Error::other(
                "archive segment does not extend this stream",
            ));
        }
        let existing = self.file.metadata()?.len();
        if existing < segment.start {
            return Err(io::Error::other(
                "restored journal lost its verified prefix",
            ));
        }
        let overlap =
            usize::try_from((existing - segment.start).min(length)).map_err(io::Error::other)?;
        self.file.seek(SeekFrom::Start(segment.start))?;
        let mut buffer = [0_u8; 8192];
        let mut offset = 0;
        while offset < overlap {
            let count = (overlap - offset).min(buffer.len());
            self.file.read_exact(&mut buffer[..count])?;
            if buffer[..count] != segment.bytes[offset..offset + count] {
                return Err(io::Error::other(
                    "archive segments disagree on restored bytes",
                ));
            }
            offset += count;
        }
        self.file.write_all(&segment.bytes[overlap..])?;
        self.file.sync_all()?;
        self.verified = Some(self.verified.unwrap_or(0).max(segment.end));
        Ok(())
    }

    /// Requires the exact producer completion boundary, including sequence and
    /// byte offset. Partial trailing records and extra unverified bytes remain
    /// unfinished; this operation never repairs or truncates archive evidence.
    pub fn finish(self, required: JournalPosition) -> io::Result<JournalPosition> {
        if self.verified != Some(required.byte_offset)
            || self.file.metadata()?.len() != required.byte_offset
        {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "journal restoration is incomplete",
            ));
        }
        let mut reader = JournalReader::open(&self.path, JournalPosition::default())?;
        while reader.next_record()?.is_some() {}
        if reader.position() != required {
            return Err(io::Error::other(
                "restored frames differ from producer completion position",
            ));
        }
        self.file.sync_all()?;
        Ok(required)
    }
}
