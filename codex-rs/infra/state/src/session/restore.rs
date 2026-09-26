use std::io;
use std::num::NonZeroUsize;
use std::path::Path;

use codex_infra_protocol::CommitId;

use crate::ArchiveStream;
use crate::JournalPosition;
use crate::JournalRestore;

use super::SessionShard;

impl SessionShard {
    /// Fetches into a read cache ref, preserving the machine writer's branch.
    /// An older receipt revision remains valid when it is an ancestor of the
    /// fetched archive; restoration always reads that explicit revision.
    pub fn fetch_archive_revision(&mut self, revision: &CommitId) -> io::Result<()> {
        let suffix = self
            .session_ref
            .strip_prefix("refs/codex/session-shards/")
            .ok_or_else(|| io::Error::other("Session shard ref is not recognized"))?;
        let cache = format!("refs/codex/session-read-cache/{suffix}");
        let refspec = format!("{}:{cache}", self.session_ref);
        self.run([
            "fetch",
            "--no-write-fetch-head",
            "--",
            &self.remote,
            &refspec,
        ])?;
        let revision = revision.to_string();
        self.run(["merge-base", "--is-ancestor", &revision, &cache])?;
        Ok(())
    }

    /// Restores one producer boundary from fetched objects. Pages and bodies
    /// are read at the same immutable commit. Existing partial output is
    /// verified from offset zero; source segments remain unchanged when the
    /// requested boundary lies inside a later archived segment.
    pub fn restore_journal(
        &self,
        revision: &CommitId,
        stream: ArchiveStream,
        required: JournalPosition,
        destination: &Path,
        page_size: NonZeroUsize,
    ) -> io::Result<JournalPosition> {
        let mut restore = JournalRestore::open(destination, stream.clone())?;
        let mut cursor = None;
        'pages: loop {
            let page = self.list_segments(revision, &stream, cursor.as_deref(), page_size)?;
            for receipt in page.data {
                let mut segment = self.read_segment(&receipt)?;
                if segment.stream != stream
                    || segment.end.checked_sub(segment.start) != Some(segment.bytes.len() as u64)
                    || segment.end > segment.durable.byte_offset
                {
                    return Err(io::Error::other(
                        "catalog segment differs from requested stream or range",
                    ));
                }
                if segment.start > required.byte_offset
                    || (segment.start == required.byte_offset && segment.start != 0)
                {
                    break 'pages;
                }
                if segment.end > required.byte_offset {
                    let length = usize::try_from(required.byte_offset - segment.start)
                        .map_err(io::Error::other)?;
                    segment.bytes.truncate(length);
                    segment.end = required.byte_offset;
                }
                restore.apply(&segment)?;
                if required == JournalPosition::default() {
                    break 'pages;
                }
            }
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        restore.finish(required)
    }
}
