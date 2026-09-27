use std::io;
use std::num::NonZeroUsize;
use std::path::Path;

use codex_infra_protocol::CommitId;
use codex_infra_state::ArchiveProducer;
use codex_infra_state::ArchiveStream;
use codex_infra_state::JournalPosition;
use codex_infra_state::SessionShard;

use crate::AccountDirectorySource;

/// A complete published journal prefix, discovered at one immutable Git commit.
/// Active archives can expose a partial segment; those are left for a later pass.
pub struct PublishedAccountDirectory {
    stream: ArchiveStream,
    revision: CommitId,
    position: JournalPosition,
}

pub struct AccountDirectoryPage {
    pub data: Vec<PublishedAccountDirectory>,
    pub next_cursor: Option<String>,
}

impl AccountDirectoryPage {
    /// Use SessionShard::fetch_archive_head once, then keep that revision across
    /// pages. An empty filtered page can still have a next_cursor.
    pub fn read(
        shard: &SessionShard,
        revision: &CommitId,
        after: Option<&str>,
        limit: NonZeroUsize,
    ) -> io::Result<Self> {
        let page = shard.list_streams(revision, after, limit)?;
        let data = page
            .data
            .into_iter()
            .filter_map(|head| {
                if head.stream.name == "account-directory"
                    && matches!(head.stream.producer, ArchiveProducer::Machine { .. })
                    && head.end == head.observed_durable.byte_offset
                {
                    Some(PublishedAccountDirectory {
                        stream: head.stream,
                        revision: revision.clone(),
                        position: head.observed_durable,
                    })
                } else {
                    None
                }
            })
            .collect();
        Ok(Self {
            data,
            next_cursor: page.next_cursor,
        })
    }
}

impl PublishedAccountDirectory {
    pub fn stream(&self) -> &ArchiveStream {
        &self.stream
    }

    pub fn position(&self) -> JournalPosition {
        self.position
    }

    pub fn revision(&self) -> &CommitId {
        &self.revision
    }

    /// Blocking restoration reuses the archive's content-address verification
    /// and journal-frame checks. The caller creates the destination parent.
    /// After success, pass position() to the follower as its confirmed boundary.
    pub fn restore(
        &self,
        shard: &SessionShard,
        destination: &Path,
        page_size: NonZeroUsize,
    ) -> io::Result<AccountDirectorySource> {
        shard.restore_journal(
            &self.revision,
            self.stream.clone(),
            self.position,
            destination,
            page_size,
        )?;
        Ok(AccountDirectorySource {
            stream: self.stream.clone(),
            path: destination.canonicalize()?,
        })
    }
}
