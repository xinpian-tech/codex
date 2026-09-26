use std::io;
use std::num::NonZeroUsize;

use codex_infra_protocol::CommitId;

use crate::ArchiveReceipt;
use crate::ArchiveStream;
use crate::JournalPosition;
use crate::JournalSegment;

use super::SessionShard;
use super::catalog::stream_key;

/// The most recently published segment, not a producer completion record.
/// `observed_durable` can extend beyond the bytes contained in this segment.
pub struct ArchivedStreamHead {
    pub stream: ArchiveStream,
    pub start: u64,
    pub end: u64,
    pub observed_durable: JournalPosition,
    pub receipt: ArchiveReceipt,
}

pub struct StreamPage {
    pub data: Vec<ArchivedStreamHead>,
    pub next_cursor: Option<String>,
}

impl SessionShard {
    pub(crate) fn publish_completion(
        &mut self,
        completion: &crate::StreamCompletion,
    ) -> io::Result<ArchiveReceipt> {
        if completion.archive.session_ref != self.session_ref {
            return Err(io::Error::other(
                "stream completion belongs to another shard",
            ));
        }
        let name = format!(
            "stream-completions/{}.json",
            stream_key(&completion.stream)?
        );
        let bytes = serde_json::to_vec(completion).map_err(io::Error::other)?;
        self.publish_named(&bytes, &[name])
    }

    /// Reads a final producer boundary at an explicit archive revision.
    /// Active streams have no completion entry. Restore the linked prefix to
    /// validate its complete journal frames before resuming from that boundary.
    pub fn read_stream_completion(
        &self,
        revision: &CommitId,
        stream: &ArchiveStream,
    ) -> io::Result<crate::StreamCompletion> {
        let name = format!("stream-completions/{}.json", stream_key(stream)?);
        let object = format!("{revision}:{name}");
        let body = self.run(["show", &object])?;
        let completion: crate::StreamCompletion =
            serde_json::from_str(&body).map_err(io::Error::other)?;
        if &completion.stream != stream || completion.archive.session_ref != self.session_ref {
            return Err(io::Error::other("stream completion identity changed"));
        }
        let segment = self.read_segment(&completion.archive)?;
        if segment.stream != completion.stream
            || segment.end != completion.position.byte_offset
            || segment.durable != completion.position
        {
            return Err(io::Error::other(
                "stream completion differs from final segment",
            ));
        }
        Ok(completion)
    }

    /// Discovers producer identities from the archive itself. Read one segment
    /// body at a time, retaining only metadata for the requested page. Keep the
    /// same revision across pages; the cursor is a path at that revision.
    pub fn list_streams(
        &self,
        revision: &CommitId,
        after: Option<&str>,
        limit: NonZeroUsize,
    ) -> io::Result<StreamPage> {
        let page = self.catalog_names(revision, "stream-heads/", after, limit)?;
        let mut data = Vec::new();
        for name in page.data {
            let object = format!("{revision}:{name}");
            let body = self.run(["show", &object])?;
            let segment: JournalSegment = serde_json::from_str(&body).map_err(io::Error::other)?;
            let expected_name = format!("stream-heads/{}.bin", stream_key(&segment.stream)?);
            let expected_ref = format!(
                "refs/codex/session-shards/{}/{}",
                segment.stream.root_session_id, segment.stream.machine_id
            );
            if name != expected_name
                || expected_ref != self.session_ref
                || segment.end.checked_sub(segment.start) != Some(segment.bytes.len() as u64)
                || segment.end > segment.durable.byte_offset
            {
                return Err(io::Error::other(
                    "stream catalog entry differs from its segment",
                ));
            }
            data.push(ArchivedStreamHead {
                stream: segment.stream,
                start: segment.start,
                end: segment.end,
                observed_durable: segment.durable,
                receipt: ArchiveReceipt {
                    session_ref: self.session_ref.clone(),
                    commit: revision.clone(),
                    segment_name: format!("segments/{}.bin", blake3::hash(body.as_bytes())),
                },
            });
        }
        Ok(StreamPage {
            data,
            next_cursor: page.next_cursor,
        })
    }
}
