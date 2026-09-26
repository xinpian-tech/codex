use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::num::NonZeroUsize;
use std::process::Stdio;

use codex_infra_protocol::CommitId;

use crate::ArchiveReceipt;
use crate::ArchiveStream;
use crate::JournalSegment;

use super::SessionShard;

/// Cursor is a catalog path within the requested immutable commit and stream.
pub struct SegmentPage {
    pub data: Vec<ArchiveReceipt>,
    pub next_cursor: Option<String>,
}

impl SessionShard {
    /// Adds the stream/range catalog entry in the same Git tree as the blob.
    /// Both paths reference one object; the catalog does not duplicate bytes.
    pub fn publish_journal_segment(
        &mut self,
        segment: &JournalSegment,
    ) -> io::Result<ArchiveReceipt> {
        let prefix = stream_prefix(&segment.stream)?;
        let expected_ref = format!(
            "refs/codex/session-shards/{}/{}",
            segment.stream.root_session_id, segment.stream.machine_id
        );
        if expected_ref != self.session_ref {
            return Err(io::Error::other(
                "journal segment belongs to another machine shard",
            ));
        }
        let bytes = serde_json::to_vec(segment).map_err(io::Error::other)?;
        let name = format!(
            "{prefix}{:020}-{:020}/{}.bin",
            segment.start,
            segment.end,
            blake3::hash(&bytes)
        );
        self.publish_named(&bytes, &[name])
    }

    /// Enumerates ranges by start/end at an immutable revision. Memory holds
    /// only one page of names, never all archived bodies or the whole catalog.
    /// Receipts use the canonical content-addressed path for read_segment.
    pub fn list_segments(
        &self,
        commit: &CommitId,
        stream: &ArchiveStream,
        after: Option<&str>,
        limit: NonZeroUsize,
    ) -> io::Result<SegmentPage> {
        let prefix = stream_prefix(stream)?;
        let expected_ref = format!(
            "refs/codex/session-shards/{}/{}",
            stream.root_session_id, stream.machine_id
        );
        if expected_ref != self.session_ref
            || after.is_some_and(|cursor| !cursor.starts_with(&prefix))
        {
            return Err(io::Error::other("segment catalog binding changed"));
        }
        let revision = commit.to_string();
        let mut child = self
            .command([
                "ls-tree",
                "-r",
                "--name-only",
                "-z",
                &revision,
                "--",
                &prefix,
            ])
            .stdout(Stdio::piped())
            .spawn()?;
        let result = (|| {
            let stdout = child
                .stdout
                .take()
                .ok_or_else(|| io::Error::other("Git catalog stdout missing"))?;
            let mut reader = BufReader::new(stdout);
            let mut data = Vec::new();
            let mut last = None;
            let mut more = false;
            let mut bytes = Vec::new();
            loop {
                bytes.clear();
                if reader.read_until(0, &mut bytes)? == 0 {
                    break;
                }
                if bytes.pop() != Some(0) {
                    return Err(io::Error::other("Git catalog path is incomplete"));
                }
                let name = std::str::from_utf8(&bytes).map_err(io::Error::other)?;
                if !name.starts_with(&prefix) {
                    return Err(io::Error::other("Git returned a different stream catalog"));
                }
                if after.is_some_and(|cursor| name <= cursor) {
                    continue;
                }
                if data.len() == limit.get() {
                    more = true;
                    continue;
                }
                let filename = name
                    .rsplit('/')
                    .next()
                    .ok_or_else(|| io::Error::other("segment filename missing"))?;
                data.push(ArchiveReceipt {
                    session_ref: self.session_ref.clone(),
                    commit: commit.clone(),
                    segment_name: format!("segments/{filename}"),
                });
                last = Some(name.to_owned());
            }
            Ok(SegmentPage {
                data,
                next_cursor: if more { last } else { None },
            })
        })();
        let status = child.wait()?;
        if !status.success() {
            return Err(io::Error::other(format!("Git catalog exited {status}")));
        }
        result
    }
}

fn stream_prefix(stream: &ArchiveStream) -> io::Result<String> {
    let identity = serde_json::to_vec(stream).map_err(io::Error::other)?;
    Ok(format!("streams/{}/", blake3::hash(&identity)))
}
