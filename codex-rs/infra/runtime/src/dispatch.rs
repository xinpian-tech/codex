use std::io;
use std::num::NonZeroUsize;
use std::path::Path;

use codex_infra_protocol::MessageId;
use codex_infra_state::Journal;
use codex_infra_state::QueueItem;
use codex_infra_state::SpoolQueue;
use codex_infra_tmux::TransportFrame;
use serde::Deserialize;
use serde::Serialize;

use crate::ControlBatch;
use crate::ControlCursor;
use crate::ObservedControl;

#[derive(Debug, Serialize, Deserialize)]
pub struct CapturedFrame {
    pub pane_id: String,
    pub frame: TransportFrame,
}

#[derive(Serialize, Deserialize)]
struct ExtractionCheckpoint {
    attachment_id: MessageId,
    cursor: ControlCursor,
}

/// One dispatch spool per collector attachment. Staging commits every frame
/// before advancing its extraction cursor. Dispatch retries use stable source
/// positions; the receiving host still owns semantic message deduplication.
pub struct ControlDispatch {
    attachment_id: MessageId,
    queue: SpoolQueue,
    checkpoints: Journal,
    cursor: ControlCursor,
}

impl ControlDispatch {
    pub fn open(
        queue_path: &Path,
        checkpoint_path: &Path,
        attachment_id: MessageId,
    ) -> io::Result<Self> {
        let queue = SpoolQueue::open(queue_path)?;
        let mut cursor = ControlCursor::default();
        let checkpoints = Journal::open(checkpoint_path, |record| {
            let checkpoint: ExtractionCheckpoint =
                serde_json::from_slice(&record.payload).map_err(io::Error::other)?;
            if checkpoint.attachment_id != attachment_id
                || cursor.next_source_sequence().checked_add(1)
                    != Some(checkpoint.cursor.next_source_sequence())
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "extraction checkpoint does not continue this attachment",
                ));
            }
            cursor = checkpoint.cursor;
            Ok(())
        })?;
        Ok(Self {
            attachment_id,
            queue,
            checkpoints,
            cursor,
        })
    }

    pub fn cursor(&self) -> ControlCursor {
        self.cursor.clone()
    }

    pub fn stage(&mut self, batch: ControlBatch, cursor: ControlCursor) -> io::Result<()> {
        if batch.source_sequence != self.cursor.next_source_sequence()
            || batch.source_sequence.checked_add(1) != Some(cursor.next_source_sequence())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "dispatch batch and extraction cursor differ",
            ));
        }
        for (record_index, record) in batch.records.into_iter().enumerate() {
            match record {
                ObservedControl::PaneOutput { output, frames } => {
                    for (frame_index, frame) in frames.into_iter().enumerate() {
                        let lane = match &frame {
                            TransportFrame::Chunk(chunk) => chunk.route.to_agent_id.to_string(),
                            TransportFrame::Receipt(receipt) => {
                                receipt.route.from_agent_id.to_string()
                            }
                            TransportFrame::Ready(ready) => format!("ready:{}", ready.agent_id),
                        };
                        let key = format!(
                            "{}:{}:{record_index}:{frame_index}",
                            self.attachment_id, batch.source_sequence
                        );
                        let captured = CapturedFrame {
                            pane_id: output.pane_id.clone(),
                            frame,
                        };
                        self.queue.enqueue(QueueItem {
                            key,
                            lane,
                            payload: serde_json::to_vec(&captured).map_err(io::Error::other)?,
                        })?;
                    }
                }
                ObservedControl::Notification(_) => {}
            }
        }
        let checkpoint = ExtractionCheckpoint {
            attachment_id: self.attachment_id,
            cursor,
        };
        self.checkpoints
            .append(&serde_json::to_vec(&checkpoint).map_err(io::Error::other)?)?;
        self.cursor = checkpoint.cursor;
        Ok(())
    }

    pub fn lanes(&self) -> impl Iterator<Item = &str> {
        self.queue.lanes()
    }

    pub fn next_frame(&self, lane: &str) -> io::Result<Option<(String, CapturedFrame)>> {
        let keys = self
            .queue
            .pending_keys(lane, /*first_sequence*/ 0, NonZeroUsize::MIN);
        let Some(key) = keys.first() else {
            return Ok(None);
        };
        let item = self.queue.read(key)?;
        let frame = serde_json::from_slice(&item.payload).map_err(io::Error::other)?;
        Ok(Some((item.key, frame)))
    }

    pub fn complete(&mut self, key: &str) -> io::Result<()> {
        self.queue.complete(key)
    }
}
