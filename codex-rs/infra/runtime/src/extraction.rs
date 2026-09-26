use std::collections::BTreeMap;
use std::io;
use std::path::Path;

use codex_infra_state::JournalPosition;
use codex_infra_state::JournalReader;
use codex_infra_tmux::ControlDecoder;
use codex_infra_tmux::ControlRecord;
use codex_infra_tmux::FrameDecoder;
use codex_infra_tmux::PaneOutput;
use codex_infra_tmux::TransportFrame;
use serde::Deserialize;
use serde::Serialize;

/// A cursor belongs to one collector attachment. Persist it after enqueueing the
/// extracted batch durably; replay after an interrupted handoff keeps message IDs.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ControlCursor {
    position: JournalPosition,
    control: ControlDecoder,
    panes: BTreeMap<String, FrameDecoder>,
}

impl ControlCursor {
    pub fn next_source_sequence(&self) -> u64 {
        self.position.next_sequence
    }
}

#[derive(Debug)]
pub enum ObservedControl {
    PaneOutput {
        output: PaneOutput,
        frames: Vec<TransportFrame>,
    },
    Notification(Vec<u8>),
}

#[derive(Debug)]
pub struct ControlBatch {
    pub source_sequence: u64,
    pub records: Vec<ObservedControl>,
}

/// Consumes collector journals independently of the capture threads and TCP
/// connections. Partial control lines and per-pane frames survive cursor resume.
pub struct ControlFrameReader {
    source: JournalReader,
    cursor: ControlCursor,
    failed: bool,
}

impl ControlFrameReader {
    pub fn open(stdout_journal: &Path, cursor: ControlCursor) -> io::Result<Self> {
        let source = JournalReader::open(stdout_journal, cursor.position)?;
        Ok(Self {
            source,
            cursor,
            failed: false,
        })
    }

    /// A caller processes one captured byte block at a time, so continuous logs
    /// cannot monopolize its loop waiting for a semantic message to appear.
    pub fn next_batch(&mut self) -> io::Result<Option<ControlBatch>> {
        if self.failed {
            return Err(io::Error::other(
                "reopen extraction from its saved cursor after decode failure",
            ));
        }
        let Some(record) = self.source.next_record()? else {
            return Ok(None);
        };
        self.failed = true;
        let mut records = Vec::new();
        let ControlCursor { control, panes, .. } = &mut self.cursor;
        control.feed(&record.payload, |record| {
            match record {
                ControlRecord::PaneOutput(output) => {
                    let mut frames = Vec::new();
                    panes.entry(output.pane_id.clone()).or_default().feed(
                        &output.bytes,
                        |frame| {
                            frames.push(frame);
                            Ok(())
                        },
                    )?;
                    records.push(ObservedControl::PaneOutput { output, frames });
                }
                ControlRecord::Notification(line) => {
                    records.push(ObservedControl::Notification(line))
                }
            }
            Ok(())
        })?;
        self.cursor.position = self.source.position();
        self.failed = false;
        Ok(Some(ControlBatch {
            source_sequence: record.sequence,
            records,
        }))
    }

    pub fn cursor(&self) -> io::Result<ControlCursor> {
        if self.failed {
            return Err(io::Error::other(
                "decode failure left an incomplete extraction cursor",
            ));
        }
        Ok(self.cursor.clone())
    }

    /// The runtime calls this after observing the pane's final output and exit.
    /// The original control bytes remain in the collector journal for audit.
    pub fn forget_exited_pane(&mut self, pane_id: &str) {
        self.cursor.panes.remove(pane_id);
    }
}
