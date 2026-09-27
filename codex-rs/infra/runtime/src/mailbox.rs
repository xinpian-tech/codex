use std::fs;
use std::io;
use std::io::BufWriter;
use std::io::Write;
use std::num::NonZeroUsize;
use std::path::Path;
use std::path::PathBuf;

use codex_infra_protocol::AgentId;
use codex_infra_protocol::AgentMessage;
use codex_infra_protocol::DeliveryReceipt;
use codex_infra_protocol::DeliveryStage;
use codex_infra_protocol::FrameRoute;
use codex_infra_protocol::MessageId;
use codex_infra_protocol::RootSessionId;
use codex_infra_state::Checkpoint;
use codex_infra_state::DurableInbox;
use codex_infra_state::DurableOutbox;
use codex_infra_state::InboxEntry;
use codex_infra_state::Journal;
use codex_infra_state::OutboxEntry;
use codex_infra_state::PresentedInput;
use codex_infra_state::SpoolQueue;
use codex_infra_tmux::FrameDecoder;
use codex_infra_tmux::HostReady;
use codex_infra_tmux::MessageAssembler;
use codex_infra_tmux::TransportFrame;
use codex_infra_tmux::write_message;

use crate::recorded_writer::RecordedWriter;

mod archive;
mod bootstrap;
mod publication;
pub use archive::MailboxArchiveJobIds;
pub use archive::MailboxArchiveJobs;
pub use archive::MailboxArchiveReceipts;
pub use bootstrap::BootstrapSelection;

/// Durable prefixes sampled together under the host's mailbox ownership.
/// Positions describe recorded bytes/events, not remote delivery or processing.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MailboxPositions {
    pub stdin: codex_infra_state::JournalPosition,
    pub stdout: codex_infra_state::JournalPosition,
    pub inbox: codex_infra_state::JournalPosition,
    pub outbox: codex_infra_state::JournalPosition,
    pub publications: codex_infra_state::JournalPosition,
    #[serde(default)]
    pub bootstrap: codex_infra_state::JournalPosition,
}

/// Owned by one Agent host. `stdout` is its terminal writer; `receive_stdin`
/// consumes only bytes read from that host's terminal stdin. Methods are
/// serialized by the host, including presentation at a Codex input boundary.
pub struct HostMailbox<W: Write> {
    directory: PathBuf,
    root_session_id: RootSessionId,
    agent_id: AgentId,
    inbox: DurableInbox,
    outbox: DurableOutbox,
    publications: SpoolQueue,
    bootstrap: Journal,
    bootstrap_binding: Option<bootstrap::BootstrapBinding>,
    assembler: MessageAssembler,
    decoder: FrameDecoder,
    input: Journal,
    output: BufWriter<RecordedWriter<W>>,
}

impl<W: Write> HostMailbox<W> {
    /// Flushes terminal output before sampling its journal. The finalizer stops
    /// mailbox producers before using these as completion positions; snapshots
    /// taken during operation are only incremental archive watermarks.
    pub fn flush_positions(&mut self) -> io::Result<MailboxPositions> {
        self.output.flush()?;
        Ok(MailboxPositions {
            stdin: self.input.position(),
            stdout: self.output.get_ref().journal.position(),
            inbox: self.inbox.position(),
            outbox: self.outbox.position(),
            publications: self.publications.position(),
            bootstrap: self.bootstrap.position(),
        })
    }

    pub fn open(
        directory: &Path,
        root_session_id: RootSessionId,
        agent_id: AgentId,
        stdout: W,
    ) -> io::Result<Self> {
        fs::create_dir_all(directory)?;
        let inbox =
            DurableInbox::open(&directory.join("inbox.journal"), root_session_id, agent_id)?;
        let outbox =
            DurableOutbox::open(&directory.join("outbox.journal"), root_session_id, agent_id)?;
        let publications = SpoolQueue::open(&directory.join("publications.journal"))?;
        let mut bootstrap_binding = None;
        let bootstrap = Journal::open(&directory.join("bootstrap.journal"), |record| {
            let binding: bootstrap::BootstrapBinding = serde_json::from_slice(&record.payload)?;
            if bootstrap_binding
                .as_ref()
                .is_some_and(|previous| previous != &binding)
            {
                return Err(io::Error::other("mailbox bootstrap binding changed"));
            }
            bootstrap_binding = Some(binding);
            Ok(())
        })?;
        let assembler =
            MessageAssembler::open(directory.join("incoming"), root_session_id, agent_id)?;
        let input = Journal::open(&directory.join("stdin.journal"), |_| Ok(()))?;
        let journal = Journal::open(&directory.join("stdout.journal"), |_| Ok(()))?;
        Ok(Self {
            directory: directory.canonicalize()?,
            root_session_id,
            agent_id,
            inbox,
            outbox,
            publications,
            bootstrap,
            bootstrap_binding,
            assembler,
            decoder: FrameDecoder::default(),
            input,
            output: BufWriter::new(RecordedWriter {
                journal,
                writer: stdout,
                failed: false,
            }),
        })
    }

    /// The caller serializes workspace mutations/checkpoints with this send.
    /// Retries of already queued messages use `replay_pending`, retaining the
    /// original checkpoint identity even after the worktree advances.
    pub fn send_checkpointed(
        &mut self,
        mut message: AgentMessage,
        checkpoint: &Checkpoint,
    ) -> io::Result<u64> {
        message.commit = checkpoint.pushed_commit.clone();
        let sequence = self.outbox.enqueue(message.clone())?;
        write_message(&mut self.output, &message)?;
        Ok(sequence)
    }

    /// Returns the next pagination position after emitting a bounded batch.
    /// The runtime retries from the beginning after reconnect; accepted IDs are
    /// removed from this view only when their receipts are persisted locally.
    pub fn replay_pending(
        &mut self,
        first_sequence: u64,
        limit: NonZeroUsize,
    ) -> io::Result<Option<u64>> {
        let mut next = None;
        for entry in self.outbox.pending_from(first_sequence, limit) {
            write_message(&mut self.output, &entry.message)?;
            next = entry.queued_sequence.checked_add(1);
        }
        Ok(next)
    }

    pub fn outbound(&self, message_id: MessageId) -> Option<&OutboxEntry> {
        self.outbox.get(message_id)
    }

    /// Includes presented and processed entries when recovering a bound input.
    pub fn input(&self, message_id: MessageId) -> Option<&InboxEntry> {
        self.inbox.get(message_id)
    }

    /// Called after the host has entered raw mode and opened its durable state.
    /// The machine collector observes this through the host's own tmux output.
    pub fn announce_ready(&mut self, launch_id: MessageId) -> io::Result<()> {
        TransportFrame::Ready(HostReady {
            root_session_id: self.root_session_id,
            agent_id: self.agent_id,
            launch_id,
        })
        .write(&mut self.output)
    }

    /// Re-emits the original envelope when waiting for a presentation receipt
    /// that may have been lost after acceptance. The inbox returns its existing
    /// receipts without submitting the message to the model a second time.
    pub fn replay_message(&mut self, message_id: MessageId) -> io::Result<()> {
        let entry = self
            .outbox
            .get(message_id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "outbound message missing"))?;
        write_message(&mut self.output, &entry.message)
    }

    pub fn pending_inputs(
        &self,
        first_sequence: u64,
        limit: NonZeroUsize,
    ) -> impl Iterator<Item = &InboxEntry> {
        self.inbox.pending_from(first_sequence, limit)
    }

    pub fn receive_stdin(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.input.append(bytes)?;
        let Self {
            inbox,
            outbox,
            assembler,
            decoder,
            output,
            ..
        } = self;
        decoder.feed(bytes, |frame| {
            match frame {
                TransportFrame::Chunk(chunk) => {
                    if let Some(message) = assembler.accept(chunk)? {
                        let route = FrameRoute::from(&message);
                        let sequence = inbox.accept(message)?;
                        TransportFrame::Receipt(DeliveryReceipt {
                            route: route.clone(),
                            stage: DeliveryStage::Accepted,
                            durable_sequence: sequence,
                        })
                        .write(output)?;
                        // A replay also repairs a lost presentation receipt.
                        if let Some(sequence) = inbox
                            .get(route.message_id)
                            .and_then(|entry| entry.presented_sequence)
                        {
                            TransportFrame::Receipt(DeliveryReceipt {
                                route,
                                stage: DeliveryStage::Presented,
                                durable_sequence: sequence,
                            })
                            .write(output)?;
                        }
                    }
                }
                TransportFrame::Receipt(receipt) => {
                    outbox.acknowledge(receipt)?;
                }
                TransportFrame::Ready(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "host readiness is an output notification",
                    ));
                }
            }
            Ok(())
        })
    }

    /// The extension calls this only after the input's message ID and thread /
    /// turn binding are in durable history, including history reconciliation on
    /// resume. This records presentation; model completion is recorded separately.
    pub fn confirm_presented(
        &mut self,
        message_id: MessageId,
        input: PresentedInput,
    ) -> io::Result<()> {
        let sequence = self.inbox.mark_presented(message_id, input)?;
        let entry = self
            .inbox
            .get(message_id)
            .ok_or_else(|| io::Error::other("presented inbox entry missing"))?;
        TransportFrame::Receipt(DeliveryReceipt {
            route: FrameRoute::from(&entry.message),
            stage: DeliveryStage::Presented,
            durable_sequence: sequence,
        })
        .write(&mut self.output)
    }

    pub fn confirm_processed(
        &mut self,
        message_id: MessageId,
        outcome_ref: String,
    ) -> io::Result<u64> {
        self.inbox.mark_processed(message_id, outcome_ref)
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.output.flush()
    }
}
