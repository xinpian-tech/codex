use std::io;
use std::path::Path;

use codex_infra_protocol::AgentId;
use codex_infra_protocol::MessageId;
use codex_infra_state::ArchiveProducer;
use codex_infra_state::ArchiveStream;

use super::TransportSession;
use crate::ArchiveJob;
use crate::ArchiveTarget;

impl TransportSession {
    /// Snapshots the machine's intended input and command outcomes for one Agent.
    /// None means no input journal exists; WouldBlock means execution, historical
    /// recovery or worker capacity is pending. Retry after the scheduler advances.
    /// Neither result implies input delivery or archive completion.
    /// Persist the returned job before submitting it to ArchiveController.
    pub fn prepare_input_archive(
        &mut self,
        agent_id: AgentId,
        receipts: &Path,
        job_id: MessageId,
    ) -> io::Result<Option<ArchiveJob>> {
        if !receipts.is_absolute() {
            return Err(io::Error::other(
                "pane input receipt directory must be absolute",
            ));
        }
        let Some((source, position)) = self.injector.archive_source(agent_id)? else {
            return Ok(None);
        };
        let machine_run_id = self.archive_binding.machine_run_id;
        let name = format!("pane-input-{agent_id}");
        Ok(Some(ArchiveJob {
            job_id,
            stream: ArchiveStream {
                root_session_id: self.config.root_session_id,
                machine_id: self.config.machine_id.clone(),
                producer: ArchiveProducer::Machine { machine_run_id },
                name: name.clone(),
            },
            source,
            receipt_journal: receipts.join(format!("{machine_run_id}-{name}.journal")),
            target: ArchiveTarget::Snapshot(position),
        }))
    }
}
