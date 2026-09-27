use std::io;
use std::path::Path;

use codex_infra_protocol::MessageId;
use codex_infra_state::ArchiveProducer;
use codex_infra_state::ArchiveStream;

use super::TerminalMailboxExit;
use crate::ArchiveJob;
use crate::ArchiveTarget;

impl TerminalMailboxExit {
    /// The stdin thread has joined and its lifecycle writer has closed. Persist
    /// this job before enqueueing; its remote receipt is separate from mailbox
    /// stdout/inbox completion and from restoring terminal attributes.
    pub fn input_lifecycle_archive_job(
        &self,
        receipts: &Path,
        job_id: MessageId,
    ) -> io::Result<ArchiveJob> {
        if !receipts.is_absolute() {
            return Err(io::Error::other(
                "terminal receipt directory must be absolute",
            ));
        }
        let launch = &self.launch;
        Ok(ArchiveJob {
            job_id,
            stream: ArchiveStream {
                root_session_id: launch.workspace.root_session_id,
                machine_id: launch.machine_id.clone(),
                producer: ArchiveProducer::Agent {
                    agent_id: launch.workspace.agent_id,
                    launch_id: launch.launch_id,
                },
                name: "input-lifecycle".to_owned(),
            },
            source: self.lifecycle_path.clone(),
            receipt_journal: receipts.join(format!("{}-input-lifecycle.journal", launch.launch_id)),
            target: ArchiveTarget::ProducerFinished(self.input_lifecycle),
        })
    }
}
