use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use codex_infra_protocol::MessageId;
use codex_infra_runtime::ArchiveJob;
use codex_infra_runtime::ArchiveTarget;
use codex_infra_state::ArchiveProducer;
use codex_infra_state::ArchiveStream;

use super::AgentEventCursor;
use crate::StartedAgentHost;

impl StartedAgentHost {
    /// Stop and await the event consumer before calling. Capture and inference
    /// may still finish their independent tails; this seals only processing
    /// progress. Persist the returned job before enqueueing and wait for its
    /// exact archive receipt as part of Agent finalization.
    pub async fn close_event_consumer(
        &self,
        cursor: &AgentEventCursor,
        receipts: PathBuf,
        job_id: MessageId,
    ) -> io::Result<ArchiveJob> {
        if !receipts.is_absolute() {
            return Err(io::Error::other(
                "event cursor receipt directory must be absolute",
            ));
        }
        let source = self.host.events.source().to_path_buf();
        let run_start = self.host.events.run_start();
        let launch = self.launch.clone();
        let writer = Arc::clone(&cursor.writer);
        tokio::task::spawn_blocking(move || {
            let mut writer = writer
                .lock()
                .map_err(|error| io::Error::other(error.to_string()))?;
            if writer.source != source || writer.run_start != run_start {
                return Err(io::Error::other(
                    "event consumer belongs to another host runtime",
                ));
            }
            writer.closed = true;
            let name = format!("server-events-cursor-{}", run_start.next_sequence);
            Ok(ArchiveJob {
                job_id,
                receipt_journal: receipts.join(format!("{}-{name}.journal", launch.launch_id)),
                stream: ArchiveStream {
                    root_session_id: launch.workspace.root_session_id,
                    machine_id: launch.machine_id,
                    producer: ArchiveProducer::Agent {
                        agent_id: launch.workspace.agent_id,
                        launch_id: launch.launch_id,
                    },
                    name,
                },
                source: writer.path.clone(),
                target: ArchiveTarget::ProducerFinished(writer.journal.position()),
            })
        })
        .await
        .map_err(io::Error::other)?
    }
}
