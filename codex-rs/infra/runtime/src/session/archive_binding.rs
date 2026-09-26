use std::io;

use codex_infra_protocol::MachineId;
use codex_infra_protocol::MessageId;
use codex_infra_protocol::RootSessionId;
use codex_infra_state::Journal;
use serde::Deserialize;
use serde::Serialize;

use super::TransportSessionConfig;

#[derive(Serialize, Deserialize)]
struct Binding {
    root_session_id: RootSessionId,
    machine_id: MachineId,
    machine_run_id: MessageId,
}

/// The same spool retains its stream identity across runtime restarts. A new
/// spool gets a new ID; append-only transport journals are not tied to a single
/// collector attachment, which is replaced independently on reconnect.
pub(super) struct TransportArchiveBinding {
    pub(super) machine_run_id: MessageId,
    pub(super) journal: Journal,
}

impl TransportArchiveBinding {
    pub(super) fn open(config: &TransportSessionConfig) -> io::Result<Self> {
        let mut previous = None;
        let mut journal = Journal::open(
            &config.directory.join("archive-binding.journal"),
            |record| {
                let binding: Binding = serde_json::from_slice(&record.payload)?;
                if previous.is_some()
                    || binding.root_session_id != config.root_session_id
                    || binding.machine_id != config.machine_id
                {
                    return Err(io::Error::other("transport archive binding changed"));
                }
                previous = Some(binding.machine_run_id);
                Ok(())
            },
        )?;
        let machine_run_id = match previous {
            Some(id) => id,
            None => {
                let id = MessageId::new();
                journal.append(&serde_json::to_vec(&Binding {
                    root_session_id: config.root_session_id,
                    machine_id: config.machine_id.clone(),
                    machine_run_id: id,
                })?)?;
                id
            }
        };
        Ok(Self {
            machine_run_id,
            journal,
        })
    }
}
