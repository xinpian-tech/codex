use std::io;
use std::path::Path;

use codex_infra_protocol::AgentId;
use codex_infra_protocol::RootSessionId;
use codex_infra_state::Journal;
use codex_infra_tmux::TmuxClient;
use codex_infra_tmux::TransportFrame;
use serde::Deserialize;
use serde::Serialize;

use crate::FrameRouter;
use crate::PaneReadiness;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PaneInputEvent {
    Intended {
        root_session_id: RootSessionId,
        agent_id: AgentId,
        directory_revision: u64,
        session: String,
        window: String,
        pane: String,
        bytes: Vec<u8>,
    },
    Injected {
        intended_sequence: u64,
    },
    Failed {
        intended_sequence: u64,
        error: String,
    },
}

/// Captures exact intended pane input independently of the receiving host's
/// inbox. A successful tmux command never substitutes for the host's receipt.
pub struct PaneInputJournal {
    journal: Journal,
}

impl PaneInputJournal {
    pub fn open(path: &Path) -> io::Result<Self> {
        let journal = Journal::open(path, |record| {
            let _: PaneInputEvent =
                serde_json::from_slice(&record.payload).map_err(io::Error::other)?;
            Ok(())
        })?;
        Ok(Self { journal })
    }

    /// The runtime calls this after the destination host's raw-mode handshake.
    /// Retries resolve current placement again and record another input attempt;
    /// the original frame/message ID is preserved for receiving-host deduplication.
    pub fn inject(
        &mut self,
        router: &FrameRouter<'_>,
        readiness: &PaneReadiness,
        client: &TmuxClient,
        frame: &TransportFrame,
    ) -> io::Result<u64> {
        let target = router.incoming(frame)?;
        readiness.require_ready(target)?;
        let mut bytes = Vec::new();
        frame.write(&mut bytes)?;
        let intended = PaneInputEvent::Intended {
            root_session_id: target.root_session_id,
            agent_id: target.agent_id,
            directory_revision: target.revision,
            session: target.tmux_session.clone(),
            window: target.tmux_window.clone(),
            pane: target.tmux_pane.clone(),
            bytes,
        };
        let sequence = self
            .journal
            .append(&serde_json::to_vec(&intended).map_err(io::Error::other)?)?;
        let PaneInputEvent::Intended { pane, bytes, .. } = &intended else {
            unreachable!("input attempt constructed above");
        };
        let result = client.write_input(pane, bytes);
        let event = match &result {
            Ok(()) => PaneInputEvent::Injected {
                intended_sequence: sequence,
            },
            Err(error) => PaneInputEvent::Failed {
                intended_sequence: sequence,
                error: error.to_string(),
            },
        };
        self.journal
            .append(&serde_json::to_vec(&event).map_err(io::Error::other)?)?;
        result?;
        Ok(sequence)
    }
}
