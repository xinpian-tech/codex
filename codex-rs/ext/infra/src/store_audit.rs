use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;

use codex_infra_protocol::AgentId;
use codex_infra_protocol::MachineId;
use codex_infra_protocol::RootSessionId;
use codex_infra_state::Journal;
use codex_thread_store::ThreadStoreError;
use codex_thread_store::ThreadStoreResult;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreAuditIdentity {
    pub root_session_id: RootSessionId,
    pub agent_id: AgentId,
    pub machine_id: MachineId,
}

/// Audit history is independent of the backing store's retention or revert policy.
/// A started operation without a finished record has an unknown outcome; replay
/// consumers must reconcile with the store rather than repeat its side effects.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StoreAuditEvent {
    Opened {
        identity: StoreAuditIdentity,
    },
    Started {
        operation: String,
        payload: Value,
    },
    Finished {
        started_sequence: u64,
        outcome: Result<Value, String>,
    },
}

/// Prepared audit writer shared by adapters within one Agent host.
#[derive(Clone)]
pub struct StoreAudit {
    journal: Arc<Mutex<Journal>>,
}

impl StoreAudit {
    /// Recovers and opens a journal in an existing spool directory before host startup.
    pub fn open(path: &Path, identity: StoreAuditIdentity) -> io::Result<Self> {
        let mut journal = Journal::open(path, |record| {
            let event: StoreAuditEvent = serde_json::from_slice(&record.payload)?;
            if let StoreAuditEvent::Opened { identity: previous } = event
                && previous != identity
            {
                return Err(io::Error::other("thread store audit identity changed"));
            }
            Ok(())
        })?;
        journal.append(&serde_json::to_vec(&StoreAuditEvent::Opened { identity })?)?;
        Ok(Self {
            journal: Arc::new(Mutex::new(journal)),
        })
    }

    pub(crate) async fn append(&self, event: StoreAuditEvent) -> ThreadStoreResult<u64> {
        let journal = Arc::clone(&self.journal);
        tokio::task::spawn_blocking(move || {
            let bytes = serde_json::to_vec(&event).map_err(audit_error)?;
            journal
                .lock()
                .map_err(audit_error)?
                .append(&bytes)
                .map_err(audit_error)
        })
        .await
        .map_err(audit_error)?
    }
}

pub(crate) fn audit_error(error: impl std::fmt::Display) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: format!("thread store audit: {error}"),
    }
}
