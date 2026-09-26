use std::collections::BTreeSet;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use codex_infra_protocol::AgentId;
use codex_infra_protocol::MachineId;
use codex_infra_protocol::RootSessionId;
use codex_infra_state::Journal;
use codex_infra_state::JournalPosition;
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
    writer: Arc<Mutex<Writer>>,
    pending: Arc<AtomicUsize>,
}

struct Writer {
    journal: Journal,
    active: BTreeSet<u64>,
    failure: Option<String>,
}

struct PendingWrite(Arc<AtomicUsize>);

impl Drop for PendingWrite {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl StoreAudit {
    /// Recovers and opens a journal in an existing spool directory before host startup.
    pub fn open(path: &Path, identity: StoreAuditIdentity) -> io::Result<Self> {
        let mut active = BTreeSet::new();
        let mut journal = Journal::open(path, |record| {
            let event: StoreAuditEvent = serde_json::from_slice(&record.payload)?;
            if let StoreAuditEvent::Opened { identity: previous } = &event
                && previous != &identity
            {
                return Err(io::Error::other("thread store audit identity changed"));
            }
            reconcile(&mut active, record.sequence, &event)
        })?;
        journal.append(&serde_json::to_vec(&StoreAuditEvent::Opened { identity })?)?;
        Ok(Self {
            writer: Arc::new(Mutex::new(Writer {
                journal,
                active,
                failure: None,
            })),
            pending: Arc::default(),
        })
    }

    /// Call with store dispatch quiescent. Unfinished mutations, including
    /// cancelled operations recovered from disk, are not completion evidence.
    pub fn settled_position(&self) -> io::Result<JournalPosition> {
        let writer = self
            .writer
            .lock()
            .map_err(|error| io::Error::other(error.to_string()))?;
        if self.pending.load(Ordering::SeqCst) != 0 || !writer.active.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "thread store audit operations pending",
            ));
        }
        if let Some(error) = &writer.failure {
            return Err(io::Error::other(error.clone()));
        }
        Ok(writer.journal.position())
    }

    pub(crate) async fn append(&self, event: StoreAuditEvent) -> ThreadStoreResult<u64> {
        let writer = Arc::clone(&self.writer);
        self.pending.fetch_add(1, Ordering::SeqCst);
        let pending = PendingWrite(Arc::clone(&self.pending));
        tokio::task::spawn_blocking(move || {
            let _pending = pending;
            let mut writer = writer.lock().map_err(audit_error)?;
            let result = (|| {
                let sequence = writer.journal.append(&serde_json::to_vec(&event)?)?;
                reconcile(&mut writer.active, sequence, &event)?;
                Ok::<_, io::Error>(sequence)
            })();
            if let Err(error) = &result {
                writer.failure.get_or_insert_with(|| error.to_string());
            }
            result.map_err(audit_error)
        })
        .await
        .map_err(audit_error)?
    }
}

fn reconcile(active: &mut BTreeSet<u64>, sequence: u64, event: &StoreAuditEvent) -> io::Result<()> {
    match event {
        StoreAuditEvent::Opened { .. } => {}
        StoreAuditEvent::Started { .. } => {
            active.insert(sequence);
        }
        StoreAuditEvent::Finished {
            started_sequence, ..
        } => {
            if !active.remove(started_sequence) {
                return Err(io::Error::other("thread store finish has no active start"));
            }
        }
    }
    Ok(())
}

pub(crate) fn audit_error(error: impl std::fmt::Display) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: format!("thread store audit: {error}"),
    }
}
