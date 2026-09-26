use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::fs::TryLockError;
use std::io;
use std::path::Path;

use codex_infra_protocol::AgentId;
use codex_infra_protocol::InferenceBinding;
use codex_infra_protocol::MachineId;
use codex_infra_protocol::MessageId;
use codex_infra_protocol::RootSessionId;
use codex_infra_state::Journal;
use codex_infra_state::JournalPosition;
use codex_infra_state::JournalReader;
use serde::Deserialize;
use serde::Serialize;
use serde_json::json;

use super::ProviderAttemptEnd;
use super::ProviderAttemptFinished;
use super::WIRE_LANES;
use super::WireLane;

/// Identity captured by the producer before request bytes are accepted.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderAttemptIdentity {
    pub attempt_id: MessageId,
    pub root_session_id: RootSessionId,
    pub machine_id: MachineId,
    pub agent_id: AgentId,
    pub launch_id: MessageId,
    pub binding: InferenceBinding,
}

impl ProviderAttemptIdentity {
    pub fn read(directory: &Path) -> io::Result<Option<Self>> {
        let mut reader = match JournalReader::open(
            &directory.join("lifecycle.journal"),
            JournalPosition::default(),
        ) {
            Ok(reader) => reader,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        reader
            .next_record()?
            .map(|record| serde_json::from_slice(&record.payload).map_err(io::Error::other))
            .transpose()
    }
}

/// Blocking recovery for one attempt. A live owner is skipped. The run lock
/// covers every audit clone and pending blocking write, so successful lock
/// acquisition establishes that no producer can append during journal replay.
pub fn recover_provider_attempt(directory: &Path) -> io::Result<Option<ProviderAttemptFinished>> {
    if let Some(finished) = ProviderAttemptFinished::read(directory)? {
        return Ok(Some(finished));
    }
    let lock = match OpenOptions::new()
        .read(true)
        .write(true)
        .open(directory.join("run.lock"))
    {
        Ok(lock) => lock,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    match lock.try_lock() {
        Ok(()) => {}
        Err(TryLockError::WouldBlock) => return Ok(None),
        Err(TryLockError::Error(error)) => return Err(error),
    }
    if let Some(finished) = ProviderAttemptFinished::read(directory)? {
        return Ok(Some(finished));
    }
    let Some(identity) = ProviderAttemptIdentity::read(directory)? else {
        return Ok(None);
    };
    let mut positions = BTreeMap::new();
    for lane in WIRE_LANES {
        let mut journal =
            Journal::open(&directory.join(format!("{}.journal", lane.name())), |_| {
                Ok(())
            })?;
        if matches!(lane, WireLane::Lifecycle) {
            journal.append(&serde_json::to_vec(
                &json!({"event": "owner_exit_recovered", "attempt_id": identity.attempt_id}),
            )?)?;
        }
        positions.insert(lane.name().to_owned(), journal.position());
    }
    let finished = ProviderAttemptFinished {
        attempt_id: identity.attempt_id,
        end: ProviderAttemptEnd::OwnerExited,
        positions,
    };
    let mut completion = Journal::open(&directory.join("completion.journal"), |_| {
        Err(io::Error::other(
            "provider completion appeared during recovery",
        ))
    })?;
    completion.append(&serde_json::to_vec(&finished)?)?;
    Ok(Some(finished))
}
