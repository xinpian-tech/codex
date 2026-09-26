use std::fs::File;
use std::fs::OpenOptions;
use std::fs::TryLockError;
use std::io;
use std::path::Path;
use std::sync::Arc;

use codex_infra_protocol::MachineId;
use codex_infra_protocol::MessageId;
use codex_infra_protocol::RootSessionId;
use codex_infra_runtime::ArchiveJob;
use codex_infra_state::Journal;
use codex_infra_state::JournalPosition;
use codex_infra_state::JournalReader;
use serde::Deserialize;
use serde::Serialize;

use super::archive::ControlArchive;
use super::archive::read_prepared;

#[derive(Serialize, Deserialize)]
pub(super) struct ControlIdentity {
    pub(super) root_session_id: RootSessionId,
    pub(super) machine_id: MachineId,
    pub(super) run_id: MessageId,
}

impl ControlIdentity {
    pub(super) fn create(&self, directory: &Path) -> io::Result<Arc<File>> {
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(directory.join("run.lock"))?;
        lock.lock()?;
        let mut binding = Journal::open(&directory.join("identity.journal"), |_| {
            Err(io::Error::other("control identity already exists"))
        })?;
        binding.append(&serde_json::to_vec(self)?)?;
        Ok(Arc::new(lock))
    }
}

/// Owns the run lock while repairing interrupted journals and preparing jobs.
/// A live output owner or input reader keeps that lock, so discovery skips it.
pub(super) fn recover_control(directory: &Path) -> io::Result<Vec<ArchiveJob>> {
    let prepared = read_prepared(directory)?;
    let lock = match OpenOptions::new()
        .read(true)
        .write(true)
        .open(directory.join("run.lock"))
    {
        Ok(lock) => lock,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(prepared),
        Err(error) => return Err(error),
    };
    match lock.try_lock() {
        Ok(()) => {}
        Err(TryLockError::WouldBlock) => return Ok(prepared),
        Err(TryLockError::Error(error)) => return Err(error),
    }
    let mut identity = match JournalReader::open(
        &directory.join("identity.journal"),
        JournalPosition::default(),
    ) {
        Ok(reader) => reader,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(prepared),
        Err(error) => return Err(error),
    };
    let Some(record) = identity.next_record()? else {
        return Ok(prepared);
    };
    let identity: ControlIdentity = serde_json::from_slice(&record.payload)?;
    // Re-read under the lock: the former owner may have prepared jobs between
    // the first read and releasing its lock. Published streams remain closed.
    let prepared = read_prepared(directory)?;
    let mut positions = Vec::new();
    for name in ["stdout", "stdin", "lifecycle", "stdin-lifecycle"] {
        if prepared
            .iter()
            .any(|job| job.stream.name == format!("machine-control-{name}"))
        {
            continue;
        }
        let mut journal = Journal::open(&directory.join(format!("{name}.journal")), |_| Ok(()))?;
        match name {
            "stdin-lifecycle" => {
                journal.append(b"reader_recovered_after_owner_exit")?;
            }
            "lifecycle" => {
                journal.append(&serde_json::to_vec(&serde_json::json!({
                    "event": "control_recovered_after_owner_exit",
                    "detail": (),
                }))?)?;
            }
            _ => {}
        }
        positions.push((name, journal.position()));
    }
    if positions.is_empty() {
        return Ok(prepared);
    }
    ControlArchive {
        directory: directory.canonicalize()?,
        root_session_id: identity.root_session_id,
        machine_id: identity.machine_id,
        run_id: identity.run_id,
    }
    .prepare(&positions)
}
