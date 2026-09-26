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
    let lock = match OpenOptions::new()
        .read(true)
        .write(true)
        .open(directory.join("run.lock"))
    {
        Ok(lock) => lock,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    match lock.try_lock() {
        Ok(()) => {}
        Err(TryLockError::WouldBlock) => return Ok(Vec::new()),
        Err(TryLockError::Error(error)) => return Err(error),
    }
    let mut identity = match JournalReader::open(
        &directory.join("identity.journal"),
        JournalPosition::default(),
    ) {
        Ok(reader) => reader,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let Some(record) = identity.next_record()? else {
        return Ok(Vec::new());
    };
    let identity: ControlIdentity = serde_json::from_slice(&record.payload)?;
    // A prepared record is immutable: preserve the original job IDs and avoid
    // appending to streams whose final positions may already be published.
    match JournalReader::open(
        &directory.join("archive.journal"),
        JournalPosition::default(),
    ) {
        Ok(mut reader) => {
            if let Some(record) = reader.next_record()? {
                return serde_json::from_slice(&record.payload).map_err(io::Error::other);
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let output = Journal::open(&directory.join("stdout.journal"), |_| Ok(()))?;
    let input = Journal::open(&directory.join("stdin.journal"), |_| Ok(()))?;
    let mut input_lifecycle =
        Journal::open(&directory.join("stdin-lifecycle.journal"), |_| Ok(()))?;
    let mut lifecycle = Journal::open(&directory.join("lifecycle.journal"), |_| Ok(()))?;
    input_lifecycle.append(b"reader_recovered_after_owner_exit")?;
    lifecycle.append(&serde_json::to_vec(&serde_json::json!({
        "event": "control_recovered_after_owner_exit",
        "detail": { "input": input.position(), "output": output.position() },
    }))?)?;
    ControlArchive {
        directory: directory.canonicalize()?,
        root_session_id: identity.root_session_id,
        machine_id: identity.machine_id,
        run_id: identity.run_id,
    }
    .prepare(&[
        ("stdout", output.position()),
        ("stdin", input.position()),
        ("lifecycle", lifecycle.position()),
        ("stdin-lifecycle", input_lifecycle.position()),
    ])
}
