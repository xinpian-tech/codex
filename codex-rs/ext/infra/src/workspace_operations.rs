use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use std::sync::Mutex;

use codex_exec_server_protocol::ExecMetadata;

use crate::ToolOperation;
use crate::WorkspaceLease;

struct ActiveOperation {
    operation: ToolOperation,
    lease: WorkspaceLease,
}

/// Controller-owned leases available to synchronous process reservations.
/// Register before dispatch; take the controller share for checkpoint handoff
/// after the handler finishes. Already reserved children retain their shares.
#[derive(Clone, Default)]
pub struct WorkspaceOperations {
    active: Arc<Mutex<BTreeMap<(String, String), ActiveOperation>>>,
}

impl WorkspaceOperations {
    pub fn register(&self, operation: ToolOperation, lease: WorkspaceLease) -> io::Result<()> {
        let key = (operation.thread_id.clone(), operation.call_id.clone());
        let mut active = self
            .active
            .lock()
            .map_err(|error| io::Error::other(error.to_string()))?;
        if active.contains_key(&key) {
            return Err(io::Error::other("workspace operation already registered"));
        }
        active.insert(key, ActiveOperation { operation, lease });
        Ok(())
    }

    pub fn take(&self, operation: &ToolOperation) -> io::Result<WorkspaceLease> {
        let key = (operation.thread_id.clone(), operation.call_id.clone());
        let mut active = self
            .active
            .lock()
            .map_err(|error| io::Error::other(error.to_string()))?;
        let entry = active
            .get(&key)
            .ok_or_else(|| io::Error::other("workspace operation not registered"))?;
        if &entry.operation != operation {
            return Err(io::Error::other("workspace operation identity changed"));
        }
        active
            .remove(&key)
            .map(|entry| entry.lease)
            .ok_or_else(|| io::Error::other("workspace operation disappeared"))
    }

    pub(crate) fn reserve(&self, metadata: Option<&ExecMetadata>) -> io::Result<WorkspaceLease> {
        let metadata = metadata
            .ok_or_else(|| io::Error::other("workspace process has no tool attribution"))?;
        let thread_id = metadata
            .thread_id
            .as_ref()
            .ok_or_else(|| io::Error::other("workspace process has no thread identity"))?;
        let call_id = metadata
            .tool_call_id
            .as_ref()
            .ok_or_else(|| io::Error::other("workspace process has no tool call identity"))?;
        let active = self
            .active
            .lock()
            .map_err(|error| io::Error::other(error.to_string()))?;
        active
            .get(&(thread_id.to_string(), call_id.clone()))
            .map(|entry| entry.lease.clone())
            .ok_or_else(|| io::Error::other("workspace process operation is no longer active"))
    }
}
