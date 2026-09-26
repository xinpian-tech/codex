use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use std::sync::Mutex;

use codex_exec_server_protocol::ExecMetadata;
use codex_extension_api::ToolExecutionOrigin;

use crate::ToolOperation;
use crate::WorkspaceLease;
use crate::workspace_gate::WorkspaceLeaseRef;

struct ActiveOperation {
    operation: ToolOperation,
    lease: WorkspaceLease,
}

#[derive(Default)]
struct OperationRegistry {
    active: BTreeMap<(String, String), ActiveOperation>,
    handed_off: BTreeMap<(String, String), WorkspaceLeaseRef>,
}

/// Controller-owned leases available to synchronous process reservations.
/// Register before dispatch; take the controller share for checkpoint handoff
/// after the handler finishes. Already reserved children retain their shares.
#[derive(Clone, Default)]
pub struct WorkspaceOperations {
    registry: Arc<Mutex<OperationRegistry>>,
}

impl WorkspaceOperations {
    pub fn register(&self, operation: ToolOperation, lease: WorkspaceLease) -> io::Result<()> {
        let key = (operation.thread_id.clone(), operation.call_id.clone());
        let mut registry = self
            .registry
            .lock()
            .map_err(|error| io::Error::other(error.to_string()))?;
        registry.handed_off.retain(|_, lease| lease.is_live());
        if registry.active.contains_key(&key) || registry.handed_off.contains_key(&key) {
            return Err(io::Error::other("workspace operation already registered"));
        }
        registry
            .active
            .insert(key, ActiveOperation { operation, lease });
        Ok(())
    }

    pub fn take(&self, operation: &ToolOperation) -> io::Result<WorkspaceLease> {
        let key = (operation.thread_id.clone(), operation.call_id.clone());
        let mut registry = self
            .registry
            .lock()
            .map_err(|error| io::Error::other(error.to_string()))?;
        let entry = registry
            .active
            .get(&key)
            .ok_or_else(|| io::Error::other("workspace operation not registered"))?;
        if &entry.operation != operation {
            return Err(io::Error::other("workspace operation identity changed"));
        }
        let entry = registry
            .active
            .remove(&key)
            .ok_or_else(|| io::Error::other("workspace operation disappeared"))?;
        registry.handed_off.retain(|_, lease| lease.is_live());
        registry.handed_off.insert(key, entry.lease.downgrade());
        Ok(entry.lease)
    }

    /// Retains an observed originating operation through interaction and hooks.
    /// Returns None once checkpoint owns it exclusively or it has finished;
    /// the caller must then acquire a new operation before running hooks.
    pub fn continue_operation(
        &self,
        origin: &ToolExecutionOrigin,
    ) -> io::Result<Option<WorkspaceLease>> {
        let key = (origin.thread_id.clone(), origin.call_id.clone());
        let mut registry = self
            .registry
            .lock()
            .map_err(|error| io::Error::other(error.to_string()))?;
        registry.handed_off.retain(|_, lease| lease.is_live());
        if let Some(entry) = registry.active.get(&key) {
            return Ok(Some(entry.lease.clone()));
        }
        Ok(registry
            .handed_off
            .get(&key)
            .and_then(WorkspaceLeaseRef::upgrade))
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
        let registry = self
            .registry
            .lock()
            .map_err(|error| io::Error::other(error.to_string()))?;
        registry
            .active
            .get(&(thread_id.to_string(), call_id.clone()))
            .map(|entry| entry.lease.clone())
            .ok_or_else(|| io::Error::other("workspace process operation is no longer active"))
    }
}
