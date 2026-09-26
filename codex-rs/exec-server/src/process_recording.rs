use std::collections::HashMap;
use std::sync::Arc;

use codex_utils_absolute_path::AbsolutePathBuf;
use serde::Deserialize;
use serde::Serialize;

use crate::ExecProcessEvent;
use crate::ExecProcessFuture;
use crate::protocol::ExecParams;
use crate::protocol::JSONRPCErrorError;
use crate::protocol::ProcessSandboxType;
use crate::protocol::WriteParams;
use crate::protocol::WriteResponse;

/// Effective host-local arguments after command preparation and shell snapshot
/// rewriting, immediately before the spawn request. This is not a spawn receipt.
/// Inherited descriptor contents and platform launcher internals are separate
/// provenance; argv alone does not reconstruct a shell snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreparedProcessCommand {
    pub command: Vec<String>,
    pub cwd: AbsolutePathBuf,
    pub env: HashMap<String, String>,
    pub arg0: Option<String>,
    pub sandbox: ProcessSandboxType,
}

/// Records the outcome of one stdin request after its raw input was persisted.
/// Accepted means queued for writing, not consumed by the child. A missing
/// outcome remains unknown and must not cause automatic replay of input.
pub trait ProcessInputRecorder: Send + Sync {
    fn finish(
        &self,
        outcome: Result<WriteResponse, JSONRPCErrorError>,
    ) -> ExecProcessFuture<'_, ()>;
}

/// Receives producer-owned output and lifecycle events before bounded fan-out.
/// Implementations persist the event before resolving, offloading filesystem
/// work from async runtime threads. An error makes the process recording fail.
/// Callbacks run inside the producer's ordering boundary and must not reenter
/// the execution backend; subsequent work can consume the persisted records.
pub trait ProcessRecorder: Send + Sync {
    /// Persists the effective command before the backend attempts to spawn it.
    fn prepared(&self, command: PreparedProcessCommand) -> ExecProcessFuture<'_, ()>;

    fn record(&self, event: ExecProcessEvent) -> ExecProcessFuture<'_, ()>;
}

/// Prepares a recorder before process creation or output consumption starts.
/// The host owns correlation and storage layout. Repeated starts for the same
/// process ID must reuse the recorder, matching exec-server's retry semantics.
pub trait ProcessRecorderFactory: Send + Sync {
    /// Persists every stdin request, including retries and unknown process IDs.
    /// Each returned recorder correlates exactly one request with its outcome.
    fn open_input<'a>(
        &'a self,
        params: &'a WriteParams,
    ) -> ExecProcessFuture<'a, Arc<dyn ProcessInputRecorder>>;

    fn open<'a>(
        &'a self,
        params: &'a ExecParams,
    ) -> ExecProcessFuture<'a, Arc<dyn ProcessRecorder>>;
}
