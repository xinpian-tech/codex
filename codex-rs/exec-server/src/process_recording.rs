use std::collections::HashMap;
use std::sync::Arc;

use codex_utils_absolute_path::AbsolutePathBuf;
use serde::Deserialize;
use serde::Serialize;

use crate::ExecProcessEvent;
use crate::ExecProcessFuture;
use crate::protocol::ByteChunk;
use crate::protocol::ExecParams;
use crate::protocol::ExecResponse;
use crate::protocol::JSONRPCErrorError;
use crate::protocol::ProcessSandboxType;
use crate::protocol::WriteParams;
use crate::protocol::WriteResponse;

/// Effective host-local arguments after command preparation and shell snapshot
/// rewriting, immediately before the spawn request. This is not a spawn receipt.
/// Platform launcher internals are separate provenance. Snapshot bytes include
/// the descriptor-closing prefix actually passed to the child shell.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreparedProcessCommand {
    pub command: Vec<String>,
    pub cwd: AbsolutePathBuf,
    pub env: HashMap<String, String>,
    pub arg0: Option<String>,
    pub sandbox: ProcessSandboxType,
    /// Older records omit this field and do not establish snapshot provenance.
    #[serde(default)]
    pub shell_snapshot: Option<PreparedShellSnapshot>,
}

/// Exact inherited snapshot descriptor and its contents at spawn preparation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreparedShellSnapshot {
    pub descriptor: i32,
    pub contents: ByteChunk,
}

#[cfg(unix)]
impl PreparedShellSnapshot {
    pub(crate) async fn capture(file: &std::fs::File) -> Result<Self, crate::ExecServerError> {
        use std::os::fd::AsRawFd;
        use std::os::unix::fs::FileExt;

        let descriptor = file.as_raw_fd();
        let file = file.try_clone().map_err(|error| {
            crate::ExecServerError::Protocol(format!("snapshot recording: {error}"))
        })?;
        tokio::task::spawn_blocking(move || -> std::io::Result<Self> {
            let length = usize::try_from(file.metadata()?.len()).map_err(std::io::Error::other)?;
            let mut contents = vec![0; length];
            // dup shares a file offset; positional reads leave the child's
            // source descriptor at its original position.
            file.read_exact_at(&mut contents, /*offset*/ 0)?;
            Ok(Self {
                descriptor,
                contents: contents.into(),
            })
        })
        .await
        .map_err(|error| crate::ExecServerError::Protocol(format!("snapshot recording: {error}")))?
        .map_err(|error| crate::ExecServerError::Protocol(format!("snapshot recording: {error}")))
    }
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
    /// Records the outcome of this start attempt, independently of producer
    /// failures. Output can arrive before the start response is recorded.
    fn start_finished(
        &self,
        outcome: Result<ExecResponse, JSONRPCErrorError>,
    ) -> ExecProcessFuture<'_, ()>;

    /// Persists the effective command before the backend attempts to spawn it.
    fn prepared(&self, command: PreparedProcessCommand) -> ExecProcessFuture<'_, ()>;

    fn record(&self, event: ExecProcessEvent) -> ExecProcessFuture<'_, ()>;
}

/// Prepares a recorder before process creation or output consumption starts.
/// The host owns correlation and storage layout. Each attempt gets a distinct
/// recorder: a rejected duplicate start must not fail the existing process.
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
