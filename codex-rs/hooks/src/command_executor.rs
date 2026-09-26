use std::ffi::OsString;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::time::Duration;

use codex_protocol::ThreadId;

/// How the process accepts its final argument or shell command string.
pub enum HookCommandArgument {
    Argument(String),
    WindowsRaw(String),
}

/// Input supplied to the child, independently of the audited hook event.
pub enum HookCommandStdin {
    Closed,
    Bytes(Vec<u8>),
}

/// Owned host-local hook launch, prepared before asynchronous scheduling.
/// Environment entries are ordered; later entries override earlier ones.
pub struct HookCommandRequest {
    pub thread_id: ThreadId,
    /// Actual invoking tool operation, independently of hook-visible IDs such
    /// as the original command ID reported by a write_stdin post hook.
    pub tool_call_id: Option<String>,
    pub program: OsString,
    pub arguments: Vec<OsString>,
    pub command: HookCommandArgument,
    pub environment: Vec<(OsString, OsString)>,
    pub cwd: PathBuf,
    pub event_json: String,
    pub stdin: HookCommandStdin,
    pub timeout: Option<Duration>,
}

/// Complete producer output, before hook interpretation and context spilling.
pub struct HookCommandOutput {
    pub exit_code: Option<i32>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

pub type HookCommandFuture =
    Pin<Box<dyn Future<Output = Result<HookCommandOutput, String>> + Send + 'static>>;

/// Supplies managed command execution without coupling hooks to its host.
/// `prepare` captures operation ownership synchronously, before background
/// scheduling. Its future owns startup, timeout, process cleanup, and complete
/// output collection. Implementations retain detached producers independently
/// when cancellation drops the future or the hook process leaves descendants.
pub trait HookCommandExecutor: Send + Sync {
    fn prepare(&self, request: HookCommandRequest) -> HookCommandFuture;
}
