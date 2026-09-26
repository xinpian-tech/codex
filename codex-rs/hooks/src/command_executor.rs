use std::ffi::OsString;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::time::Duration;

use codex_protocol::ThreadId;

/// How the configured shell accepts its final command string.
pub enum HookCommandArgument {
    Argument(String),
    WindowsRaw(String),
}

/// Owned host-local hook launch, prepared before asynchronous scheduling.
/// Environment entries are ordered; later entries override earlier ones.
pub struct HookCommandRequest {
    pub thread_id: ThreadId,
    pub program: OsString,
    pub arguments: Vec<OsString>,
    pub command: HookCommandArgument,
    pub environment: Vec<(OsString, OsString)>,
    pub cwd: PathBuf,
    pub stdin: Vec<u8>,
    pub timeout: Duration,
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
