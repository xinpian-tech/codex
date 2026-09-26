use codex_tools::ToolCallSource;
use codex_tools::ToolExecutionKind;
use codex_tools::ToolName;
use codex_tools::ToolPayload;

use crate::ExtensionFuture;

/// Host-owned identity before pre-tool hooks and handler dispatch. Arguments
/// may still be rewritten by hooks; finalized payloads arrive at tool start.
pub struct ToolExecutionInput<'a> {
    pub thread_id: &'a str,
    pub turn_id: &'a str,
    pub call_id: &'a str,
    pub tool_name: &'a ToolName,
    pub source: &'a ToolCallSource,
    pub kind: ToolExecutionKind,
    pub payload: &'a ToolPayload,
}

/// Owned execution scope held through dispatch and its finish callbacks.
/// Implementations release their resources on Drop, including cancellation.
/// Descendant work that outlives the handler must retain its own ownership;
/// dropping this lease alone does not prove child processes have stopped.
pub trait ToolExecutionLease: Send + Sync {}

pub type ToolExecutionFuture<'a> =
    ExtensionFuture<'a, Result<Option<Box<dyn ToolExecutionLease>>, String>>;
