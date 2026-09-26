use std::sync::Arc;
use std::time::Duration;

use codex_protocol::ThreadId;
use codex_protocol::mcp::CallToolResult;
use futures::future::BoxFuture;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Map;
use serde_json::Value;

/// One MCP tool call requested by a configured hook handler.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HookMcpCall {
    pub server: String,
    pub tool: String,
    pub environment_id: Option<String>,
    pub metadata: Option<Map<String, Value>>,
    pub input: Map<String, Value>,
    pub timeout: Duration,
}

/// Response before command-hook text interpretation. Legacy executors expose
/// text only; a structured response preserves all MCP content and metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum HookMcpOutput {
    Text(String),
    Response(CallToolResult),
}

impl HookMcpOutput {
    pub fn into_text(self) -> anyhow::Result<String> {
        match self {
            Self::Text(text) => Ok(text),
            Self::Response(response) => {
                let text = response
                    .content
                    .iter()
                    .filter_map(|content| {
                        (content.get("type").and_then(Value::as_str) == Some("text"))
                            .then(|| content.get("text").and_then(Value::as_str))
                            .flatten()
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                if response.is_error == Some(true) {
                    anyhow::bail!("MCP tool returned an error: {text}");
                }
                Ok(text)
            }
        }
    }
}

/// Executes already-connected MCP tools on behalf of hooks without coupling this crate to core.
///
/// Implementations own server readiness, policy enforcement, timeout handling, and elicitation.
pub trait HookMcpExecutor: Send + Sync {
    /// Returns text that is interpreted using ordinary command-hook output semantics.
    fn execute(&self, call: HookMcpCall) -> BoxFuture<'_, anyhow::Result<String>>;

    /// Preserves the complete server response for managed recording before
    /// hook output parsing. Existing text-only executors remain explicit Text.
    fn execute_response(&self, call: HookMcpCall) -> BoxFuture<'_, anyhow::Result<HookMcpOutput>> {
        Box::pin(async move { self.execute(call).await.map(HookMcpOutput::Text) })
    }
}

/// Invocation-local ownership carried outside the public hook input schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HookMcpContext {
    pub thread_id: ThreadId,
    pub tool_call_id: Option<String>,
    pub event_json: String,
}

pub type HookMcpFuture = BoxFuture<'static, anyhow::Result<HookMcpOutput>>;

/// Wraps the host's connected MCP executor with managed operation ownership.
/// Preparation happens synchronously before queueing. The returned future
/// owns its resources and delegate; implementations retain running calls and
/// recording independently if the hook caller cancels its wait.
pub trait ManagedHookMcpExecutor: Send + Sync {
    fn prepare(
        &self,
        context: HookMcpContext,
        call: HookMcpCall,
        delegate: Arc<dyn HookMcpExecutor>,
    ) -> HookMcpFuture;
}
