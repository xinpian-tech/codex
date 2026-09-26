use std::sync::Arc;

use codex_hooks::HookMcpCall;
use codex_hooks::HookMcpExecutor;
use codex_hooks::HookMcpOutput;
use codex_mcp::McpRuntime;
use codex_protocol::ThreadId;
use futures::FutureExt;
use futures::future::BoxFuture;
use serde_json::Value;

pub(crate) struct CoreHookMcpExecutor {
    pub(crate) runtime: Arc<McpRuntime>,
    // Session-scoped MCP tools require the owning thread ID in request metadata.
    pub(crate) thread_id: ThreadId,
}

impl HookMcpExecutor for CoreHookMcpExecutor {
    fn execute(&self, call: HookMcpCall) -> BoxFuture<'_, anyhow::Result<String>> {
        Box::pin(async move { self.execute_response(call).await?.into_text() })
    }

    fn execute_response(&self, call: HookMcpCall) -> BoxFuture<'_, anyhow::Result<HookMcpOutput>> {
        async move {
            let mut metadata = call.metadata.unwrap_or_default();
            metadata.insert(
                "threadId".to_string(),
                Value::String(self.thread_id.to_string()),
            );

            let result = self
                .runtime
                .latest_call_tool(
                    &call.server,
                    &call.tool,
                    call.environment_id.as_deref(),
                    Some(Value::Object(call.input)),
                    Some(Value::Object(metadata)),
                    Some(call.timeout),
                    /*wait_for_server*/ false,
                )
                .await?;
            Ok(HookMcpOutput::Response(result))
        }
        .boxed()
    }
}
