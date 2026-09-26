use std::sync::Arc;
use std::time::Instant;

use futures::future::BoxFuture;
use serde_json::Map;
use serde_json::Value;

use super::ClaudeHooksEngine;
use super::ConfiguredHandler;
use super::ConfiguredHandlerKind;
use super::HandlerRunResult;
use super::mcp_runner::prepare_call;
use super::mcp_runner::run_mcp_tool;
use crate::HookMcpContext;

/// Returns None for command handlers. MCP preparation, including owned lease
/// capture by the host, happens before the returned task enters a queue.
pub(super) fn prepare(
    engine: &ClaudeHooksEngine,
    handler: &ConfiguredHandler,
    input_json: &str,
    metadata: Option<&Map<String, Value>>,
) -> Option<BoxFuture<'static, HandlerRunResult>> {
    let ConfiguredHandlerKind::McpTool {
        server,
        tool,
        input,
    } = &handler.kind
    else {
        return None;
    };
    let delegate = Arc::clone(&engine.mcp_executor);
    let Some(executor) = &engine.command_runtime.managed_mcp_executor else {
        let handler = handler.clone();
        let server = server.clone();
        let tool = tool.clone();
        let input = input.clone();
        let input_json = input_json.to_owned();
        let metadata = metadata.cloned();
        return Some(Box::pin(async move {
            run_mcp_tool(
                delegate.as_ref(),
                &handler,
                &server,
                &tool,
                &input,
                &input_json,
                metadata.as_ref(),
            )
            .await
        }));
    };
    let prepared = prepare_call(handler, server, tool, input, input_json, metadata).map(|call| {
        executor.prepare(
            HookMcpContext {
                thread_id: engine.command_runtime.thread_id,
                tool_call_id: engine.command_runtime.tool_call_id.clone(),
                event_json: input_json.to_owned(),
            },
            call,
            delegate,
        )
    });
    Some(Box::pin(async move {
        let started_at = chrono::Utc::now().timestamp();
        let started = Instant::now();
        let result = match prepared {
            Ok(future) => future.await.and_then(crate::HookMcpOutput::into_text),
            Err(error) => Err(error),
        };
        let (exit_code, stdout, error) = match result {
            Ok(text) => (Some(0), text, None),
            Err(error) => (None, String::new(), Some(error.to_string())),
        };
        HandlerRunResult {
            started_at,
            completed_at: chrono::Utc::now().timestamp(),
            duration_ms: started.elapsed().as_millis().try_into().unwrap_or(i64::MAX),
            exit_code,
            stdout,
            stderr: String::new(),
            error,
        }
    }))
}
