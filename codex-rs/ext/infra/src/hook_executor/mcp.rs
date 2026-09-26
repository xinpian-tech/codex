use std::sync::Arc;

use codex_extension_api::ToolCallSource;
use codex_extension_api::ToolExecutionInput;
use codex_extension_api::ToolExecutionKind;
use codex_extension_api::ToolExecutionOrigin;
use codex_extension_api::ToolLifecycleContributor;
use codex_extension_api::ToolName;
use codex_extension_api::ToolPayload;
use codex_hooks::HookMcpCall;
use codex_hooks::HookMcpContext;
use codex_hooks::HookMcpExecutor;
use codex_hooks::HookMcpFuture;
use codex_hooks::HookMcpOutput;
use codex_hooks::ManagedHookMcpExecutor;
use codex_infra_protocol::MessageId;

use super::Attribution;
use super::HookJob;
use super::RecordedHookExecutor;
use crate::RecordedToolPayload;
use crate::ToolAuditEvent;
use crate::ToolOperation;
use crate::ToolOrigin;
use crate::ToolOutcome;

impl ManagedHookMcpExecutor for RecordedHookExecutor {
    fn prepare(
        &self,
        context: HookMcpContext,
        call: HookMcpCall,
        delegate: Arc<dyn HookMcpExecutor>,
    ) -> HookMcpFuture {
        let prepared = (|| {
            let admission = {
                let tasks = self.tasks.lock().map_err(|error| error.to_string())?;
                if tasks.is_closed() {
                    return Err("hook executor is closed".to_owned());
                }
                tasks.token()
            };
            let attribution: Attribution =
                serde_json::from_str(&context.event_json).map_err(|error| error.to_string())?;
            let origin = context
                .tool_call_id
                .clone()
                .map(|call_id| ToolExecutionOrigin {
                    thread_id: context.thread_id.to_string(),
                    call_id,
                });
            let reservation = self
                .tools
                .workspace()
                .map_err(|error| error.to_string())?
                .as_ref()
                .and_then(|workspace| {
                    origin
                        .as_ref()
                        .map(|origin| workspace.operations().continue_operation(origin))
                })
                .transpose()
                .map_err(|error| error.to_string())?
                .flatten();
            let call_id = format!("mcp-hook-{}", MessageId::new());
            let operation = ToolOperation {
                thread_id: context.thread_id.to_string(),
                turn_id: attribution.turn_id.unwrap_or_else(|| call_id.clone()),
                call_id,
                tool_name: "hook_mcp".to_owned(),
                source: ToolOrigin::Direct,
            };
            Ok::<_, String>((admission, reservation, operation, origin))
        })();
        let worker = self.clone();
        Box::pin(async move {
            let (admission, reservation, operation, origin) =
                prepared.map_err(std::io::Error::other)?;
            let mut job = HookJob {
                failure: Arc::clone(&worker.failure),
                finished: false,
            };
            tokio::spawn(async move {
                let result = worker
                    .execute_mcp(context, call, delegate, operation, origin)
                    .await;
                if let Err(error) = &result {
                    job.failure
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .get_or_insert_with(|| error.clone());
                }
                drop(reservation);
                job.finished = true;
                drop(job);
                drop(admission);
                result
            })
            .await
            .map_err(std::io::Error::other)?
            .map_err(|error| std::io::Error::other(error).into())
        })
    }
}

impl RecordedHookExecutor {
    async fn execute_mcp(
        &self,
        context: HookMcpContext,
        mut call: HookMcpCall,
        delegate: Arc<dyn HookMcpExecutor>,
        operation: ToolOperation,
        origin: Option<ToolExecutionOrigin>,
    ) -> Result<HookMcpOutput, String> {
        // Match the thread metadata the connected Core executor adds, so the
        // journal contains the host-supplied attribution before dispatch.
        call.metadata.get_or_insert_default().insert(
            "threadId".to_owned(),
            serde_json::Value::String(context.thread_id.to_string()),
        );
        let body = serde_json::to_string(&serde_json::json!({ "context": context, "call": call }))
            .map_err(|error| error.to_string())?;
        let payload = ToolPayload::Custom {
            input: body.clone(),
        };
        let tool_name = ToolName::plain("hook_mcp");
        let lease = self
            .tools
            .acquire_tool_execution(ToolExecutionInput {
                thread_id: &operation.thread_id,
                turn_id: &operation.turn_id,
                call_id: &operation.call_id,
                tool_name: &tool_name,
                source: &ToolCallSource::Direct,
                kind: if origin.is_some() {
                    ToolExecutionKind::ExistingProcess
                } else {
                    ToolExecutionKind::Operation
                },
                origin: origin.as_ref(),
                payload: &payload,
            })
            .await;
        let result = match &lease {
            Ok(_) => match self
                .tools
                .append(Ok(ToolAuditEvent::Started {
                    operation: operation.clone(),
                    root_turn_id: None,
                    originating_item_id: None,
                    payload: RecordedToolPayload::Custom { input: body },
                }))
                .await
            {
                Ok(()) => {
                    let response = delegate
                        .execute_response(call)
                        .await
                        .map_err(|error| error.to_string());
                    self.tools
                        .append(Ok(ToolAuditEvent::McpHookResult {
                            operation: operation.clone(),
                            outcome: response.clone(),
                        }))
                        .await?;
                    response
                }
                Err(error) => Err(error),
            },
            Err(error) => Err(error.clone()),
        };
        let outcome = match &result {
            Ok(HookMcpOutput::Response(response)) => ToolOutcome::Completed {
                success: response.is_error != Some(true),
            },
            Ok(HookMcpOutput::Text(_)) => ToolOutcome::Completed { success: true },
            Err(_) => ToolOutcome::Failed {
                handler_executed: lease.is_ok(),
            },
        };
        self.tools
            .append(Ok(ToolAuditEvent::Finished { operation, outcome }))
            .await?;
        drop(lease);
        result
    }
}
