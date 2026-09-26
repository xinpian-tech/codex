use codex_extension_api::ToolExecutionInput;
use codex_extension_api::ToolExecutionLease;

use super::context::ToolInvocation;
use super::lifecycle::extension_tool_call_source;

pub(super) async fn acquire(
    invocation: &ToolInvocation,
    kind: codex_tools::ToolExecutionKind,
) -> Result<Vec<Box<dyn ToolExecutionLease>>, String> {
    let thread_id = invocation.session.thread_id.to_string();
    let source = extension_tool_call_source(invocation.source.clone());
    let mut leases = Vec::new();
    for contributor in invocation
        .session
        .services
        .extensions
        .tool_lifecycle_contributors()
    {
        if let Some(lease) = contributor
            .acquire_tool_execution(ToolExecutionInput {
                thread_id: &thread_id,
                turn_id: &invocation.turn.sub_id,
                call_id: &invocation.call_id,
                tool_name: &invocation.tool_name,
                source: &source,
                kind,
                payload: &invocation.payload,
            })
            .await?
        {
            leases.push(lease);
        }
    }
    Ok(leases)
}
