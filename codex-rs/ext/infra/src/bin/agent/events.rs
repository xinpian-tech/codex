use std::io;
use std::time::SystemTime;

use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::ServerRequest;
use codex_app_server_protocol::ThreadItem;
use codex_infra_extension::AgentServerEvent;
use codex_infra_extension::AgentServerEventRecord;
use codex_infra_extension::StartedAgentHost;
use codex_infra_protocol::AgentMessage;
use codex_infra_protocol::MessageAddress;
use codex_infra_protocol::MessageId;
use codex_infra_protocol::MessageKind;
use codex_infra_protocol::Presentation;
use codex_infra_state::InboxEntry;
use serde_json::Value;
use serde_json::json;

use super::run::LoopState;

pub async fn handle(
    host: &StartedAgentHost,
    config: &super::run::RunConfig,
    bootstrap: &InboxEntry,
    record: &AgentServerEventRecord,
    state: &mut LoopState,
) -> io::Result<()> {
    if let AgentServerEvent::Notification { notification } = &record.event {
        let notification = notification.as_ref();
        if let ServerNotification::TurnStarted(event) = notification
            && event.thread_id == state.thread_id
        {
            state.active = true;
        }
        if let ServerNotification::TurnCompleted(event) = notification
            && event.thread_id == state.thread_id
        {
            state.active = false;
            if let Some(error) = &event.turn.error {
                return Err(io::Error::other(error.message.clone()));
            }
        }
        if let ServerNotification::ItemCompleted(event) = notification
            && !state.finished
            && let ThreadItem::AgentMessage { text, .. } = &event.item
        {
            state.result = text.clone();
        }
    }
    if let AgentServerEvent::Request { request } = &record.event {
        let response = match request.as_ref() {
            ServerRequest::DynamicToolCall { params, .. } => {
                let result = tool(host, config, bootstrap, &params.tool, &params.arguments, state).await;
                let (success, text) = match result { Ok(value) => (true, value), Err(error) => (false, error.to_string()) };
                Ok(json!({"contentItems":[{"type":"inputText","text":text}],"success":success}))
            }
            ServerRequest::CommandExecutionRequestApproval { .. }
            | ServerRequest::FileChangeRequestApproval { .. } => Ok(json!({"decision":"accept"})),
            ServerRequest::ApplyPatchApproval { .. } | ServerRequest::ExecCommandApproval { .. } => Ok(json!({"decision":"approved"})),
            ServerRequest::CurrentTimeRead { .. } => Ok(json!({"currentTimeAt":SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).map_err(io::Error::other)?.as_secs()})),
            ServerRequest::PermissionsRequestApproval { params, .. } => Ok(json!({"permissions":params.permissions,"scope":"turn"})),
            ServerRequest::ToolRequestUserInput { .. }
            | ServerRequest::McpServerElicitationRequest { .. }
            | ServerRequest::ChatgptAuthTokensRefresh { .. }
            | ServerRequest::AttestationGenerate { .. } => Err(codex_app_server_protocol::JSONRPCErrorError {
                code: -32601, message: "Use directed infra messages for task decisions; native account refresh is provided by the configured account service.".to_owned(), data: None,
            }),
        };
        host.host.events.reply(record.position, response).await?;
    }
    Ok(())
}

async fn tool(
    host: &StartedAgentHost,
    config: &super::run::RunConfig,
    bootstrap: &InboxEntry,
    name: &str,
    arguments: &Value,
    state: &mut LoopState,
) -> io::Result<String> {
    match name {
        "infra_directory" => {
            let events: Vec<codex_infra_protocol::DirectoryEvent> =
                serde_json::from_slice(&std::fs::read(&config.directory_file)?)?;
            let mut agents = std::collections::BTreeMap::new();
            for event in events {
                agents.insert(event.descriptor.agent_id, event.descriptor);
            }
            let machines: std::collections::BTreeMap<
                codex_infra_protocol::MachineId,
                std::net::SocketAddr,
            > = serde_json::from_slice(&std::fs::read(&config.machines_file)?)?;
            Ok(serde_json::to_string(
                &json!({"agents":agents.values().take(16).collect::<Vec<_>>(),"machines":machines.keys().take(16).collect::<Vec<_>>(),"profiles":config.worker_profiles.iter().take(8).map(|(name, profile)| (name, &profile.generation.inference)).collect::<std::collections::BTreeMap<_,_>>()}),
            )?)
        }
        "infra_spawn" => super::spawn::spawn(host, config, bootstrap, arguments, state).await,
        "infra_send" => {
            let to: MessageAddress = serde_json::from_value(arguments["to"].clone())?;
            let body = arguments["body"]
                .as_str()
                .ok_or_else(|| io::Error::other("body is required"))?;
            state.pending.push(message(
                host,
                bootstrap,
                to,
                MessageKind::Progress,
                body.to_owned(),
            ));
            Ok(
                "Queued for checkpoint and tmux delivery at turn end. End this turn to deliver it."
                    .to_owned(),
            )
        }
        "infra_complete" | "infra_escalate" => {
            state.result = arguments["result"]
                .as_str()
                .ok_or_else(|| io::Error::other("result is required"))?
                .to_owned();
            state.finished = true;
            state.escalation = name == "infra_escalate";
            Ok(
                "Completion recorded. End this turn for final commit/push and result delivery."
                    .to_owned(),
            )
        }
        _ => Err(io::Error::other(format!("unknown infra tool: {name}"))),
    }
}

pub fn message(
    host: &StartedAgentHost,
    bootstrap: &InboxEntry,
    to: MessageAddress,
    kind: MessageKind,
    body: String,
) -> AgentMessage {
    AgentMessage {
        message_id: MessageId::new(),
        root_session_id: host.launch.workspace.root_session_id,
        from: MessageAddress {
            agent_id: host.launch.workspace.agent_id,
            machine_id: host.launch.machine_id.clone(),
            role: host.launch.role.clone(),
        },
        to,
        repo: host.task.repo.clone(),
        commit: host.launch.initial_commit.clone(),
        task_id: host.task.task_id,
        assignment_id: bootstrap.message.assignment_id,
        kind,
        presentation: Presentation::NextTurn,
        reply_to: Some(bootstrap.message.message_id),
        body,
    }
}
