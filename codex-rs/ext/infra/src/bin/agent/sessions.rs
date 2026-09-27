use std::io;
use std::num::NonZeroUsize;

use codex_infra_extension::StartedAgentHost;
use codex_infra_protocol::CommitId;
use codex_infra_protocol::MachineId;
use codex_infra_protocol::MessageAddress;
use codex_infra_protocol::MessageId;
use codex_infra_protocol::MessageKind;
use codex_infra_runtime::MachineLaunchConfig;
use codex_infra_state::ArchiveStream;
use codex_infra_state::InboxEntry;
use codex_infra_state::JournalPosition;
use codex_infra_state::JournalReader;
use codex_infra_state::SessionShard;
use serde_json::Value;
use serde_json::json;

use super::run::LoopState;

pub async fn query(
    host: &StartedAgentHost,
    bootstrap: &InboxEntry,
    arguments: &Value,
    state: &mut LoopState,
) -> io::Result<String> {
    let action = arguments["action"]
        .as_str()
        .ok_or_else(|| io::Error::other("choose list or read"))?;
    let deliver = match action {
        "list" => false,
        "read" => true,
        _ => return Err(io::Error::other("choose list or read")),
    };
    if deliver
        && (!state
            .role
            .accepted_input_kinds
            .contains(&MessageKind::WorkingContext)
            || !state
                .role
                .produced_output_kinds
                .contains(&MessageKind::WorkingContext)
            || !state.role.accepts_from_roles.contains(&state.role.role_id))
    {
        return Err(io::Error::other(
            "archive reader role must produce and receive its own WorkingContext excerpts through tmux",
        ));
    }
    let arguments = arguments.clone();
    let root = if arguments["root_session_id"].is_null() {
        host.launch.workspace.root_session_id
    } else {
        serde_json::from_value(arguments["root_session_id"].clone())?
    };
    let machine: MachineId = serde_json::from_value(arguments["machine_id"].clone())?;
    let config_path = host
        .launch
        .generation
        .config_store_path
        .join("machine-runtime.json");
    let directory = host.directory.clone();
    let response = tokio::task::spawn_blocking(move || {
        let config = MachineLaunchConfig::read(&config_path)?;
        let query_id = MessageId::new();
        let mut shard = SessionShard::open(config.programs.git, config.team_state_repository, directory.join(format!("session-query-{query_id}.index")), config.archive_remote, root, &machine)?;
        let revision: CommitId = if arguments["revision"].is_null() {
            shard.fetch_archive_head()?
        } else {
            let revision = serde_json::from_value(arguments["revision"].clone())?;
            shard.fetch_archive_revision(&revision)?;
            revision
        };
        let limit = NonZeroUsize::new(2).ok_or_else(|| io::Error::other("session page size"))?;
        if !deliver {
            let page = shard.list_streams(&revision, arguments["after"].as_str(), limit)?;
            let data: Vec<_> = page.data.into_iter().map(|head| json!({
                "stream":head.stream,"required":head.observed_durable,"archived_end":head.end,
                "prefix_available":head.end == head.observed_durable.byte_offset,
                "receipt":head.receipt
            })).collect();
            return Ok::<_, io::Error>(json!({"revision":revision,"streams":data,"next":page.next_cursor}));
        }
        let stream: ArchiveStream = serde_json::from_value(arguments["stream"].clone())?;
        let required: JournalPosition = serde_json::from_value(arguments["required"].clone())?;
        let cursor: JournalPosition = if arguments["record_cursor"].is_null() { JournalPosition::default() } else { serde_json::from_value(arguments["record_cursor"].clone())? };
        let offset = usize::try_from(arguments["payload_offset"].as_u64().unwrap_or(0)).map_err(io::Error::other)?;
        let restored = directory.join(format!("session-query-{query_id}.journal"));
        shard.restore_journal(&revision, stream.clone(), required, &restored, limit)?;
        let mut reader = JournalReader::open(&restored, cursor)?;
        let Some(record) = reader.next_record()? else {
            return Ok(json!({"revision":revision,"stream":stream,"record_cursor":cursor,"end":true}));
        };
        if offset > record.payload.len() { return Err(io::Error::other("payload_offset is outside this record")); }
        let (payload, end) = match std::str::from_utf8(&record.payload) {
            Ok(text) => {
                if !text.is_char_boundary(offset) { return Err(io::Error::other("payload_offset splits a character")); }
                let end = text.floor_char_boundary((offset + 1000).min(text.len()));
                (json!({"text":&text[offset..end]}), end)
            }
            Err(_) => {
                let end = (offset + 200).min(record.payload.len());
                (json!({"bytes":&record.payload[offset..end]}), end)
            }
        };
        let complete = end == record.payload.len();
        let response = json!({"revision":revision,"stream":stream,"sequence":record.sequence,
            "payload_offset":offset,"payload":payload,"record_cursor":if complete {reader.position()} else {cursor},
            "next_payload_offset":if complete {0} else {end},"required":required,"end":complete && reader.position() == required});
        std::fs::write(directory.join(format!("session-query-{query_id}.json")), serde_json::to_vec_pretty(&response)?)?;
        Ok(response)
    }).await.map_err(io::Error::other)??;
    if !deliver {
        return Ok(serde_json::to_string(&response)?);
    }
    let recipient = MessageAddress {
        agent_id: host.launch.workspace.agent_id,
        machine_id: host.launch.machine_id.clone(),
        role: host.launch.role.clone(),
    };
    let mut message = super::events::message(
        host,
        bootstrap,
        recipient,
        MessageKind::WorkingContext,
        serde_json::to_string(&response)?,
    );
    message.reply_to = Some(state.active_input.message_id);
    let message_id = message.message_id;
    state.pending.push(message);
    Ok(serde_json::to_string(
        &json!({"message_id":message_id,"delivery":"archive excerpt queued for tmux at turn end"}),
    )?)
}
