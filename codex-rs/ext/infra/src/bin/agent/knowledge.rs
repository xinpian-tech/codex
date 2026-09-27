use std::io;
use std::path::Path;

use codex_infra_extension::StartedAgentHost;
use codex_infra_protocol::*;
use codex_infra_state::InboxEntry;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use serde_json::json;

use super::run::LoopState;
use super::run::RunConfig;

#[derive(Clone, Serialize, Deserialize)]
pub struct KnowledgeCandidate {
    pub reviewer: AgentId,
    pub sources: Vec<KnowledgeSource>,
    pub files: Vec<KnowledgeFile>,
    pub confirmation: Option<KnowledgeConfirmation>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct KnowledgeSource {
    root_session_id: RootSessionId,
    agent_id: AgentId,
    session_commit: CommitId,
    event: String,
    repo: String,
    repo_commit: CommitId,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct KnowledgeFile {
    candidate: String,
    published: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct KnowledgeConfirmation {
    reviewer: AgentId,
    source_message: MessageId,
    candidate_commit: CommitId,
    accepted: bool,
}

pub async fn handle(
    host: &StartedAgentHost,
    config: &RunConfig,
    bootstrap: &InboxEntry,
    arguments: &Value,
    state: &mut LoopState,
) -> io::Result<String> {
    match arguments["action"].as_str() {
        Some("propose") => {
            let candidate = KnowledgeCandidate {
                reviewer: serde_json::from_value(arguments["reviewer"].clone())?,
                sources: serde_json::from_value(arguments["sources"].clone())?,
                files: serde_json::from_value(arguments["files"].clone())?,
                confirmation: None,
            };
            if candidate.sources.is_empty() || candidate.files.is_empty() {
                return Err(io::Error::other(
                    "knowledge requires source references and candidate files",
                ));
            }
            if serde_json::to_vec(&candidate)?.len() > 1600 {
                return Err(io::Error::other(
                    "split knowledge proposals into smaller contributions",
                ));
            }
            for file in &candidate.files {
                let path = relative(&file.candidate)?;
                relative(&file.published)?;
                std::fs::metadata(host.launch.workspace.worktree.join(path))?;
            }
            let mut proposal = arguments.clone();
            proposal["knowledge"] = serde_json::to_value(&candidate)?;
            proposal["recipient"] = serde_json::to_value(candidate.reviewer)?;
            super::contributions::handle(
                host,
                config,
                bootstrap,
                "infra_contribute",
                &proposal,
                state,
            )
            .await
        }
        Some("confirm") => {
            if state.active_input.kind != MessageKind::Contribution {
                return Err(io::Error::other("select the knowledge Contribution input"));
            }
            let mut body: Value = serde_json::from_str(&state.active_input.body)?;
            let contribution: Contribution = serde_json::from_value(body["contribution"].clone())?;
            let mut candidate: KnowledgeCandidate =
                serde_json::from_value(body["knowledge"].clone())?;
            if candidate.reviewer != host.launch.workspace.agent_id {
                return Err(io::Error::other(
                    "this candidate is assigned to another reviewer",
                ));
            }
            let accepted = arguments["accepted"]
                .as_bool()
                .ok_or_else(|| io::Error::other("accepted is required"))?;
            candidate.confirmation = Some(KnowledgeConfirmation {
                reviewer: host.launch.workspace.agent_id,
                source_message: state.active_input.message_id,
                candidate_commit: contribution.proposal.head_commit.clone(),
                accepted,
            });
            body["knowledge"] = serde_json::to_value(&candidate)?;
            let recipient_id = if accepted {
                contribution.proposal.integration_owner
            } else {
                contribution.proposal.author_agent_id
            };
            let directory = super::directory::read(&config.directory_file)?;
            let recipient = directory
                .get(&recipient_id)
                .ok_or_else(|| io::Error::other("knowledge recipient missing from directory"))?;
            if !state.role.routes_to_roles.contains(&recipient.role)
                || !state
                    .role
                    .produced_output_kinds
                    .contains(&MessageKind::Contribution)
            {
                return Err(io::Error::other(
                    "knowledge confirmation needs a Contribution role route",
                ));
            }
            let body = serde_json::to_string(&body)?;
            std::fs::write(
                host.directory.join(format!(
                    "knowledge-confirmation-{}.json",
                    contribution.proposal.contribution_id
                )),
                body.as_bytes(),
            )?;
            let mut message = super::events::message(
                host,
                bootstrap,
                MessageAddress {
                    agent_id: recipient.agent_id,
                    machine_id: recipient.machine_id.clone(),
                    role: recipient.role.clone(),
                },
                MessageKind::Contribution,
                body,
            );
            message.task_id = contribution.proposal.task_id;
            message.assignment_id = contribution.proposal.assignment_id;
            message.reply_to = Some(state.active_input.message_id);
            state.pending.push(message);
            Ok(json!({"accepted":accepted,"delivery":"queued for tmux at turn end"}).to_string())
        }
        _ => Err(io::Error::other("choose propose or confirm")),
    }
}

pub fn promote(
    candidate: &KnowledgeCandidate,
    contribution: &Contribution,
    worktree: &Path,
) -> io::Result<()> {
    let confirmation = candidate
        .confirmation
        .as_ref()
        .ok_or_else(|| io::Error::other("knowledge candidate awaits team confirmation"))?;
    if !confirmation.accepted
        || confirmation.reviewer != candidate.reviewer
        || confirmation.candidate_commit != contribution.proposal.head_commit
    {
        return Err(io::Error::other(
            "confirm this candidate revision before publishing",
        ));
    }
    for file in &candidate.files {
        let source = worktree.join(relative(&file.candidate)?);
        let target = worktree.join(relative(&file.published)?);
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::copy(source, target)?;
    }
    let directory = worktree.join("knowledge/publications");
    std::fs::create_dir_all(&directory)?;
    std::fs::write(
        directory.join(format!("{}.json", contribution.proposal.contribution_id)),
        serde_json::to_vec_pretty(&json!({"contribution":contribution,"knowledge":candidate}))?,
    )?;
    Ok(())
}

fn relative(value: &str) -> io::Result<&Path> {
    let path = Path::new(value);
    if value.is_empty()
        || path.components().any(|part| {
            !matches!(
                part,
                std::path::Component::Normal(_) | std::path::Component::CurDir
            )
        })
    {
        return Err(io::Error::other(
            "knowledge file paths must be relative to the worktree",
        ));
    }
    Ok(path)
}
