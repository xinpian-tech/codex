use std::io;
use std::process::Command;

use codex_infra_extension::StartedAgentHost;
use codex_infra_protocol::*;
use codex_infra_runtime::TerminalMailbox;
use codex_infra_state::CheckpointKind;
use codex_infra_state::CheckpointPhase;
use codex_infra_state::InboxEntry;
use serde_json::Value;
use serde_json::json;

use super::run::LoopState;
use super::run::RunConfig;

pub struct PendingIntegration {
    contribution: Contribution,
    recipient: MessageAddress,
    reply_to: MessageId,
    knowledge: Option<super::knowledge::KnowledgeCandidate>,
}

pub async fn handle(
    host: &StartedAgentHost,
    config: &RunConfig,
    bootstrap: &InboxEntry,
    name: &str,
    arguments: &Value,
    state: &mut LoopState,
) -> io::Result<String> {
    if !state
        .role
        .produced_output_kinds
        .contains(&MessageKind::Contribution)
    {
        return Err(io::Error::other(
            "Contribution is not an output of this role",
        ));
    }
    if name == "infra_contribute" {
        let owner: AgentId = serde_json::from_value(arguments["owner"].clone())?;
        let recipient_id = if arguments["recipient"].is_null() {
            owner
        } else {
            serde_json::from_value(arguments["recipient"].clone())?
        };
        let directory = super::directory::read(&config.directory_file)?;
        let recipient = directory
            .get(&recipient_id)
            .ok_or_else(|| io::Error::other("integration owner missing from directory"))?;
        if !state.role.routes_to_roles.contains(&recipient.role) {
            return Err(io::Error::other(
                "integration owner role is not a route for this role",
            ));
        }
        let output = Command::new(&host.generation.config.preparation.git)
            .current_dir(&host.launch.workspace.worktree)
            .args(["rev-parse", "HEAD"])
            .output()?;
        if !output.status.success() {
            return Err(io::Error::other(
                String::from_utf8_lossy(&output.stderr).into_owned(),
            ));
        }
        let head_commit = String::from_utf8(output.stdout)
            .map_err(io::Error::other)?
            .trim()
            .parse()
            .map_err(io::Error::other)?;
        let contribution = Contribution::ready(ContributionProposal {
            contribution_id: ContributionId::new(),
            task_id: host.task.task_id,
            assignment_id: bootstrap.message.assignment_id,
            author_agent_id: host.launch.workspace.agent_id,
            repo: host.task.repo.clone(),
            base_commit: host.launch.initial_commit.clone(),
            head_commit,
            dependencies: serde_json::from_value(arguments["dependencies"].clone())?,
            target_branch: arguments["target_branch"]
                .as_str()
                .ok_or_else(|| io::Error::other("target_branch required"))?
                .to_owned(),
            integration_owner: owner,
        });
        let body = serde_json::to_string(
            &json!({"contribution":contribution,"summary":arguments["summary"],"knowledge":arguments["knowledge"]}),
        )?;
        std::fs::write(
            host.directory.join(format!(
                "contribution-{}.json",
                contribution.proposal.contribution_id
            )),
            body.as_bytes(),
        )?;
        state.pending.push(super::events::message(
            host,
            bootstrap,
            MessageAddress {
                agent_id: recipient_id,
                machine_id: recipient.machine_id.clone(),
                role: recipient.role.clone(),
            },
            MessageKind::Contribution,
            body,
        ));
        return Ok(serde_json::to_string(
            &json!({"contribution_id":contribution.proposal.contribution_id,"delivery":"queued for tmux at turn end"}),
        )?);
    }
    if state.active_input.kind != MessageKind::Contribution {
        return Err(io::Error::other(
            "select a received Contribution input before integrating",
        ));
    }
    let body: Value = serde_json::from_str(&state.active_input.body)?;
    let contribution: Contribution = serde_json::from_value(body["contribution"].clone())?;
    let knowledge: Option<super::knowledge::KnowledgeCandidate> =
        serde_json::from_value(body["knowledge"].clone())?;
    if contribution.proposal.integration_owner != host.launch.workspace.agent_id
        || contribution.proposal.repo != host.task.repo
    {
        return Err(io::Error::other(
            "integrate in the assigned owner's target repository",
        ));
    }
    if !matches!(contribution.status(), ContributionStatus::Ready) {
        return Err(io::Error::other("contribution already integrated"));
    }
    let git = host.generation.config.preparation.git.clone();
    let worktree = host.launch.workspace.worktree.clone();
    let remote = host.launch.workspace.remote.clone();
    let proposal = contribution.proposal.clone();
    let mode = arguments["mode"]
        .as_str()
        .ok_or_else(|| io::Error::other("integration mode required"))?
        .to_owned();
    tokio::task::spawn_blocking(move || {
        if mode == "cherry_pick" {
            for args in [
                vec!["fetch".to_owned(), remote],
                vec![
                    "cherry-pick".to_owned(),
                    format!("{}..{}", proposal.base_commit, proposal.head_commit),
                ],
            ] {
                let output = Command::new(&git)
                    .current_dir(&worktree)
                    .args(args)
                    .output()?;
                if !output.status.success() {
                    return Err(io::Error::other(
                        String::from_utf8_lossy(&output.stderr).into_owned(),
                    ));
                }
            }
        } else if mode != "complete" {
            return Err(io::Error::other("choose cherry_pick or complete"));
        }
        Ok::<_, io::Error>(())
    })
    .await
    .map_err(io::Error::other)??;
    if let Some(candidate) = &knowledge {
        super::knowledge::promote(candidate, &contribution, &host.launch.workspace.worktree)?;
    }
    state.integrations.push(PendingIntegration {
        contribution,
        recipient: state.active_input.from.clone(),
        reply_to: state.active_input.message_id,
        knowledge,
    });
    Ok(
        "Integration staged. End this turn for target-branch push and integrated notification."
            .to_owned(),
    )
}

pub async fn publish_integrations(
    host: &StartedAgentHost,
    config: &RunConfig,
    terminal: &TerminalMailbox,
    bootstrap: &InboxEntry,
    state: &mut LoopState,
) -> io::Result<()> {
    for PendingIntegration {
        mut contribution,
        recipient,
        reply_to,
        knowledge,
    } in std::mem::take(&mut state.integrations)
    {
        let id = contribution.proposal.contribution_id;
        let lease = host
            .checkpoints
            .gate()
            .acquire(format!("integrate-{id}"))
            .await?;
        let record = host
            .checkpoints
            .checkpoint(lease, CheckpointKind::Mutation)
            .await?;
        let CheckpointPhase::Completed { receipt, .. } = record.phase else {
            return Err(io::Error::other("integration checkpoint pending"));
        };
        let git = host.generation.config.preparation.git.clone();
        let worktree = host.launch.workspace.worktree.clone();
        let remote = host.launch.workspace.remote.clone();
        let refspec = format!(
            "{}:refs/heads/{}",
            receipt.pushed_commit, contribution.proposal.target_branch
        );
        tokio::task::spawn_blocking(move || {
            let output = Command::new(git)
                .current_dir(worktree)
                .args(["push", &remote, &refspec])
                .output()?;
            if output.status.success() {
                Ok(())
            } else {
                Err(io::Error::other(
                    String::from_utf8_lossy(&output.stderr).into_owned(),
                ))
            }
        })
        .await
        .map_err(io::Error::other)??;
        contribution
            .integrate(host.launch.workspace.agent_id, receipt.pushed_commit)
            .map_err(io::Error::other)?;
        super::tasks::request(
            config,
            &host.launch.machine_id,
            codex_infra_runtime::TaskCommand::Integrated {
                contribution_id: id,
            },
        )
        .await?;
        let body =
            serde_json::to_string(&json!({"contribution":contribution,"knowledge":knowledge}))?;
        state.tasks_changed = true;
        std::fs::write(
            host.directory.join(format!("integration-{id}.json")),
            body.as_bytes(),
        )?;
        let mut message =
            super::events::message(host, bootstrap, recipient, MessageKind::Contribution, body);
        message.task_id = contribution.proposal.task_id;
        message.assignment_id = contribution.proposal.assignment_id;
        message.reply_to = Some(reply_to);
        super::finish::publish(host, terminal, message).await?;
    }
    Ok(())
}
