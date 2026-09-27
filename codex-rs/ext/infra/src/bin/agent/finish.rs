use std::io;
use std::num::NonZeroUsize;
use std::time::Duration;

use codex_infra_extension::*;
use codex_infra_protocol::AgentMessage;
use codex_infra_protocol::MessageId;
use codex_infra_protocol::MessageKind;
use codex_infra_runtime::*;
use codex_infra_state::CheckpointKind;
use codex_infra_state::CheckpointPhase;
use codex_infra_state::InboxEntry;
use codex_infra_state::SessionShard;

use super::run::LoopState;

pub async fn publish(
    host: &StartedAgentHost,
    terminal: &TerminalMailbox,
    message: AgentMessage,
) -> io::Result<()> {
    let lease = host
        .checkpoints
        .gate()
        .acquire(format!("send-{}", message.message_id))
        .await?;
    let record = host
        .checkpoints
        .checkpoint(lease, CheckpointKind::Mutation)
        .await?;
    let CheckpointPhase::Completed { receipt, .. } = record.phase else {
        return Err(io::Error::other("checkpoint pending"));
    };
    terminal
        .with_mailbox(move |mailbox| mailbox.send_checkpointed(message, &receipt))
        .await?;
    Ok(())
}

pub async fn finish(
    host: StartedAgentHost,
    terminal: TerminalMailbox,
    bootstrap: AgentHostBootstrap,
    entry: &InboxEntry,
    result: io::Result<LoopState>,
    run_config: &super::run::RunConfig,
) -> io::Result<()> {
    let receipts = host.directory.join("archive-receipts");
    std::fs::create_dir_all(&receipts)?;
    let mut jobs = vec![
        bootstrap.binding_archive_job(&receipts, MessageId::new())?,
        host.preparation_archive_job(&receipts, MessageId::new())?,
    ];
    let drivers = host
        .close_input_drivers(
            receipts.clone(),
            AgentDriverArchiveJobIds {
                thread: MessageId::new(),
                inputs: MessageId::new(),
            },
        )
        .await?;
    jobs.extend([drivers.thread, drivers.inputs]);
    let outcome = match &result {
        Ok(state) if state.finished => state.result.clone(),
        Ok(_) => "Agent stopped before task completion".to_owned(),
        Err(error) => format!("Agent execution failed: {error}"),
    };
    let kind = if result
        .as_ref()
        .is_ok_and(|state| state.finished && !state.escalation)
    {
        MessageKind::Result
    } else {
        MessageKind::Escalation
    };
    let message = super::events::message(&host, entry, entry.message.from.clone(), kind, outcome);
    let native_account =
        codex_infra_provider::ResolvedAccount::read_generation(&host.launch.generation)?
            .provider
            .protocol
            == codex_infra_provider::ProviderProtocol::Responses;
    let host_jobs = host
        .host
        .shutdown_and_snapshot(
            receipts.clone(),
            HostArchiveJobIds {
                processes: MessageId::new(),
                tools: MessageId::new(),
                thread_store: MessageId::new(),
                account: native_account.then(MessageId::new),
                rpc: Some(MessageId::new()),
                model_inputs: Some(MessageId::new()),
                server_events: Some(MessageId::new()),
            },
        )
        .await?;
    jobs.extend([host_jobs.processes, host_jobs.tools, host_jobs.thread_store]);
    jobs.extend(host_jobs.account);
    jobs.extend(host_jobs.rpc);
    jobs.extend(host_jobs.model_inputs);
    jobs.extend(host_jobs.server_events);
    let lease = host
        .checkpoints
        .gate()
        .acquire(format!("final-{}", host.launch.launch_id))
        .await?;
    let checkpoint = host
        .checkpoints
        .checkpoint(lease, CheckpointKind::Finalization)
        .await?;
    let CheckpointPhase::Completed { receipt, .. } = checkpoint.phase else {
        return Err(io::Error::other("final checkpoint pending"));
    };
    let message_id = entry.message.message_id;
    let outcome_ref = receipt.pushed_commit.to_string();
    let final_commit = receipt.pushed_commit.clone();
    terminal
        .with_mailbox(move |mailbox| {
            mailbox.send_checkpointed(message, &receipt)?;
            if mailbox
                .input(message_id)
                .is_some_and(|entry| entry.presented.is_some())
            {
                mailbox.confirm_processed(message_id, outcome_ref)?;
            }
            Ok(())
        })
        .await?;
    let mut exit = terminal.stop().await?;
    jobs.push(exit.input_lifecycle_archive_job(&receipts, MessageId::new())?);
    exit.restore_terminal()?;
    let mailbox = exit.mailbox.finish_archive(
        host.launch.machine_id.clone(),
        host.launch.launch_id,
        &receipts,
        MailboxArchiveJobIds {
            stdin: MessageId::new(),
            stdout: MessageId::new(),
            inbox: MessageId::new(),
            outbox: MessageId::new(),
            publications: MessageId::new(),
            bootstrap: Some(MessageId::new()),
        },
    )?;
    jobs.extend(mailbox.jobs);
    jobs.extend(mailbox.bootstrap);
    let snapshot_home = host.home.clone();
    let snapshot_generation = host.launch.generation.config_store_path.clone();
    let snapshot_directory = host.directory.clone();
    let snapshot_launch = host.launch.clone();
    let snapshot_receipts = receipts.clone();
    jobs.push(
        tokio::task::spawn_blocking(move || {
            super::assets::capture(
                snapshot_home,
                snapshot_generation,
                snapshot_directory,
                snapshot_launch,
                snapshot_receipts,
            )
        })
        .await
        .map_err(io::Error::other)??,
    );
    let manifest = host.directory.join("agent-archive-jobs.json");
    std::fs::write(&manifest, serde_json::to_vec_pretty(&jobs)?)?;
    let config = MachineLaunchConfig::read(
        &host
            .launch
            .generation
            .config_store_path
            .join("machine-runtime.json"),
    )?;
    let archive_directory = host.directory.join("archive");
    std::fs::create_dir_all(&archive_directory)?;
    let launch = host.launch;
    let writer = tokio::task::spawn_blocking(move || {
        let shard = SessionShard::open(
            config.programs.git,
            config.team_state_repository,
            archive_directory.join("git-index"),
            config.archive_remote,
            launch.workspace.root_session_id,
            &launch.machine_id,
        )?;
        MachineArchiveWriter::open(
            shard,
            &archive_directory.join("jobs.journal"),
            config.scheduling.archive_chunk_bytes,
        )
    })
    .await
    .map_err(io::Error::other)??;
    let actor = ArchiveActor::start(
        writer,
        Duration::from_millis(100),
        NonZeroUsize::new(32).ok_or_else(|| io::Error::other("archive capacity"))?,
    )?;
    let controller = actor.controller();
    for job in &jobs {
        controller.enqueue(job.clone()).await?;
    }
    for job in jobs {
        while controller.completion(job.job_id).await?.is_none() {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
    actor.stop().await?;
    let machines: std::collections::BTreeMap<
        codex_infra_protocol::MachineId,
        std::net::SocketAddr,
    > = serde_json::from_slice(&std::fs::read(&run_config.machines_file)?)?;
    let local = *machines
        .get(&bootstrap.launch.machine_id)
        .ok_or_else(|| io::Error::other("local machine endpoint missing"))?;
    let status = if result
        .as_ref()
        .is_ok_and(|state| state.finished && !state.escalation)
    {
        codex_infra_protocol::AgentStatus::Completed
    } else {
        codex_infra_protocol::AgentStatus::Failed
    };
    let update = launch_request(
        local,
        LaunchServiceRequest::Status {
            agent_id: bootstrap.launch.workspace.agent_id,
            commit: final_commit,
            status,
        },
    )
    .await?;
    if let Some(parent) = machines.get(&entry.message.from.machine_id)
        && *parent != local
    {
        launch_request(
            *parent,
            LaunchServiceRequest::Sync {
                events: update.events,
            },
        )
        .await?;
    }
    result.map(|_| ())
}
