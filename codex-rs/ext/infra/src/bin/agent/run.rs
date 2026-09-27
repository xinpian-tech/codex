use std::collections::BTreeSet;
use std::io;
use std::num::NonZeroU32;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::Duration;

use codex_infra_extension::*;
use codex_infra_protocol::AgentMessage;
use codex_infra_protocol::MessageId;
use codex_infra_runtime::TerminalInputState;
use codex_infra_runtime::TerminalMailbox;
use codex_infra_state::InboxEntry;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct RunConfig {
    pub account: AgentAccountSource,
    pub directory_file: PathBuf,
    pub machines_file: PathBuf,
    #[serde(default)]
    pub worker_profiles: std::collections::BTreeMap<String, super::spawn::WorkerProfile>,
    #[serde(default)]
    pub worker_profiles_file: Option<PathBuf>,
    #[serde(default)]
    pub initial_task: Option<codex_infra_protocol::TaskSpec>,
}

pub struct LoopState {
    pub thread_id: String,
    pub active: bool,
    pub finished: bool,
    pub result: String,
    pub escalation: bool,
    pub pending: Vec<AgentMessage>,
    pub accepted: BTreeSet<MessageId>,
    pub deliveries: Vec<AgentInputDelivery>,
}

pub async fn run(binding: PathBuf) -> io::Result<()> {
    let bootstrap = tokio::task::spawn_blocking(move || AgentHostBootstrap::read(&binding))
        .await
        .map_err(io::Error::other)??;
    let config_path = bootstrap
        .launch
        .generation
        .config_store_path
        .join("agent-run.json");
    let mut config: RunConfig = serde_json::from_slice(&std::fs::read(config_path)?)?;
    if let Some(path) = &config.worker_profiles_file {
        let bytes = std::fs::read(path)?;
        std::fs::write(bootstrap.directory.join("worker-profiles.json"), &bytes)?;
        config.worker_profiles.extend(serde_json::from_slice::<
            std::collections::BTreeMap<String, super::spawn::WorkerProfile>,
        >(&bytes)?);
    }
    let directory = bootstrap.mailbox_directory.clone();
    let launch = bootstrap.launch.clone();
    let terminal = tokio::task::spawn_blocking(move || TerminalMailbox::open(&directory, &launch))
        .await
        .map_err(io::Error::other)??;
    if let Some(task) = &config.initial_task {
        let launch = bootstrap.launch.clone();
        let git = bootstrap.generation.config.preparation.git.clone();
        let checkpoint = tokio::task::spawn_blocking(move || {
            codex_infra_state::GitWorkspace::resume(git, launch.workspace)?
                .checkpoint("leader-task-input")
        })
        .await
        .map_err(io::Error::other)??;
        let launch = &bootstrap.launch;
        let address = codex_infra_protocol::MessageAddress {
            agent_id: launch.workspace.agent_id,
            machine_id: launch.machine_id.clone(),
            role: launch.role.clone(),
        };
        let message = AgentMessage {
            message_id: MessageId::new(),
            root_session_id: launch.workspace.root_session_id,
            from: address.clone(),
            to: address,
            repo: task.repo.clone(),
            commit: checkpoint.pushed_commit.clone(),
            task_id: task.task_id,
            assignment_id: codex_infra_protocol::AssignmentId::new(),
            kind: codex_infra_protocol::MessageKind::Bootstrap,
            presentation: codex_infra_protocol::Presentation::NextTurn,
            reply_to: None,
            body: serde_json::to_string(task)?,
        };
        terminal
            .with_mailbox(move |mailbox| mailbox.send_checkpointed(message, &checkpoint))
            .await?;
    }
    let prepared = bootstrap.prepare(&terminal).await;
    let (prepared, entry) = match prepared {
        Ok(value) => value,
        Err(error) => {
            terminal.stop().await?.restore_terminal()?;
            return Err(error);
        }
    };
    let host = match prepared.start(config.account.clone()).await {
        Ok(host) => host,
        Err(error) => {
            terminal.stop().await?.restore_terminal()?;
            return Err(error);
        }
    };
    let result = drive(&host, &terminal, &entry, &config).await;
    super::finish::finish(host, terminal, bootstrap, &entry, result, &config).await
}

async fn drive(
    host: &StartedAgentHost,
    terminal: &TerminalMailbox,
    bootstrap: &InboxEntry,
    config: &RunConfig,
) -> io::Result<LoopState> {
    let client = host.host.request_client();
    let thread_id = match host
        .thread
        .ensure(
            &client,
            NonZeroU32::new(32).ok_or_else(|| io::Error::other("thread page size"))?,
        )
        .await?
    {
        AgentThreadOutcome::Ready { thread_id } => thread_id,
        AgentThreadOutcome::Uncertain { .. } => {
            return Err(io::Error::other("thread creation needs recovery"));
        }
    };
    let mut state = LoopState {
        thread_id,
        active: false,
        finished: false,
        result: String::new(),
        escalation: false,
        pending: Vec::new(),
        accepted: BTreeSet::new(),
        deliveries: Vec::new(),
    };
    let cursor = host.host.events.open_cursor().await?;
    let batch = host.generation.config.input_batch;
    let mut input_cursor = 0;
    submit(host, terminal, bootstrap, &mut state).await?;
    let mut timer = tokio::time::interval(Duration::from_millis(100));
    let mut signals = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    loop {
        tokio::select! {
            _ = timer.tick() => {},
            result = tokio::signal::ctrl_c() => { result?; break; },
            _ = signals.recv() => break,
        }
        let page = cursor.read_page(&host.host.events, batch).await?;
        for record in &page.records {
            super::events::handle(host, config, bootstrap, record, &mut state).await?;
        }
        cursor.acknowledge(&page).await?;
        for index in (0..state.deliveries.len()).rev() {
            if matches!(
                state.deliveries[index]
                    .advance(&client, terminal, batch)
                    .await?,
                AgentInputDeliveryProgress::Confirmed { .. }
            ) {
                state.deliveries.swap_remove(index);
            }
        }
        if !state.active {
            for message in std::mem::take(&mut state.pending) {
                super::finish::publish(host, terminal, message).await?;
            }
            if state.finished {
                if state.deliveries.is_empty() {
                    break;
                }
                continue;
            }
            let entries = terminal
                .with_mailbox(move |mailbox| {
                    Ok(mailbox
                        .pending_inputs(input_cursor, batch)
                        .cloned()
                        .collect::<Vec<_>>())
                })
                .await?;
            if let Some(entry) = entries.iter().find(|entry| {
                !state.accepted.contains(&entry.message.message_id)
                    && entry.message.presentation != codex_infra_protocol::Presentation::Archive
            }) {
                input_cursor = entry.accepted_sequence + 1;
                submit(host, terminal, entry, &mut state).await?;
            } else if let Some(entry) = entries.last() {
                input_cursor = entry.accepted_sequence + 1;
            }
        }
        if let TerminalInputState::Stopped { error } = &*terminal.subscribe().borrow() {
            return Err(io::Error::other(format!("tmux input stopped: {error:?}")));
        }
    }
    cursor.close().await?;
    Ok(state)
}

async fn submit(
    host: &StartedAgentHost,
    terminal: &TerminalMailbox,
    entry: &InboxEntry,
    state: &mut LoopState,
) -> io::Result<()> {
    let client = host.host.request_client();
    let mut delivery = host
        .inputs
        .track_delivery(
            entry,
            state.thread_id.clone(),
            NonZeroUsize::new(256).ok_or_else(|| io::Error::other("evidence capacity"))?,
        )
        .await?;
    // Restore recorded presentation before deciding whether to submit a turn.
    loop {
        match delivery
            .advance(&client, terminal, host.generation.config.input_batch)
            .await?
        {
            AgentInputDeliveryProgress::Scanning => continue,
            AgentInputDeliveryProgress::Confirmed { .. } => {
                state.accepted.insert(entry.message.message_id);
                return Ok(());
            }
            AgentInputDeliveryProgress::Waiting => break,
        }
    }
    let (submission, outcome) = host
        .inputs
        .inject(&client, entry, state.thread_id.clone())
        .await?;
    match outcome {
        AgentRpcOutcome::Reply(Ok(_)) => {}
        AgentRpcOutcome::Reply(Err(error)) => return Err(io::Error::other(error.message)),
        AgentRpcOutcome::Uncertain { .. } => {
            return Err(io::Error::other("input injection needs recovery"));
        }
    }
    match host.inputs.start_turn(&client, &submission).await? {
        AgentInputTurnOutcome::Associated { .. } => state.active = true,
        AgentInputTurnOutcome::Rejected { error } => return Err(io::Error::other(error.message)),
        AgentInputTurnOutcome::Uncertain { .. } => {
            return Err(io::Error::other("turn start needs recovery"));
        }
    }
    state.accepted.insert(entry.message.message_id);
    state.deliveries.push(delivery);
    Ok(())
}
