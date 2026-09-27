use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::SystemTime;

use codex_infra_protocol::*;
use codex_infra_state::GitWorkspace;
use codex_infra_tmux::TmuxClient;
use serde::Deserialize;
use serde::Serialize;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::oneshot;

use crate::LaunchCoordinator;
use crate::LaunchIntent;
use crate::MachineLaunchConfig;
use crate::SessionController;
use crate::SessionUpdate;

/// Machine control metadata. Task bodies are delivered only through tmux.
#[derive(Clone, Serialize, Deserialize)]
pub struct SpawnAgent {
    pub agent_id: AgentId,
    pub task_id: TaskId,
    pub parent_agent_id: Option<AgentId>,
    pub role: String,
    pub repository: PathBuf,
    pub repo: String,
    pub source_commit: CommitId,
    pub remote: String,
    pub generation: ConfigGeneration,
    pub host_program: PathBuf,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LaunchServiceRequest {
    Status {
        agent_id: AgentId,
        commit: CommitId,
        status: AgentStatus,
    },
    Spawn {
        agent: Box<SpawnAgent>,
    },
    Sync {
        events: Vec<DirectoryEvent>,
    },
    Connect {
        agent_id: AgentId,
        peer: AgentId,
    },
    List,
}

#[derive(Serialize, Deserialize)]
pub struct LaunchServiceResponse {
    pub events: Vec<DirectoryEvent>,
    pub spawned: Option<AgentDescriptor>,
    pub error: Option<String>,
}

pub struct LaunchService {
    stop: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<io::Result<()>>,
    pub endpoint: SocketAddr,
}

impl LaunchService {
    pub async fn start(
        config: MachineLaunchConfig,
        controller: SessionController,
        tmux_endpoint: SocketAddr,
    ) -> io::Result<Self> {
        let listener = TcpListener::bind((config.bind_address, 0)).await?;
        let endpoint = listener.local_addr()?;
        let path = config.spool_directory.join("agent-directory.json");
        let events: Vec<DirectoryEvent> = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(error),
        };
        std::fs::write(
            config.spool_directory.join("agent-launch-endpoint.json"),
            serde_json::to_vec(&endpoint)?,
        )?;
        let (stop, mut stopping) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut events = events;
            loop {
                let (socket, _) = tokio::select! {
                    _ = &mut stopping => return Ok(()),
                    accepted = listener.accept() => accepted?,
                };
                let mut socket = BufReader::new(socket);
                let mut line = String::new();
                socket.read_line(&mut line).await?;
                let request = serde_json::from_str(&line).map_err(io::Error::other);
                let result = match request {
                    Ok(request) => {
                        handle(&config, &controller, tmux_endpoint, &mut events, request).await
                    }
                    Err(error) => Err(error),
                };
                std::fs::write(&path, serde_json::to_vec(&events)?)?;
                let response = LaunchServiceResponse {
                    spawned: result.as_ref().ok().cloned().flatten(),
                    error: result.err().map(|error| error.to_string()),
                    events: events.clone(),
                };
                socket
                    .get_mut()
                    .write_all(&serde_json::to_vec(&response)?)
                    .await?;
                socket.get_mut().write_all(b"\n").await?;
            }
        });
        Ok(Self {
            stop,
            task,
            endpoint,
        })
    }

    pub async fn stop(self) -> io::Result<()> {
        let _ = self.stop.send(());
        self.task.await.map_err(io::Error::other)?
    }
}

pub async fn launch_request(
    endpoint: SocketAddr,
    request: LaunchServiceRequest,
) -> io::Result<LaunchServiceResponse> {
    let mut socket = TcpStream::connect(endpoint).await?;
    socket.write_all(&serde_json::to_vec(&request)?).await?;
    socket.write_all(b"\n").await?;
    let mut line = String::new();
    BufReader::new(socket).read_line(&mut line).await?;
    let response: LaunchServiceResponse = serde_json::from_str(&line)?;
    if let Some(error) = &response.error {
        return Err(io::Error::other(error.clone()));
    }
    Ok(response)
}

async fn handle(
    config: &MachineLaunchConfig,
    controller: &SessionController,
    endpoint: SocketAddr,
    events: &mut Vec<DirectoryEvent>,
    request: LaunchServiceRequest,
) -> io::Result<Option<AgentDescriptor>> {
    let mut descriptor = match request {
        LaunchServiceRequest::Status {
            agent_id,
            commit,
            status,
        } => {
            let mut descriptor = events
                .iter()
                .rev()
                .find(|event| event.descriptor.agent_id == agent_id)
                .ok_or_else(|| io::Error::other("Agent missing from directory"))?
                .descriptor
                .clone();
            if descriptor.machine_id != config.machine_id {
                return Err(io::Error::other("status on owning machine"));
            }
            descriptor.revision += 1;
            descriptor.commit = commit;
            descriptor.status = status;
            descriptor
        }
        LaunchServiceRequest::List => return Ok(None),
        LaunchServiceRequest::Sync { events: incoming } => {
            for event in incoming {
                controller
                    .apply(SessionUpdate::Directory(Box::new(event.clone())))
                    .await?;
                if !events.contains(&event) {
                    events.push(event);
                }
            }
            return Ok(None);
        }
        LaunchServiceRequest::Connect { agent_id, peer } => {
            let mut descriptor = events
                .iter()
                .rev()
                .find(|event| event.descriptor.agent_id == agent_id)
                .ok_or_else(|| io::Error::other("Agent missing from directory"))?
                .descriptor
                .clone();
            if descriptor.machine_id != config.machine_id {
                return Err(io::Error::other("connect on owning machine"));
            }
            if !descriptor.sends_to.contains(&peer) {
                descriptor.sends_to.push(peer);
            }
            if !descriptor.receives_from.contains(&peer) {
                descriptor.receives_from.push(peer);
            }
            descriptor.revision += 1;
            descriptor
        }
        LaunchServiceRequest::Spawn { agent } => {
            let spawn_config = config.clone();
            let (descriptor, launch_id) = tokio::task::spawn_blocking(move || {
                let config = spawn_config;
                let fetched = std::process::Command::new(&config.programs.git)
                    .current_dir(&agent.repository)
                    .args(["fetch", &agent.remote])
                    .output()?;
                if !fetched.status.success() {
                    return Err(io::Error::other(
                        String::from_utf8_lossy(&fetched.stderr).into_owned(),
                    ));
                }
                let workspace = GitWorkspace::prepare(
                    config.programs.git.clone(),
                    &agent.repository,
                    config.root_session_id,
                    agent.agent_id,
                    &agent.source_commit,
                    agent.remote,
                )?;
                let launch_id = MessageId::new();
                let checkpoint = workspace.checkpoint(&format!("launch-{launch_id}"))?;
                let intent = LaunchIntent {
                    launch_id,
                    machine_id: config.machine_id.clone(),
                    task_id: agent.task_id,
                    role: agent.role.clone(),
                    initial_commit: checkpoint.pushed_commit.clone(),
                    workspace: workspace.binding().clone(),
                    generation: agent.generation,
                    host_program: agent.host_program,
                };
                let tmux = TmuxClient::new(
                    config.programs.tmux,
                    config.tmux_socket,
                    config.programs.tmux_config,
                    config.programs.keeper,
                );
                let mut launcher = LaunchCoordinator::open(
                    &config.spool_directory.join("launches.journal"),
                    config.spool_directory.join("bindings"),
                    config.machine_id.clone(),
                )?;
                let process = launcher.start(intent, &workspace, &checkpoint, &tmux)?;
                Ok::<_, io::Error>((
                    AgentDescriptor {
                        agent_id: agent.agent_id,
                        parent_agent_id: agent.parent_agent_id,
                        root_session_id: config.root_session_id,
                        revision: 0,
                        updated_at: 0,
                        role: agent.role.clone(),
                        responsibility: agent.role,
                        task_id: agent.task_id,
                        status: AgentStatus::Running,
                        machine_id: config.machine_id,
                        repo: agent.repo,
                        commit: checkpoint.pushed_commit,
                        tmux_session: process.placement.session,
                        tmux_window: process.placement.window_id,
                        tmux_pane: process.placement.pane_id,
                        tmux_endpoint: endpoint,
                        receives_from: agent.parent_agent_id.into_iter().collect(),
                        sends_to: agent.parent_agent_id.into_iter().collect(),
                    },
                    launch_id,
                ))
            })
            .await
            .map_err(io::Error::other)??;
            publish(config, controller, events, descriptor.clone()).await?;
            controller
                .apply(SessionUpdate::Launch {
                    agent_id: descriptor.agent_id,
                    launch_id,
                })
                .await?;
            return Ok(Some(descriptor));
        }
    };
    descriptor.updated_at = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(io::Error::other)?
        .as_secs() as i64;
    publish(config, controller, events, descriptor).await?;
    Ok(None)
}

async fn publish(
    config: &MachineLaunchConfig,
    controller: &SessionController,
    events: &mut Vec<DirectoryEvent>,
    mut descriptor: AgentDescriptor,
) -> io::Result<()> {
    descriptor.updated_at = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(io::Error::other)?
        .as_secs() as i64;
    let sequence = events
        .iter()
        .filter(|event| event.source_machine_id == config.machine_id)
        .map(|event| event.source_sequence + 1)
        .max()
        .unwrap_or(0);
    let event = DirectoryEvent {
        source_machine_id: config.machine_id.clone(),
        publisher: DirectoryPublisher::Machine(config.machine_id.clone()),
        source_sequence: sequence,
        descriptor,
    };
    controller
        .apply(SessionUpdate::Directory(Box::new(event.clone())))
        .await?;
    events.push(event);
    Ok(())
}
