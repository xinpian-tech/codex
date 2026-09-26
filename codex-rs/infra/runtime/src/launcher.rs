use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use codex_infra_protocol::CommitId;
use codex_infra_protocol::ConfigGeneration;
use codex_infra_protocol::MachineId;
use codex_infra_protocol::MessageId;
use codex_infra_protocol::TaskId;
use codex_infra_state::Checkpoint;
use codex_infra_state::GitWorkspace;
use codex_infra_state::Journal;
use codex_infra_state::WorkspaceBinding;
use codex_infra_tmux::AgentLaunch;
use codex_infra_tmux::PaneProcess;
use codex_infra_tmux::PaneProcessState;
use codex_infra_tmux::TmuxClient;
use serde::Deserialize;
use serde::Serialize;

/// Immutable host binding. Task prose and Bootstrap are delivered separately
/// through the parent and child panes after transport readiness.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LaunchIntent {
    pub launch_id: MessageId,
    pub machine_id: MachineId,
    pub task_id: TaskId,
    pub role: String,
    pub initial_commit: CommitId,
    pub workspace: WorkspaceBinding,
    pub generation: ConfigGeneration,
    pub host_program: PathBuf,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum LaunchEvent {
    Intended {
        intent: Box<LaunchIntent>,
    },
    Started {
        launch_id: MessageId,
        process: PaneProcess,
    },
    Failed {
        launch_id: MessageId,
        error: String,
    },
}

/// The machine serializes launches for each Agent. Intent is durable before
/// file materialization or tmux creation; retries reconcile the same launch name.
pub struct LaunchCoordinator {
    machine_id: MachineId,
    bindings: PathBuf,
    journal: Journal,
    intents: BTreeMap<MessageId, LaunchIntent>,
}

impl LaunchCoordinator {
    pub fn open(journal_path: &Path, bindings: PathBuf, machine_id: MachineId) -> io::Result<Self> {
        fs::create_dir_all(&bindings)?;
        let mut intents = BTreeMap::new();
        let journal = Journal::open(journal_path, |record| {
            match serde_json::from_slice(&record.payload).map_err(io::Error::other)? {
                LaunchEvent::Intended { intent } => {
                    if intent.machine_id != machine_id || intents.contains_key(&intent.launch_id) {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "launch intent ownership or identity mismatch",
                        ));
                    }
                    intents.insert(intent.launch_id, *intent);
                }
                LaunchEvent::Started { launch_id, .. } | LaunchEvent::Failed { launch_id, .. } => {
                    if !intents.contains_key(&launch_id) {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "launch outcome without intent",
                        ));
                    }
                }
            }
            Ok(())
        })?;
        Ok(Self {
            machine_id,
            bindings: bindings.canonicalize()?,
            journal,
            intents,
        })
    }

    pub fn intent(&self, launch_id: MessageId) -> Option<&LaunchIntent> {
        self.intents.get(&launch_id)
    }

    /// Workspace preparation and its initial commit/push checkpoint precede this
    /// operation. Generation fixes the Provider/account/model before process start.
    pub fn start(
        &mut self,
        intent: LaunchIntent,
        workspace: &GitWorkspace,
        checkpoint: &Checkpoint,
        tmux: &TmuxClient,
    ) -> io::Result<PaneProcess> {
        let expected_parent = Path::new("/tmp/codex")
            .join(intent.workspace.root_session_id.to_string())
            .join(intent.workspace.agent_id.to_string());
        if intent.machine_id != self.machine_id
            || &intent.workspace != workspace.binding()
            || intent.role.is_empty()
            || intent.initial_commit != checkpoint.pushed_commit
            || !intent.workspace.worktree.starts_with(expected_parent)
            || !intent.host_program.starts_with("/nix/store")
            || !intent
                .generation
                .config_store_path
                .starts_with("/nix/store")
            || workspace.current_commit()? != checkpoint.pushed_commit
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "launch differs from its machine, Nix generation or checkpointed worktree",
            ));
        }
        let inference = &intent.generation.inference;
        if inference.provider_id.is_empty()
            || inference.account_id.is_empty()
            || inference.model_id.is_empty()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "launch requires an explicit Provider, account and model",
            ));
        }
        match self.intents.get(&intent.launch_id) {
            Some(previous) if previous != &intent => {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "launch ID already has another binding",
                ));
            }
            Some(_) => {}
            None => {
                self.journal.append(
                    &serde_json::to_vec(&LaunchEvent::Intended {
                        intent: Box::new(intent.clone()),
                    })
                    .map_err(io::Error::other)?,
                )?;
                self.intents.insert(intent.launch_id, intent.clone());
            }
        }
        let path = self.bindings.join(format!("{}.json", intent.launch_id));
        write_binding(&path, &intent)?;
        let session = tmux.ensure_session(intent.workspace.root_session_id)?;
        let launched = tmux
            .create_agent(
                &session,
                &AgentLaunch {
                    agent_id: intent.workspace.agent_id,
                    launch_id: intent.launch_id,
                    worktree: intent.workspace.worktree.clone(),
                    host_program: intent.host_program.clone(),
                    host_args: vec![
                        OsString::from("agent"),
                        OsString::from("--binding"),
                        path.into_os_string(),
                    ],
                },
            )
            .and_then(|_| {
                tmux.inspect_launch(&session, intent.workspace.agent_id, intent.launch_id)?
                    .filter(|process| process.state == PaneProcessState::Alive)
                    .ok_or_else(|| io::Error::other("Agent host exited during launch"))
            });
        let event = match &launched {
            Ok(process) => LaunchEvent::Started {
                launch_id: intent.launch_id,
                process: process.clone(),
            },
            Err(error) => LaunchEvent::Failed {
                launch_id: intent.launch_id,
                error: error.to_string(),
            },
        };
        self.journal
            .append(&serde_json::to_vec(&event).map_err(io::Error::other)?)?;
        launched
    }
}

fn write_binding(path: &Path, intent: &LaunchIntent) -> io::Result<()> {
    match fs::read(path) {
        Ok(bytes) => {
            let previous: LaunchIntent =
                serde_json::from_slice(&bytes).map_err(io::Error::other)?;
            return if previous == *intent {
                Ok(())
            } else {
                Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "host binding file differs from launch intent",
                ))
            };
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let temporary = path.with_extension(format!("{}.pending", MessageId::new()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    file.write_all(&serde_json::to_vec(intent).map_err(io::Error::other)?)?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    #[cfg(unix)]
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}
