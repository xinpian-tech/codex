use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;

use codex_infra_protocol::MachineId;
use codex_infra_runtime::LaunchIntent;
use codex_infra_runtime::TerminalMailbox;
use codex_infra_state::InboxEntry;
use codex_infra_state::Journal;
use serde::Deserialize;
use serde::Serialize;

use super::AgentHostGeneration;
use super::PreparedAgentHost;

/// Launcher metadata resolved before entering the Agent's raw tmux terminal.
/// The owner opens TerminalMailbox at mailbox_directory and retains it across
/// preparation failures so terminal input can still be stopped and archived.
pub struct AgentHostBootstrap {
    pub launch: LaunchIntent,
    pub generation: AgentHostGeneration,
    pub directory: PathBuf,
    pub mailbox_directory: PathBuf,
}

#[derive(PartialEq, Eq, Serialize, Deserialize)]
struct LaunchBinding {
    launch: LaunchIntent,
    generation: AgentHostGeneration,
    binding_bytes: Vec<u8>,
}

impl AgentHostBootstrap {
    /// Run on a blocking worker. Persists the exact launcher file and selected
    /// generation before terminal readiness; semantic input arrives separately.
    pub fn read(binding_path: &Path) -> io::Result<Self> {
        let binding_bytes = fs::read(binding_path)?;
        let launch: LaunchIntent = serde_json::from_slice(&binding_bytes)?;
        let generation = AgentHostGeneration::read(&launch.generation)?;
        let output = Command::new(&generation.config.preparation.hostid).output()?;
        if !output.status.success() {
            return Err(io::Error::other(format!(
                "hostid failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }
        let actual: MachineId = std::str::from_utf8(&output.stdout)
            .map_err(io::Error::other)?
            .trim()
            .parse()
            .map_err(io::Error::other)?;
        if actual != launch.machine_id {
            return Err(io::Error::other("Agent binding belongs to another hostid"));
        }
        let directory = generation
            .config
            .preparation
            .spool_directory
            .join(launch.workspace.root_session_id.to_string())
            .join(launch.workspace.agent_id.to_string())
            .join(launch.launch_id.to_string());
        fs::create_dir_all(&directory)?;
        let directory = directory.canonicalize()?;
        let binding = LaunchBinding {
            launch,
            generation,
            binding_bytes,
        };
        let mut recorded = false;
        let mut journal = Journal::open(&directory.join("launch-binding.journal"), |record| {
            let previous: LaunchBinding = serde_json::from_slice(&record.payload)?;
            if previous != binding {
                return Err(io::Error::other("recorded Agent launch binding changed"));
            }
            recorded = true;
            Ok(())
        })?;
        if !recorded {
            journal.append(&serde_json::to_vec(&binding)?)?;
        }
        Ok(Self {
            launch: binding.launch,
            generation: binding.generation,
            mailbox_directory: directory.join("mailbox"),
            directory,
        })
    }

    /// Waits for the terminal's durable bootstrap, then prepares the same launch
    /// and returns its full inbox entry for begin_bootstrap. The caller retains
    /// terminal ownership throughout this operation and any returned error.
    pub async fn prepare(
        &self,
        terminal: &TerminalMailbox,
    ) -> io::Result<(PreparedAgentHost, InboxEntry)> {
        let entry = terminal
            .wait_bootstrap_entry(self.launch.clone(), self.generation.config.input_batch)
            .await?;
        let prepared =
            PreparedAgentHost::open_generation(self.launch.clone(), entry.message.clone()).await?;
        Ok((prepared, entry))
    }
}
