use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use codex_config::CloudConfigBundleLoader;
use codex_infra_account::AccountConnectionConfig;
use codex_infra_account::AccountReplicaConfig;
use codex_infra_protocol::TaskSpec;
use codex_infra_provider::ProviderAuditConfig;
use codex_infra_provider::ProviderProtocol;
use codex_infra_provider::ResolvedAccount;
use codex_infra_runtime::LaunchIntent;
use serde::Deserialize;
use serde::Serialize;

use super::AgentHostGeneration;
use super::ManagedHost;
use super::NativeAccountBootstrap;
use super::PreparedAgentHost;
use crate::AgentContext;
use crate::AgentInputSubmissions;
use crate::AgentThread;
use crate::WorkspaceCheckpoints;

/// Account control metadata supplied by the launcher, separate from the Task
/// delivered through tmux. Provider/model selection remains in the generation.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentAccountSource {
    Generation,
    Published {
        replica: Box<AccountReplicaConfig>,
        connection: AccountConnectionConfig,
    },
}

/// Running inference plus the resources needed by the terminal loop/finalizer.
pub struct StartedAgentHost {
    pub host: ManagedHost,
    pub thread: AgentThread,
    pub inputs: AgentInputSubmissions,
    pub launch: LaunchIntent,
    pub task: TaskSpec,
    pub context: Arc<AgentContext>,
    pub checkpoints: WorkspaceCheckpoints,
    pub directory: PathBuf,
    pub home: PathBuf,
    pub generation: AgentHostGeneration,
}

impl PreparedAgentHost {
    /// Resolves the selected transport, recovers pending checkpoint work, and
    /// starts inference. The terminal owner drives startup to an outcome before
    /// handling shutdown so intermediate services can finish their handoff.
    pub async fn start(self, source: AgentAccountSource) -> io::Result<StartedAgentHost> {
        let generation = self
            .generation
            .as_ref()
            .ok_or_else(|| io::Error::other("Agent startup requires a recorded generation"))?
            .clone();
        let binding = self.launch.generation.clone();
        let protocol = tokio::task::spawn_blocking(move || {
            ResolvedAccount::read_generation(&binding).map(|account| account.provider.protocol)
        })
        .await
        .map_err(io::Error::other)??;
        match (&source, protocol) {
            (AgentAccountSource::Generation, ProviderProtocol::ChatCompletions)
            | (AgentAccountSource::Published { .. }, ProviderProtocol::Responses) => {}
            (AgentAccountSource::Generation, ProviderProtocol::Responses) => {
                return Err(io::Error::other(
                    "native Agent requires a published account source",
                ));
            }
            (AgentAccountSource::Published { .. }, ProviderProtocol::ChatCompletions) => {
                return Err(io::Error::other(
                    "Chat Agent uses its generation credential snapshot",
                ));
            }
        }
        let lease = self
            .checkpoints
            .gate()
            .acquire_recovery(format!("startup:{}", self.launch.launch_id))
            .await?;
        self.checkpoints.recover(lease).await?;
        let thread_path = self.directory.join("thread.journal");
        let input_path = self.directory.join("input-submissions.journal");
        let launch = self.launch.clone();
        let (thread, inputs) = tokio::task::spawn_blocking(move || {
            let thread = AgentThread::open(&thread_path, launch.clone())?;
            let inputs = AgentInputSubmissions::open(&input_path, launch)?;
            Ok::<_, io::Error>((thread, inputs))
        })
        .await
        .map_err(io::Error::other)??;
        let local_config = self.load_config(CloudConfigBundleLoader::default()).await?;
        let host = match source {
            AgentAccountSource::Generation => {
                let args = self.start_args(local_config).await?;
                let audit = ProviderAuditConfig {
                    directory: generation.config.provider_audit_directory.clone(),
                    root_session_id: self.launch.workspace.root_session_id,
                    machine_id: self.launch.machine_id.clone(),
                    agent_id: self.launch.workspace.agent_id,
                    launch_id: self.launch.launch_id,
                };
                self.services
                    .start_with_chat_generation(args, self.processes, &self.launch, audit)
                    .await?
            }
            AgentAccountSource::Published {
                replica,
                connection,
            } => {
                let account = NativeAccountBootstrap::prepare_remote(
                    &self.launch,
                    self.home.clone(),
                    *replica,
                    connection,
                    generation.config.account_exchange_directory.clone(),
                )
                .await?;
                let configured = async {
                    let cloud = account.cloud_config_bundle(&local_config.config).await?;
                    let loaded = self.load_config(cloud).await?;
                    self.start_args(loaded).await
                }
                .await;
                let args = match configured {
                    Ok(args) => args,
                    Err(error) => {
                        return match account.stop().await {
                            Ok(()) => Err(error),
                            Err(cleanup) => Err(io::Error::other(format!(
                                "Agent configuration: {error}; account shutdown: {cleanup}"
                            ))),
                        };
                    }
                };
                self.services
                    .start_with_native_account(args, self.processes, &self.launch, account)
                    .await?
            }
        };
        Ok(StartedAgentHost {
            host,
            thread,
            inputs,
            launch: self.launch,
            task: self.task,
            context: self.context,
            checkpoints: self.checkpoints,
            directory: self.directory,
            home: self.home,
            generation,
        })
    }
}
