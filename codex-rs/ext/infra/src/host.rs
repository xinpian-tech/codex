use std::sync::Arc;

use codex_app_server::host_services::HostServices;
use codex_app_server::in_process::InProcessClientHandle;
use codex_app_server::in_process::InProcessStartArgs;
use codex_app_server::in_process::start_with_host_services;
use codex_core::config::Config;
use codex_exec_server::EnvironmentManager;
use codex_exec_server::ExecBackend;
use codex_extension_api::ExtensionRegistry;
use codex_thread_store::ThreadStore;

use crate::AgentContext;
use crate::AuditedThreadStore;
use crate::ProcessAudit;
use crate::RecordedHookExecutor;
use crate::StoreAudit;
use crate::ToolAudit;

mod account;
mod archive;
mod config;
mod driver_archive;
mod generation;
mod home;
mod preparation;
mod provider;
mod shutdown;
mod shutdown_journal;
mod start_args;
mod startup;
pub use account::NativeAccountBootstrap;
pub use archive::HostArchiveJobIds;
pub use archive::HostArchiveJobs;
pub use archive::HostArchivePhase;
pub use archive::HostArchiveReceipts;
pub use config::AgentLoadedConfig;
pub use driver_archive::AgentDriverArchiveJobIds;
pub use driver_archive::AgentDriverArchiveJobs;
pub use driver_archive::AgentDriverArchiveReceipts;
pub use generation::AgentHostConfig;
pub use generation::AgentHostGeneration;
pub use generation::AgentHostPrograms;
pub use preparation::AgentPreparationConfig;
pub use preparation::PreparedAgentHost;
pub use shutdown_journal::HostShutdownJournal;
pub use shutdown_journal::HostShutdownPlan;
pub use shutdown_journal::HostShutdownStatus;
pub use startup::AgentAccountSource;
pub use startup::StartedAgentHost;

// A prepared writer can be passed directly to start_with_host_services. The
// default queue and extension assembly remain owned by the embedded app-server.
impl HostServices for StoreAudit {
    fn thread_store(&self, default: Arc<dyn ThreadStore>) -> Arc<dyn ThreadStore> {
        Arc::new(AuditedThreadStore::new(default, self.clone()))
    }
}

/// Combines prepared per-Agent persistence and current execution context.
pub struct ManagedHostServices {
    audit: StoreAudit,
    context: Arc<AgentContext>,
    tools: Arc<ToolAudit>,
    hooks: Option<Arc<RecordedHookExecutor>>,
    external_auth: Option<Arc<dyn codex_login::ExternalAuth>>,
    launch_binding: Option<Arc<codex_infra_state::Journal>>,
    home: Option<std::path::PathBuf>,
}

/// The initialized app-server and its recorded execution lifecycle boundary.
/// Finalization must settle child processes and output separately from draining
/// start/stdin requests; the embedded client's shutdown alone does not do this.
pub struct ManagedHost {
    pub client: InProcessClientHandle,
    pub rpc: crate::AgentRpc,
    exec_backend: Arc<dyn ExecBackend>,
    tools: Arc<ToolAudit>,
    processes: ProcessAudit,
    hooks: Arc<RecordedHookExecutor>,
    store_audit: StoreAudit,
    provider: Option<codex_infra_provider::ChatFrontend>,
    account_replica: Option<codex_infra_account::AccountRuntimeControl>,
    account_observation: Option<codex_infra_account::AccountObservation>,
    _launch_binding: Option<Arc<codex_infra_state::Journal>>,
}

/// Completion boundaries for the host's currently recorded audit streams.
/// These do not cover provider transport, terminal, mailbox, or machine logs.
pub struct HostAuditPositions {
    pub processes: codex_infra_state::JournalPosition,
    pub tools: codex_infra_state::JournalPosition,
    pub thread_store: codex_infra_state::JournalPosition,
}

impl ManagedHost {
    /// Reads bounded raw history evidence while inference and store writes
    /// continue. This does not acknowledge mailbox presentation by itself.
    pub async fn read_store_audit(
        &self,
        cursor: codex_infra_state::JournalPosition,
        limit: std::num::NonZeroUsize,
    ) -> std::io::Result<crate::StoreAuditPage> {
        self.store_audit.read_page(cursor, limit).await
    }

    /// Sample after dispatch is quiescent, process/hook output has drained and
    /// store operations have finished. Keep those producers quiescent while
    /// archiving the returned positions. This does not itself close the host.
    pub fn settled_audit_positions(&self) -> std::io::Result<HostAuditPositions> {
        Ok(HostAuditPositions {
            processes: self.processes.settled_position()?,
            tools: self.tools.settled_position()?,
            thread_store: self.store_audit.settled_position()?,
        })
    }

    /// Waits for recorded requests and child output producers before checkpoint.
    /// A long-running child must finish or be terminated by the task owner.
    /// The caller first quiesces tool and hook dispatch; this checks tool audit health
    /// but does not itself stop non-process tools or infer their completion.
    pub async fn drain_recorded_processes(&self) -> std::io::Result<()> {
        // Hooks can still submit stdin and EOF requests while their workers
        // drain. Keep backend request admission open until those workers end.
        let hook_result = self.hooks.shutdown().await.map_err(std::io::Error::other);
        self.exec_backend
            .drain_recorded_processes()
            .await
            .map_err(std::io::Error::other)?;
        self.processes.check_health()?;
        self.tools.check_health()?;
        if let Some(workspace) = self.tools.workspace()? {
            workspace.drain().await?;
        }
        hook_result
    }

    /// Stops admitting recorded starts/stdin and waits for existing requests.
    pub async fn close_recorded_requests(&self) -> std::io::Result<()> {
        let hook_result = self.hooks.shutdown().await.map_err(std::io::Error::other);
        let request_result = self
            .exec_backend
            .close_recorded_requests()
            .await
            .map_err(std::io::Error::other);
        hook_result.and(request_result)
    }
}

impl ManagedHostServices {
    pub fn new(audit: StoreAudit, context: Arc<AgentContext>, tools: Arc<ToolAudit>) -> Self {
        Self {
            audit,
            context,
            tools,
            hooks: None,
            external_auth: None,
            launch_binding: None,
            home: None,
        }
    }

    /// Starts this Agent with a fresh recorded local execution environment.
    /// Remote work belongs to independently launched Agents on those machines.
    /// The supplied environment contributes runtime paths and HTTP policy only;
    /// it must not have started work for this Agent before this call.
    pub async fn start(
        mut self,
        mut args: InProcessStartArgs,
        process_audit: ProcessAudit,
    ) -> std::io::Result<ManagedHost> {
        if let Some(home) = &self.home
            && args.config.codex_home.as_path() != home
        {
            return Err(std::io::Error::other("host config uses another Agent home"));
        }
        self.context.validate_audit_identity(&self.audit.identity)?;
        if self.tools.identity != self.audit.identity
            || process_audit.identity != self.audit.identity
            || self.tools.launch_id != process_audit.launch_id
        {
            return Err(std::io::Error::other(
                "host audit streams have different launch bindings",
            ));
        }
        let process_audit = match self.tools.workspace()? {
            Some(workspace) => process_audit.with_operations(workspace.operations()),
            None => process_audit,
        };
        let local = args
            .environment_manager
            .try_local_environment()
            .ok_or_else(|| std::io::Error::other("managed host requires local execution paths"))?;
        let runtime_paths = local
            .local_runtime_paths()
            .cloned()
            .ok_or_else(|| std::io::Error::other("managed host requires executor runtime paths"))?;
        args.environment_manager = Arc::new(
            EnvironmentManager::recorded_local(
                runtime_paths,
                args.environment_manager.http_client_factory().clone(),
                Arc::new(process_audit.clone()),
            )
            .map_err(std::io::Error::other)?,
        );
        let exec_backend = args
            .environment_manager
            .try_local_environment()
            .ok_or_else(|| std::io::Error::other("recorded local environment unavailable"))?
            .get_exec_backend();
        let tools = Arc::clone(&self.tools);
        let hooks = Arc::new(RecordedHookExecutor::new(
            Arc::clone(&exec_backend),
            process_audit.clone(),
            Arc::clone(&tools),
        ));
        self.hooks = Some(Arc::clone(&hooks));
        let store_audit = self.audit.clone();
        let launch_binding = self.launch_binding.clone();
        let rpc_path = self.audit.path.with_file_name("rpc.journal");
        let identity = self.audit.identity.clone();
        let launch_id = self.tools.launch_id;
        let rpc = tokio::task::spawn_blocking(move || {
            crate::AgentRpc::open(&rpc_path, identity, launch_id)
        })
        .await
        .map_err(std::io::Error::other)??;
        let client = start_with_host_services(args, Arc::new(self)).await?;
        Ok(ManagedHost {
            client,
            rpc,
            exec_backend,
            tools,
            processes: process_audit,
            hooks,
            store_audit,
            provider: None,
            account_replica: None,
            account_observation: None,
            _launch_binding: launch_binding,
        })
    }
}

impl HostServices for ManagedHostServices {
    fn memory_generation(&self) -> codex_app_server::host_services::MemoryGeneration {
        codex_app_server::host_services::MemoryGeneration::ManagedTasks
    }

    fn external_auth(&self) -> Option<Arc<dyn codex_login::ExternalAuth>> {
        self.external_auth.clone()
    }

    fn thread_store(&self, default: Arc<dyn ThreadStore>) -> Arc<dyn ThreadStore> {
        self.audit.thread_store(default)
    }

    fn extensions(
        &self,
        default: Arc<ExtensionRegistry<Config>>,
    ) -> Arc<ExtensionRegistry<Config>> {
        let mut builder = default.to_builder();
        builder.prompt_contributor(self.context.clone());
        builder.tool_lifecycle_contributor(self.tools.clone());
        if let Some(hooks) = &self.hooks {
            builder.hook_command_executor(hooks.clone());
            builder.managed_hook_mcp_executor(hooks.clone());
        }
        Arc::new(builder.build())
    }
}
