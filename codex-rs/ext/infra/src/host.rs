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
}

/// The initialized app-server and its recorded execution lifecycle boundary.
/// Finalization must settle child processes and output separately from draining
/// start/stdin requests; the embedded client's shutdown alone does not do this.
pub struct ManagedHost {
    pub client: InProcessClientHandle,
    exec_backend: Arc<dyn ExecBackend>,
    tools: Arc<ToolAudit>,
    processes: ProcessAudit,
    hooks: Arc<RecordedHookExecutor>,
}

impl ManagedHost {
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
        let client = start_with_host_services(args, Arc::new(self)).await?;
        Ok(ManagedHost {
            client,
            exec_backend,
            tools,
            processes: process_audit,
            hooks,
        })
    }
}

impl HostServices for ManagedHostServices {
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
        }
        Arc::new(builder.build())
    }
}
