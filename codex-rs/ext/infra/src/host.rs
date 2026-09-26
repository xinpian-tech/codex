use std::sync::Arc;

use codex_app_server::host_services::HostServices;
use codex_app_server::in_process::InProcessClientHandle;
use codex_app_server::in_process::InProcessStartArgs;
use codex_app_server::in_process::start_with_host_services;
use codex_core::config::Config;
use codex_exec_server::EnvironmentManager;
use codex_extension_api::ExtensionRegistry;
use codex_thread_store::ThreadStore;

use crate::AgentContext;
use crate::AuditedThreadStore;
use crate::ProcessAudit;
use crate::StoreAudit;

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
}

impl ManagedHostServices {
    pub fn new(audit: StoreAudit, context: Arc<AgentContext>) -> Self {
        Self { audit, context }
    }

    /// Starts this Agent with a fresh recorded local execution environment.
    /// Remote work belongs to independently launched Agents on those machines.
    /// The supplied environment contributes runtime paths and HTTP policy only;
    /// it must not have started work for this Agent before this call.
    pub async fn start(
        self,
        mut args: InProcessStartArgs,
        process_audit: ProcessAudit,
    ) -> std::io::Result<InProcessClientHandle> {
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
                Arc::new(process_audit),
            )
            .map_err(std::io::Error::other)?,
        );
        start_with_host_services(args, Arc::new(self)).await
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
        Arc::new(builder.build())
    }
}
