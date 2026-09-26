use std::sync::Arc;

use codex_app_server::host_services::HostServices;
use codex_core::config::Config;
use codex_extension_api::ExtensionRegistry;
use codex_thread_store::ThreadStore;

use crate::AgentContext;
use crate::AuditedThreadStore;
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
