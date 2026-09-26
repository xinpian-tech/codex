use std::sync::Arc;

use codex_app_server::host_services::HostServices;
use codex_thread_store::ThreadStore;

use crate::AuditedThreadStore;
use crate::StoreAudit;

// A prepared writer can be passed directly to start_with_host_services. The
// default queue and extension assembly remain owned by the embedded app-server.
impl HostServices for StoreAudit {
    fn thread_store(&self, default: Arc<dyn ThreadStore>) -> Arc<dyn ThreadStore> {
        Arc::new(AuditedThreadStore::new(default, self.clone()))
    }
}
