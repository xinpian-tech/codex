use codex_extension_api::ToolExecutionOrigin;

use super::UnifiedExecProcessManager;

impl UnifiedExecProcessManager {
    pub(crate) async fn execution_origin(&self, process_id: i32) -> Option<ToolExecutionOrigin> {
        let store = self.process_store.lock().await;
        let entry = store.processes.get(&process_id)?;
        let session = entry.session.upgrade()?;
        Some(ToolExecutionOrigin {
            thread_id: session.thread_id.to_string(),
            call_id: entry.call_id.clone(),
        })
    }
}
