use super::ThreadRequestProcessor;

impl ThreadRequestProcessor {
    pub(crate) async fn drain_and_shutdown(&self) -> std::io::Result<()> {
        self.background_tasks.close();
        self.background_tasks.wait().await;
        let report = self.thread_manager.shutdown_all_threads_drained().await;
        if !report.submit_failed.is_empty() {
            return Err(std::io::Error::other(format!(
                "thread shutdown submission failed: {:?}",
                report.submit_failed
            )));
        }
        Ok(())
    }
}
