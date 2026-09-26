use super::MessageProcessor;

impl MessageProcessor {
    pub(crate) async fn drain_and_shutdown_threads(&self) -> std::io::Result<()> {
        self.models_refresh_worker.shutdown();
        if let Some(worker) = &self.turn_cost_worker {
            worker.shutdown();
        }
        self.thread_processor.drain_and_shutdown().await
    }
}
