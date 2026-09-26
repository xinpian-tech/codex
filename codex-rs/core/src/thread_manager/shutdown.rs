use std::sync::Arc;

use futures::StreamExt;
use futures::stream::FuturesUnordered;

use super::ThreadManager;
use super::ThreadShutdownReport;

impl ThreadManager {
    /// Waits for actual thread shutdown without a timeout. The caller stops
    /// thread creation first. Failed submissions remain tracked for recovery.
    pub async fn shutdown_all_threads_drained(&self) -> ThreadShutdownReport {
        let mut pending = {
            let threads = self.state.threads.read().await;
            threads
                .iter()
                .map(|(id, thread)| {
                    let id = *id;
                    let thread = Arc::clone(thread);
                    async move {
                        let result = thread.shutdown_and_wait().await;
                        (id, thread, result)
                    }
                })
                .collect::<FuturesUnordered<_>>()
        };
        let mut report = ThreadShutdownReport::default();
        while let Some((id, thread, result)) = pending.next().await {
            match result {
                Ok(()) => {
                    self.remove_thread_if_matches(&id, &thread).await;
                    report.completed.push(id);
                }
                Err(_) => report.submit_failed.push(id),
            }
        }
        report.completed.sort_by_key(ToString::to_string);
        report.submit_failed.sort_by_key(ToString::to_string);
        report
    }
}
