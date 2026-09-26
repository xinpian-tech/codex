use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;

use tokio::task::JoinHandle;
use tokio_util::task::TaskTracker;

use crate::ExecServerError;

/// Admission and tracking for request tasks that outlive their callers.
#[derive(Clone, Default)]
pub(crate) struct RecordingTasks {
    tracker: Arc<Mutex<TaskTracker>>,
}

impl RecordingTasks {
    pub(crate) fn spawn<F>(&self, future: F) -> Result<JoinHandle<F::Output>, ExecServerError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let tracker = self
            .tracker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if tracker.is_closed() {
            return Err(ExecServerError::Protocol(
                "recorded requests are closed".to_owned(),
            ));
        }
        // Admission and tracker registration share the close boundary.
        Ok(tracker.spawn(future))
    }

    pub(crate) async fn close_and_wait(&self) {
        let tracker = {
            let tracker = self
                .tracker
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            tracker.close();
            tracker.clone()
        };
        tracker.wait().await;
    }
}
