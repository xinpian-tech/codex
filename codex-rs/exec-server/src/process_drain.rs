use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;

use tokio_util::task::TaskTracker;

use crate::ExecServerError;
use crate::process::ExecProcessEventLog;

/// Tracks producer tasks through their final recording callbacks.
#[derive(Clone, Default)]
pub(crate) struct ProcessDrain {
    tasks: TaskTracker,
    failure: Arc<Mutex<Option<String>>>,
}

impl ProcessDrain {
    pub(crate) fn spawn(
        &self,
        future: impl Future<Output = ()> + Send + 'static,
        events: ExecProcessEventLog,
    ) {
        let completion = Completion {
            failure: Arc::clone(&self.failure),
            events,
            finished: false,
        };
        self.tasks.spawn(async move {
            let mut completion = completion;
            future.await;
            completion.finished = true;
        });
    }

    /// Call after start request admission is closed and all starts have settled.
    pub(crate) async fn wait(&self) -> Result<(), ExecServerError> {
        self.tasks.close();
        self.tasks.wait().await;
        let failure = self
            .failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match failure.as_ref() {
            Some(message) => Err(ExecServerError::Protocol(message.clone())),
            None => Ok(()),
        }
    }
}

struct Completion {
    failure: Arc<Mutex<Option<String>>>,
    events: ExecProcessEventLog,
    finished: bool,
}

impl Drop for Completion {
    fn drop(&mut self) {
        let error = self.events.recording_failure().or_else(|| {
            (!self.finished).then(|| "process producer task ended before completion".to_owned())
        });
        if let Some(error) = error {
            self.failure
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get_or_insert(error);
        }
    }
}
