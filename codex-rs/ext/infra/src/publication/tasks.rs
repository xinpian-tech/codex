use std::future::Future;
use std::io;
use std::sync::Arc;
use std::sync::Mutex;

use tokio::task::JoinHandle;
use tokio_util::task::TaskTracker;
use tokio_util::task::task_tracker::TaskTrackerToken;

#[derive(Default)]
pub(crate) struct PublicationTasks {
    tasks: Mutex<TaskTracker>,
    failure: Arc<Mutex<Option<String>>>,
}

impl PublicationTasks {
    pub(super) fn spawn(
        &self,
        future: impl Future<Output = io::Result<u64>> + Send + 'static,
    ) -> io::Result<JoinHandle<io::Result<u64>>> {
        let token = {
            let tasks = self
                .tasks
                .lock()
                .map_err(|error| io::Error::other(error.to_string()))?;
            if tasks.is_closed() {
                return Err(io::Error::other("publication admission is closed"));
            }
            tasks.token()
        };
        let mut job = PublicationJob {
            failure: Arc::clone(&self.failure),
            finished: false,
            _token: token,
        };
        Ok(tokio::spawn(async move {
            let result = future.await;
            if let Err(error) = &result {
                job.failure
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .get_or_insert_with(|| error.to_string());
            }
            job.finished = true;
            drop(job);
            result
        }))
    }

    pub(super) async fn shutdown(&self) -> io::Result<()> {
        let tasks = {
            let tasks = self
                .tasks
                .lock()
                .map_err(|error| io::Error::other(error.to_string()))?;
            tasks.close();
            tasks.clone()
        };
        tasks.wait().await;
        match self
            .failure
            .lock()
            .map_err(|error| io::Error::other(error.to_string()))?
            .as_ref()
        {
            Some(error) => Err(io::Error::other(error.clone())),
            None => Ok(()),
        }
    }
}

struct PublicationJob {
    failure: Arc<Mutex<Option<String>>>,
    finished: bool,
    // Fields drop after PublicationJob::drop, so even unwinding records its
    // failure before token release can wake a shutdown waiter.
    _token: TaskTrackerToken,
}

impl Drop for PublicationJob {
    fn drop(&mut self) {
        if !self.finished {
            self.failure
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get_or_insert_with(|| "publication worker ended before completion".to_owned());
        }
    }
}
