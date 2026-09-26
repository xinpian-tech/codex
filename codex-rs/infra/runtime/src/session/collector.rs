use std::io;

use tokio::task::JoinHandle;

use crate::CollectorFinished;
use crate::ControlCollector;

pub(super) enum CollectorOwner {
    Running(Box<ControlCollector>),
    Stopping(JoinHandle<io::Result<CollectorFinished>>),
    Stopped(CollectorFinished),
    Failed(String),
}

impl CollectorOwner {
    pub(super) fn running(&mut self) -> io::Result<&mut ControlCollector> {
        match self {
            Self::Running(collector) => Ok(collector),
            Self::Stopping(_) | Self::Stopped(_) => {
                Err(io::Error::other("transport capture is stopping or stopped"))
            }
            Self::Failed(error) => Err(io::Error::other(error.clone())),
        }
    }

    /// The join handle remains owned here across cancellation of an awaiting
    /// caller. Once stopped, repeats return the same persisted completion.
    pub(super) async fn stop(&mut self) -> io::Result<CollectorFinished> {
        if matches!(self, Self::Running(_)) {
            let Self::Running(mut collector) = std::mem::replace(
                self,
                Self::Failed("collector shutdown could not start".to_owned()),
            ) else {
                return Err(io::Error::other("collector ownership changed"));
            };
            *self = Self::Stopping(tokio::task::spawn_blocking(move || {
                let directory = collector.directory().to_path_buf();
                let attachment_id = collector.attachment_id();
                let detached = collector.detach();
                let finished = collector.finish();
                // Still wait for child/capture completion when detach reports
                // an error; finish persists its own producer completion record.
                finished?;
                detached?;
                CollectorFinished::read(&directory, attachment_id)?
                    .ok_or_else(|| io::Error::other("collector completion record missing"))
            }));
        }
        if let Self::Stopping(task) = self {
            let result = task
                .await
                .map_err(io::Error::other)
                .and_then(|result| result);
            *self = match result {
                Ok(finished) => Self::Stopped(finished),
                Err(error) => Self::Failed(error.to_string()),
            };
        }
        match self {
            Self::Stopped(finished) => Ok(finished.clone()),
            Self::Failed(error) => Err(io::Error::other(error.clone())),
            Self::Running(_) | Self::Stopping(_) => {
                Err(io::Error::other("collector shutdown is not complete"))
            }
        }
    }
}
