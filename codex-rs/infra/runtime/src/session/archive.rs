use std::io;
use std::path::Path;
use std::time::Duration;

use super::TransportSession;
use crate::ArchiveController;
use crate::CollectorArchiveActor;
use crate::CollectorArchiveWorker;

impl TransportSession {
    pub(crate) fn spool_directory(&self) -> &Path {
        &self.config.directory
    }

    /// Binds background collector archival to this actual transport's machine,
    /// Root Session and spool directory. The machine owner retains this actor
    /// independently of gateway reconnects and stops it before the archive actor.
    pub async fn start_collector_archiver(
        &self,
        controller: ArchiveController,
        interval: Duration,
    ) -> io::Result<CollectorArchiveActor> {
        if interval.is_zero() {
            return Err(io::Error::other(
                "collector archive interval must be positive",
            ));
        }
        let directory = self.config.directory.clone();
        let root_session_id = self.config.root_session_id;
        let machine_id = self.config.machine_id.clone();
        let worker = tokio::task::spawn_blocking(move || {
            CollectorArchiveWorker::open(&directory, root_session_id, machine_id)
        })
        .await
        .map_err(io::Error::other)??;
        CollectorArchiveActor::start(worker, controller, interval)
    }
}
