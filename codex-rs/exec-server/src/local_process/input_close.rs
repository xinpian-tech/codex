use super::LocalProcess;
use super::ProcessEntry;
use super::internal_error;
use super::invalid_params;
use super::map_handler_error;
use crate::ExecServerError;
use crate::protocol::JSONRPCErrorError;
use crate::protocol::ProcessId;
use crate::protocol::WriteResponse;
use crate::protocol::WriteStatus;

impl LocalProcess {
    pub(super) async fn close_input(
        &self,
        process_id: ProcessId,
    ) -> Result<WriteResponse, ExecServerError> {
        let Some(factory) = self.recorder_factory.clone() else {
            return self
                .close_pipe_input(&process_id)
                .await
                .map_err(map_handler_error);
        };
        let backend = self.clone();
        self.recording_tasks
            .spawn(async move {
                let recorder = factory.open_input_close(&process_id).await?;
                let outcome = backend.close_pipe_input(&process_id).await;
                recorder.finish(outcome.clone()).await?;
                outcome.map_err(map_handler_error)
            })?
            .await
            .map_err(|error| map_handler_error(internal_error(error.to_string())))?
    }

    async fn close_pipe_input(
        &self,
        process_id: &ProcessId,
    ) -> Result<WriteResponse, JSONRPCErrorError> {
        let mut processes = self.inner.processes.lock().await;
        let Some(entry) = processes.get_mut(process_id) else {
            return Ok(WriteResponse {
                status: WriteStatus::UnknownProcess,
            });
        };
        let ProcessEntry::Running(process) = entry else {
            return Ok(WriteResponse {
                status: WriteStatus::Starting,
            });
        };
        if process.tty {
            return Err(invalid_params(
                "closing stdin requires a pipe process".to_owned(),
            ));
        }
        if !process.pipe_stdin {
            return Ok(WriteResponse {
                status: WriteStatus::StdinClosed,
            });
        }
        // Stop creating writer clones under the same lock as write_input.
        // Already reserved writes retain their senders until they are queued;
        // dropping the session sender then lets the writer drain and send EOF.
        process.pipe_stdin = false;
        process.session.close_stdin();
        Ok(WriteResponse {
            status: WriteStatus::Accepted,
        })
    }
}
