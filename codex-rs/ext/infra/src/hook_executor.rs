use std::collections::HashMap;
use std::ffi::OsString;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use codex_exec_server::ExecBackend;
use codex_exec_server_protocol::ExecEnvPolicy;
use codex_exec_server_protocol::ExecMetadata;
use codex_exec_server_protocol::ExecParams;
use codex_exec_server_protocol::WriteStatus;
use codex_extension_api::ToolCallSource;
use codex_extension_api::ToolExecutionInput;
use codex_extension_api::ToolExecutionKind;
use codex_extension_api::ToolExecutionOrigin;
use codex_extension_api::ToolLifecycleContributor;
use codex_extension_api::ToolName;
use codex_extension_api::ToolPayload;
use codex_hooks::HookCommandArgument;
use codex_hooks::HookCommandExecutor;
use codex_hooks::HookCommandFuture;
use codex_hooks::HookCommandOutput;
use codex_hooks::HookCommandRequest;
use codex_infra_protocol::MessageId;
use codex_protocol::config_types::ShellEnvironmentPolicyInherit;
use codex_utils_path_uri::PathUri;
use serde::Deserialize;
use tokio_util::task::TaskTracker;

use crate::ProcessAudit;
use crate::RecordedProcessOutput;
use crate::RecordedToolPayload;
use crate::ToolAudit;
use crate::ToolAuditEvent;
use crate::ToolOperation;
use crate::ToolOrigin;
use crate::ToolOutcome;

#[derive(Deserialize)]
struct Attribution {
    turn_id: Option<String>,
}

/// Local managed hook execution using the same backend, process audit and
/// tool controller as its host. Call shutdown after stopping hook dispatch.
#[derive(Clone)]
pub struct RecordedHookExecutor {
    backend: Arc<dyn ExecBackend>,
    processes: ProcessAudit,
    tools: Arc<ToolAudit>,
    tasks: Arc<Mutex<TaskTracker>>,
    failure: Arc<Mutex<Option<String>>>,
}

impl RecordedHookExecutor {
    pub fn new(
        backend: Arc<dyn ExecBackend>,
        processes: ProcessAudit,
        tools: Arc<ToolAudit>,
    ) -> Self {
        Self {
            backend,
            processes,
            tools,
            tasks: Arc::new(Mutex::new(TaskTracker::new())),
            failure: Arc::default(),
        }
    }

    pub async fn shutdown(&self) -> Result<(), String> {
        let tasks = {
            let tasks = self.tasks.lock().map_err(|error| error.to_string())?;
            tasks.close();
            tasks.clone()
        };
        tasks.wait().await;
        match self
            .failure
            .lock()
            .map_err(|error| error.to_string())?
            .as_ref()
        {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }

    async fn execute(
        &self,
        request: HookCommandRequest,
        operation: ToolOperation,
        origin: Option<ToolExecutionOrigin>,
    ) -> Result<HookCommandOutput, String> {
        let body = String::from_utf8(request.stdin.clone()).map_err(|error| error.to_string())?;
        let payload = ToolPayload::Custom {
            input: body.clone(),
        };
        let tool_name = ToolName::plain("hook_command");
        let lease = self
            .tools
            .acquire_tool_execution(ToolExecutionInput {
                thread_id: &operation.thread_id,
                turn_id: &operation.turn_id,
                call_id: &operation.call_id,
                tool_name: &tool_name,
                source: &ToolCallSource::Direct,
                kind: if origin.is_some() {
                    ToolExecutionKind::ExistingProcess
                } else {
                    ToolExecutionKind::Operation
                },
                origin: origin.as_ref(),
                payload: &payload,
            })
            .await;
        let result = match &lease {
            Ok(_) => {
                match self
                    .tools
                    .append(Ok(ToolAuditEvent::Started {
                        operation: operation.clone(),
                        root_turn_id: None,
                        originating_item_id: None,
                        payload: RecordedToolPayload::Custom { input: body },
                    }))
                    .await
                {
                    Ok(()) => self.run(request, &operation).await,
                    Err(error) => Err(error),
                }
            }
            Err(error) => Err(error.clone()),
        };
        let outcome = match &result {
            Ok(output) => ToolOutcome::Completed {
                success: output.exit_code == Some(0),
            },
            Err(_) => ToolOutcome::Failed {
                handler_executed: lease.is_ok(),
            },
        };
        self.tools
            .append(Ok(ToolAuditEvent::Finished { operation, outcome }))
            .await?;
        drop(lease);
        result
    }

    async fn run(
        &self,
        request: HookCommandRequest,
        operation: &ToolOperation,
    ) -> Result<HookCommandOutput, String> {
        let mut argv = vec![native_string(request.program)?];
        argv.extend(
            request
                .arguments
                .into_iter()
                .map(native_string)
                .collect::<Result<Vec<_>, _>>()?,
        );
        match request.command {
            HookCommandArgument::Argument(command) => argv.push(command),
            HookCommandArgument::WindowsRaw(_) => {
                return Err("managed hook execution requires an argument-based shell".to_owned());
            }
        }
        let env = request
            .environment
            .into_iter()
            .map(|(key, value)| Ok((native_string(key)?, native_string(value)?)))
            .collect::<Result<HashMap<_, _>, String>>()?;
        let process_id: codex_exec_server::ProcessId = format!("hook-{}", MessageId::new()).into();
        let audit = self.processes.clone();
        let reader_id = process_id.clone();
        let reader = tokio::task::spawn_blocking(move || audit.output_reader(reader_id))
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
        let collected = Arc::new(Mutex::new(CollectedOutput {
            reader,
            stdout: Vec::new(),
            stderr: Vec::new(),
        }));
        let deadline = tokio::time::Instant::now() + request.timeout;
        // Await the owned startup response even past the deadline so a created
        // process always has a handle for termination and output draining.
        let started = self
            .backend
            .start(ExecParams {
                process_id,
                metadata: Some(ExecMetadata {
                    thread_id: Some(request.thread_id),
                    tool_call_id: Some(operation.call_id.clone()),
                }),
                argv,
                cwd: PathUri::from_host_native_path(request.cwd)
                    .map_err(|error| error.to_string())?,
                env_policy: Some(ExecEnvPolicy {
                    inherit: ShellEnvironmentPolicyInherit::None,
                    ignore_default_excludes: true,
                    exclude: Vec::new(),
                    r#set: HashMap::new(),
                    include_only: Vec::new(),
                }),
                shell_snapshot: None,
                env,
                tty: false,
                pipe_stdin: true,
                arg0: None,
                sandbox: None,
                enforce_managed_network: false,
                managed_network: None,
                network_proxy: None,
            })
            .await
            .map_err(|error| error.to_string())?;
        let process = started.process;
        let result = tokio::time::timeout_at(deadline, async {
            let written = process
                .write(request.stdin)
                .await
                .map_err(|error| error.to_string())?;
            if !matches!(
                written.status,
                WriteStatus::Accepted | WriteStatus::StdinClosed
            ) {
                return Err(format!("hook stdin write: {:?}", written.status));
            }
            let closed = process
                .close_stdin()
                .await
                .map_err(|error| error.to_string())?;
            if !matches!(
                closed.status,
                WriteStatus::Accepted | WriteStatus::StdinClosed
            ) {
                return Err(format!("hook stdin close: {:?}", closed.status));
            }
            collect(Arc::clone(&collected)).await
        })
        .await
        .map_err(|_| "hook command timed out".to_owned())
        .and_then(|result| result);
        if result.is_err() {
            process
                .terminate()
                .await
                .map_err(|error| error.to_string())?;
            // The producer owns the raw transcript even if interpretation or
            // timeout failed. Drain a healthy reader before releasing the hook.
            let _ = collect(Arc::clone(&collected)).await;
        }
        let exit_code = result?;
        let mut collected = collected.lock().map_err(|error| error.to_string())?;
        Ok(HookCommandOutput {
            exit_code: Some(exit_code),
            stdout: std::mem::take(&mut collected.stdout),
            stderr: std::mem::take(&mut collected.stderr),
        })
    }
}

impl HookCommandExecutor for RecordedHookExecutor {
    fn prepare(&self, request: HookCommandRequest) -> HookCommandFuture {
        let prepared = (|| {
            let admission = {
                let tasks = self.tasks.lock().map_err(|error| error.to_string())?;
                if tasks.is_closed() {
                    return Err("hook executor is closed".to_owned());
                }
                tasks.token()
            };
            let attribution: Attribution =
                serde_json::from_slice(&request.stdin).map_err(|error| error.to_string())?;
            let call_id = format!("hook-{}", MessageId::new());
            let origin = request
                .tool_call_id
                .clone()
                .map(|call_id| ToolExecutionOrigin {
                    thread_id: request.thread_id.to_string(),
                    call_id,
                });
            let reservation = self
                .tools
                .workspace()
                .map_err(|error| error.to_string())?
                .as_ref()
                .and_then(|workspace| {
                    origin
                        .as_ref()
                        .map(|origin| workspace.operations().continue_operation(origin))
                })
                .transpose()
                .map_err(|error| error.to_string())?
                .flatten();
            let operation = ToolOperation {
                thread_id: request.thread_id.to_string(),
                turn_id: attribution.turn_id.unwrap_or_else(|| call_id.clone()),
                call_id,
                tool_name: "hook_command".to_owned(),
                source: ToolOrigin::Direct,
            };
            Ok::<_, String>((operation, origin, reservation, admission))
        })();
        let executor = self.clone();
        Box::pin(async move {
            let (operation, origin, reservation, admission) = prepared?;
            let task = {
                let worker = executor.clone();
                let mut job = HookJob {
                    failure: Arc::clone(&executor.failure),
                    finished: false,
                };
                // A prepared hook is already admitted, including while queued
                // in the hook runtime. Closing admission must not reject it.
                tokio::spawn(async move {
                    let result = worker.execute(request, operation, origin).await;
                    if let Err(error) = &result {
                        job.failure
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .get_or_insert_with(|| error.clone());
                    }
                    drop(reservation);
                    job.finished = true;
                    drop(job);
                    drop(admission);
                    result
                })
            };
            task.await.map_err(|error| error.to_string())?
        })
    }
}

struct HookJob {
    failure: Arc<Mutex<Option<String>>>,
    finished: bool,
}

impl Drop for HookJob {
    fn drop(&mut self) {
        if !self.finished {
            self.failure
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get_or_insert_with(|| "hook worker ended before completion".to_owned());
        }
    }
}

struct CollectedOutput {
    reader: RecordedProcessOutput,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

async fn collect(output: Arc<Mutex<CollectedOutput>>) -> Result<i32, String> {
    loop {
        let shared = Arc::clone(&output);
        let exit_code = tokio::task::spawn_blocking(move || {
            let mut state = shared.lock().map_err(|error| error.to_string())?;
            let CollectedOutput {
                reader,
                stdout,
                stderr,
            } = &mut *state;
            reader
                .drain_into(stdout, stderr)
                .map_err(|error| error.to_string())
        })
        .await
        .map_err(|error| error.to_string())??;
        if let Some(exit_code) = exit_code {
            return Ok(exit_code);
        }
        tokio::time::sleep(Duration::from_millis(/*millis*/ 20)).await;
    }
}

fn native_string(value: OsString) -> Result<String, String> {
    value
        .into_string()
        .map_err(|_| "hook command contains non-UTF-8 native data".to_owned())
}
