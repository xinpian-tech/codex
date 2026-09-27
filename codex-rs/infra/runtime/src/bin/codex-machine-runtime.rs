use std::io;
use std::path::PathBuf;

use codex_infra_protocol::AgentId;
use codex_infra_protocol::DirectoryEvent;
use codex_infra_protocol::MachineId;
use codex_infra_protocol::MessageId;
use codex_infra_protocol::RootSessionId;
use codex_infra_runtime::MachineArchiveWriter;
use codex_infra_runtime::MachineLaunchConfig;
use codex_infra_runtime::MachineRuntime;
use codex_infra_runtime::MachineRuntimeExit;
use codex_infra_runtime::SessionUpdate;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::mpsc;

#[path = "machine_runtime/audit.rs"]
mod audit;
use audit::ControlAudit;
use audit::InputAudit;

#[path = "machine_runtime/archive.rs"]
mod archive;
use archive::archive_control;
use archive::replay_control_archives;

#[path = "machine_runtime/input_worker.rs"]
mod input_worker;
use input_worker::InputWorker;

#[path = "machine_runtime/recovery.rs"]
mod recovery;

#[path = "machine_runtime/signals.rs"]
mod signals;
use signals::ShutdownSignals;
use signals::StopReason;

#[derive(Deserialize)]
struct Request {
    id: String,
    command: ControlCommand,
}

/// Machine control metadata only. Agent messages use the tmux I/O transport.
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ControlCommand {
    AccountDirectory {
        update: codex_infra_account::AccountDirectoryUpdate,
    },
    Directory {
        event: Box<DirectoryEvent>,
    },
    Launch {
        agent_id: AgentId,
        launch_id: MessageId,
    },
    AgentExited {
        agent_id: AgentId,
    },
    Stop,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Output {
    Ready {
        control_run_id: MessageId,
        root_session_id: RootSessionId,
        machine_id: MachineId,
        endpoint: std::net::SocketAddr,
        accounts: Vec<codex_infra_account::AccountDirectoryUpdate>,
        account_directory_archive: codex_infra_state::ArchiveStream,
    },
    Response {
        id: String,
        error: Option<String>,
    },
    Stopped {
        account_updates: Vec<codex_infra_account::AccountDirectoryUpdate>,
        reason: StopReason,
        control_error: Option<String>,
        failures: Vec<String>,
    },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> io::Result<()> {
    let mut args = std::env::args_os().skip(1);
    let path = args
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| io::Error::other("usage: codex-machine-runtime <machine-runtime.json>"))?;
    if args.next().is_some() {
        return Err(io::Error::other(
            "usage: codex-machine-runtime <machine-runtime.json>",
        ));
    }
    let (config, provenance) = MachineLaunchConfig::read_generation(&path)?;
    let (mut audit, input_audit) = ControlAudit::open(&config, &provenance)?;
    let mut signals = match ShutdownSignals::open() {
        Ok(signals) => signals,
        Err(error) => {
            return fail_startup(
                "signal_setup_failed",
                error,
                audit,
                input_audit,
                /*writer*/ None,
            )
            .await;
        }
    };
    let capacity = config.scheduling.command_capacity.get();
    let root_session_id = config.root_session_id;
    let machine_id = config.machine_id.clone();
    let control_directory = config.spool_directory.join("machine-control");
    let mut machine = match config.open().await {
        Ok(machine) => machine,
        Err(error) => {
            return fail_startup(
                "open_failed",
                error,
                audit,
                input_audit,
                /*writer*/ None,
            )
            .await;
        }
    };
    if let Err(error) = machine.start().await {
        let (error, writer) = match stop_machine(machine, &mut audit).await {
            Ok(exit) => (
                io::Error::other(format!(
                    "machine startup: {error}; cleanup: {:?}",
                    exit.failures
                )),
                exit.writer,
            ),
            Err(cleanup) => (
                io::Error::other(format!("machine startup: {error}; cleanup: {cleanup}")),
                None,
            ),
        };
        return fail_startup("startup_failed", error, audit, input_audit, writer).await;
    }
    let (send, mut receive) = mpsc::channel(capacity);
    let input_worker = InputWorker::start(input_audit, send);
    let result = async {
        input_worker
            .as_ref()
            .map_err(|failure| io::Error::other(failure.error.to_string()))?;
        replay_control_archives(
            control_directory,
            machine.archive_controller()?,
            root_session_id,
            machine_id.clone(),
        )
        .await?;
        audit.emit(Output::Ready {
            control_run_id: audit.run_id,
            root_session_id,
            machine_id,
            endpoint: machine.endpoint(),
            accounts: machine.account_updates(),
            account_directory_archive: machine.account_archive_stream()?,
        })?;
        let controller = machine.controller()?;
        loop {
            let request = tokio::select! {
                reason = signals.receive() => return reason,
                request = receive.recv() => match request {
                    Some(request) => request,
                    None => return Ok(StopReason::StdinClosed),
                },
            };
            let Request { id, command } = request?;
            let update = match command {
                ControlCommand::AccountDirectory { update } => {
                    let error = machine
                        .account_directory()?
                        .apply(update)
                        .await
                        .err()
                        .map(|error| error.to_string());
                    audit.emit(Output::Response { id, error })?;
                    continue;
                }
                ControlCommand::Directory { event } => SessionUpdate::Directory(event),
                ControlCommand::Launch {
                    agent_id,
                    launch_id,
                } => SessionUpdate::Launch {
                    agent_id,
                    launch_id,
                },
                ControlCommand::AgentExited { agent_id } => SessionUpdate::AgentExited { agent_id },
                ControlCommand::Stop => {
                    audit.emit(Output::Response { id, error: None })?;
                    return Ok(StopReason::Requested);
                }
            };
            let error = controller
                .apply(update)
                .await
                .err()
                .map(|error| error.to_string());
            audit.emit(Output::Response { id, error })?;
        }
    }
    .await;
    drop(receive);
    let input_stopped = match input_worker {
        Ok(worker) => worker.stop().await,
        Err(failure) => failure.completion,
    };
    let input_recorded = match &input_stopped {
        Ok(positions) => audit.event("stdin_stopped", positions),
        Err(error) => audit.event("stdin_stop_failed", &error.to_string()),
    };
    let recorded = audit.event(
        "stop_requested",
        &serde_json::json!({
            "reason": result.as_ref().copied().unwrap_or(StopReason::ControlError),
            "control_error": result.as_ref().err().map(ToString::to_string),
        }),
    );
    let exit = match stop_machine(machine, &mut audit).await {
        Ok(exit) => exit,
        Err(error) => {
            let archived = archive_control(audit, input_stopped, /*writer*/ None).await;
            let mut failures = vec![format!("machine shutdown: {error}")];
            for (stage, result) in [
                ("stdin recording", input_recorded),
                ("stop recording", recorded),
                ("archive preparation", archived),
            ] {
                if let Err(error) = result {
                    failures.push(format!("{stage}: {error}"));
                }
            }
            return Err(io::Error::other(failures.join("; ")));
        }
    };
    let failed = !exit.failures.is_empty();
    let reported = (|| {
        input_recorded?;
        recorded?;
        audit.event("services_stopped", &exit.failures)?;
        audit.emit(Output::Stopped {
            account_updates: exit.account_updates,
            reason: result.as_ref().copied().unwrap_or(StopReason::ControlError),
            control_error: result.as_ref().err().map(ToString::to_string),
            failures: exit.failures,
        })
    })();
    let archived = archive_control(audit, input_stopped, exit.writer).await;
    reported?;
    result?;
    archived?;
    if failed {
        return Err(io::Error::other(
            "machine services reported shutdown failures",
        ));
    }
    Ok(())
}

async fn fail_startup(
    stage: &str,
    error: io::Error,
    mut audit: ControlAudit,
    input: InputAudit,
    writer: Option<MachineArchiveWriter>,
) -> io::Result<()> {
    let recorded = audit.event(stage, &error.to_string());
    let archived = archive_control(audit, input.close_unstarted(), writer).await;
    let mut failures = vec![format!("{stage}: {error}")];
    if let Err(error) = recorded {
        failures.push(format!("recording: {error}"));
    }
    if let Err(error) = archived {
        failures.push(format!("archive: {error}"));
    }
    Err(io::Error::other(failures.join("; ")))
}

async fn stop_machine(
    machine: MachineRuntime,
    audit: &mut ControlAudit,
) -> io::Result<MachineRuntimeExit> {
    match machine.stop().await {
        Ok(exit) => Ok(exit),
        Err(error) => {
            audit.event("shutdown_failed", &error.to_string())?;
            Err(error)
        }
    }
}
