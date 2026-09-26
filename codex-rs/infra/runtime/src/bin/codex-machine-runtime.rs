use std::io;
use std::path::PathBuf;

use codex_infra_protocol::AgentId;
use codex_infra_protocol::DirectoryEvent;
use codex_infra_protocol::MachineId;
use codex_infra_protocol::MessageId;
use codex_infra_protocol::RootSessionId;
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

#[path = "machine_runtime/input_worker.rs"]
mod input_worker;
use input_worker::InputWorker;

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
    },
    Response {
        id: String,
        error: Option<String>,
    },
    Stopped {
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
            audit.event("signal_setup_failed", &error.to_string())?;
            return Err(error);
        }
    };
    let capacity = config.scheduling.command_capacity.get();
    let root_session_id = config.root_session_id;
    let machine_id = config.machine_id.clone();
    let mut machine = match config.open().await {
        Ok(machine) => machine,
        Err(error) => {
            audit.event("open_failed", &error.to_string())?;
            return Err(error);
        }
    };
    if let Err(error) = machine.start().await {
        let recorded = audit.event("startup_failed", &error.to_string());
        let exit = stop_machine(machine, &mut audit).await?;
        recorded?;
        audit.event("startup_cleanup", &exit.failures)?;
        return Err(io::Error::other(format!(
            "machine startup: {error}; cleanup: {:?}",
            exit.failures
        )));
    }
    let (send, mut receive) = mpsc::channel(capacity);
    let input_worker = InputWorker::start(input_audit, send);
    let result = async {
        input_worker
            .as_ref()
            .map_err(|error| io::Error::other(error.to_string()))?;
        audit.emit(Output::Ready {
            control_run_id: audit.run_id,
            root_session_id,
            machine_id,
            endpoint: machine.endpoint(),
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
        Err(error) => Err(error),
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
    let exit = stop_machine(machine, &mut audit).await?;
    input_recorded?;
    recorded?;
    let failed = !exit.failures.is_empty();
    audit.event("services_stopped", &exit.failures)?;
    audit.emit(Output::Stopped {
        reason: result.as_ref().copied().unwrap_or(StopReason::ControlError),
        control_error: result.as_ref().err().map(ToString::to_string),
        failures: exit.failures,
    })?;
    result?;
    input_stopped?;
    if failed {
        return Err(io::Error::other(
            "machine services reported shutdown failures",
        ));
    }
    Ok(())
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
