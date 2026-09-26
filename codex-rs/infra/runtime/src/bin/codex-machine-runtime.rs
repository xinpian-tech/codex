use std::io;
use std::io::BufRead;
use std::io::Write;
use std::path::PathBuf;
use std::thread;

use codex_infra_protocol::AgentId;
use codex_infra_protocol::DirectoryEvent;
use codex_infra_protocol::MachineId;
use codex_infra_protocol::MessageId;
use codex_infra_protocol::RootSessionId;
use codex_infra_runtime::MachineLaunchConfig;
use codex_infra_runtime::SessionUpdate;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::mpsc;

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
        root_session_id: RootSessionId,
        machine_id: MachineId,
        endpoint: std::net::SocketAddr,
    },
    Response {
        id: String,
        error: Option<String>,
    },
    Stopped {
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
    let config = MachineLaunchConfig::read(&path)?;
    let capacity = config.scheduling.command_capacity.get();
    let root_session_id = config.root_session_id;
    let machine_id = config.machine_id.clone();
    let mut machine = config.open().await?;
    if let Err(error) = machine.start().await {
        let exit = machine.stop().await?;
        return Err(io::Error::other(format!(
            "machine startup: {error}; cleanup: {:?}",
            exit.failures
        )));
    }
    let (send, mut receive) = mpsc::channel(capacity);
    thread::spawn(move || {
        // This thread owns only stdin, never journals or runtime services. EOF
        // requests shutdown; an explicit Stop need not wait for another line.
        for line in io::stdin().lock().lines() {
            let request = line
                .and_then(|line| serde_json::from_str::<Request>(&line).map_err(io::Error::other));
            if send.blocking_send(request).is_err() {
                break;
            }
        }
    });
    let result = async {
        emit(Output::Ready {
            root_session_id,
            machine_id,
            endpoint: machine.endpoint(),
        })?;
        let controller = machine.controller()?;
        while let Some(request) = receive.recv().await {
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
                    emit(Output::Response { id, error: None })?;
                    break;
                }
            };
            let error = controller
                .apply(update)
                .await
                .err()
                .map(|error| error.to_string());
            emit(Output::Response { id, error })?;
        }
        Ok::<(), io::Error>(())
    }
    .await;
    drop(receive);
    let exit = machine.stop().await?;
    let failed = !exit.failures.is_empty();
    emit(Output::Stopped {
        failures: exit.failures,
    })?;
    result?;
    if failed {
        return Err(io::Error::other(
            "machine services reported shutdown failures",
        ));
    }
    Ok(())
}

fn emit(output: Output) -> io::Result<()> {
    let mut stdout = io::stdout().lock();
    serde_json::to_writer(&mut stdout, &output)?;
    stdout.write_all(b"\n")?;
    stdout.flush()
}
