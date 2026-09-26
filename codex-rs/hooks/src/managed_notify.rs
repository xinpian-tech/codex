use std::ffi::OsString;
use std::sync::Arc;

use crate::HookCommandArgument;
use crate::HookCommandExecutor;
use crate::HookCommandRequest;
use crate::HookCommandStdin;
use crate::HookEvent;
use crate::HookPayload;
use crate::HookResponse;
use crate::HookResult;
use crate::legacy_notify::legacy_notify_json;
use crate::registry::command_from_argv;

pub(crate) async fn execute(
    argv: &[String],
    environment: &[(OsString, OsString)],
    payload: &HookPayload,
    executor: &Arc<dyn HookCommandExecutor>,
) -> HookResponse {
    let result = async {
        let Some(command) = command_from_argv(argv, environment.iter().cloned()) else {
            return Ok(());
        };
        let command = command.as_std();
        let event_json = legacy_notify_json(payload).map_err(|error| error.to_string())?;
        let HookEvent::AfterAgent { event } = &payload.hook_event;
        let output = executor
            .prepare(HookCommandRequest {
                thread_id: event.thread_id,
                tool_call_id: None,
                program: command.get_program().to_owned(),
                arguments: command.get_args().map(ToOwned::to_owned).collect(),
                command: HookCommandArgument::Argument(event_json.clone()),
                environment: command
                    .get_envs()
                    .filter_map(|(key, value)| {
                        value.map(|value| (key.to_owned(), value.to_owned()))
                    })
                    .collect(),
                cwd: payload.cwd.to_path_buf(),
                event_json,
                stdin: HookCommandStdin::Closed,
                timeout: None,
            })
            .await?;
        match output.exit_code {
            Some(0) => Ok(()),
            status => Err(format!("legacy notify exited with status {status:?}")),
        }
    }
    .await;
    HookResponse {
        hook_name: "legacy_notify".to_owned(),
        result: match result {
            Ok(()) => HookResult::Success,
            Err(error) => HookResult::FailedContinue(std::io::Error::other(error).into()),
        },
    }
}
