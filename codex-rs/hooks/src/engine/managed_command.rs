use std::collections::HashMap;
use std::ffi::OsStr;
use std::ffi::OsString;
use std::future::Future;
use std::path::Path;
use std::time::Duration;

use codex_protocol::shell_environment::is_non_inheritable_env_var;

use super::CommandHookRuntime;
use super::ConfiguredHandler;
use super::HandlerRunResult;
use super::default_shell_program;
use super::run_prepared_command;

pub(crate) fn run_command<'a>(
    runtime: &'a CommandHookRuntime,
    handler: &'a ConfiguredHandler,
    command_line: &'a str,
    env: &'a HashMap<String, String>,
    input_json: &'a str,
    cwd: &'a Path,
) -> impl Future<Output = HandlerRunResult> + Send + 'a {
    let prepared = prepare_command(runtime, handler, command_line, env, input_json, cwd);
    run_prepared_command(
        runtime,
        handler,
        command_line,
        env,
        input_json,
        cwd,
        prepared,
    )
}

pub(super) fn prepare_command(
    runtime: &CommandHookRuntime,
    handler: &ConfiguredHandler,
    command_line: &str,
    env: &HashMap<String, String>,
    input_json: &str,
    cwd: &Path,
) -> Option<crate::HookCommandFuture> {
    let executor = runtime.command_executor.as_ref()?;
    let program = if runtime.shell.program.is_empty() {
        default_shell_program(&runtime.environment)
    } else {
        OsString::from(&runtime.shell.program)
    };
    let arguments = if runtime.shell.program.is_empty() {
        vec![OsString::from(if cfg!(windows) { "/C" } else { "-lc" })]
    } else {
        runtime.shell.args.iter().map(OsString::from).collect()
    };
    let command = if cfg!(windows)
        && (runtime.shell.program.is_empty()
            || runtime
                .shell
                .args
                .iter()
                .any(|arg| arg.eq_ignore_ascii_case("/c")))
    {
        crate::HookCommandArgument::WindowsRaw(format!(r#""{command_line}""#))
    } else {
        crate::HookCommandArgument::Argument(command_line.to_owned())
    };
    let environment = runtime
        .environment
        .iter()
        .map(|(key, value)| (key.as_os_str(), value.as_os_str()))
        .chain(
            env.iter()
                .map(|(key, value)| (OsStr::new(key), OsStr::new(value))),
        )
        .filter(|(key, _)| !key.to_str().is_some_and(is_non_inheritable_env_var))
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect();
    Some(executor.prepare(crate::HookCommandRequest {
        thread_id: runtime.thread_id,
        tool_call_id: runtime.tool_call_id.clone(),
        program,
        arguments,
        command,
        environment,
        cwd: cwd.to_owned(),
        stdin: input_json.as_bytes().to_vec(),
        timeout: Duration::from_secs(handler.timeout_sec),
    }))
}
