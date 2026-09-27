use std::io;
use std::path::PathBuf;

#[cfg(unix)]
#[path = "agent/assets.rs"]
mod assets;
#[cfg(unix)]
#[path = "agent/build.rs"]
mod build;
#[cfg(unix)]
#[path = "agent/contributions.rs"]
mod contributions;
#[cfg(unix)]
#[path = "agent/directory.rs"]
mod directory;
#[cfg(unix)]
#[path = "agent/events.rs"]
mod events;
#[cfg(unix)]
#[path = "agent/finish.rs"]
mod finish;
#[cfg(unix)]
#[path = "agent/knowledge.rs"]
mod knowledge;
#[cfg(unix)]
#[path = "agent/memory.rs"]
mod memory;
#[cfg(unix)]
#[path = "agent/run.rs"]
mod run;
#[cfg(unix)]
#[path = "agent/sessions.rs"]
mod sessions;
#[cfg(unix)]
#[path = "agent/spawn.rs"]
mod spawn;
#[cfg(unix)]
#[path = "agent/tasks.rs"]
mod tasks;

fn main() -> io::Result<()> {
    // The embedded executor re-enters this binary for filesystem, exec and
    // apply-patch helpers, using the same dispatch as the upstream CLI.
    let _arg0_guard = codex_arg0::arg0_dispatch();
    let mut args = std::env::args_os().skip(1);
    let first = args.next();
    #[cfg(unix)]
    if first.as_deref() == Some(std::ffi::OsStr::new("launch")) {
        let endpoint = args
            .next()
            .ok_or_else(|| io::Error::other("usage: codex-agent launch <endpoint> <spawn.json>"))?
            .into_string()
            .map_err(|_| io::Error::other("endpoint is not UTF-8"))?
            .parse()
            .map_err(io::Error::other)?;
        let path = args
            .next()
            .ok_or_else(|| io::Error::other("missing spawn specification"))?;
        if args.next().is_some() {
            return Err(io::Error::other("unexpected launch arguments"));
        }
        let agent = serde_json::from_slice(&std::fs::read(path)?)?;
        let response = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(codex_infra_runtime::launch_request(
                endpoint,
                codex_infra_runtime::LaunchServiceRequest::Spawn { agent },
            ))?;
        println!("{}", serde_json::to_string(&response)?);
        return Ok(());
    }
    let flag = if first.as_deref() == Some(std::ffi::OsStr::new("agent")) {
        args.next()
    } else {
        first
    };
    if flag.as_deref() != Some(std::ffi::OsStr::new("--binding")) {
        return Err(io::Error::other(
            "usage: codex-agent --binding <launch.json>",
        ));
    }
    let binding = PathBuf::from(
        args.next()
            .ok_or_else(|| io::Error::other("missing binding path"))?,
    );
    if args.next().is_some() {
        return Err(io::Error::other("unexpected Agent arguments"));
    }
    #[cfg(unix)]
    {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(run::run(binding))
    }
    #[cfg(not(unix))]
    {
        let _ = binding;
        Err(io::Error::other("codex-agent requires a Unix tmux host"))
    }
}
