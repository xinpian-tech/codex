use std::io;
use std::process::Command;

use codex_infra_extension::StartedAgentHost;
use codex_infra_nix::NixTaskResolver;
use codex_infra_protocol::BuildIntent;
use codex_infra_protocol::MessageId;
use serde_json::Value;
use serde_json::json;

use super::run::RunConfig;

pub async fn build(
    host: &StartedAgentHost,
    config: &RunConfig,
    arguments: &Value,
) -> io::Result<String> {
    if host.task.build == BuildIntent::NoBuild {
        return Ok("This Task declares NoBuild; no compilation was started.".to_owned());
    }
    let nix = config.nix_program.clone().ok_or_else(|| {
        io::Error::other("set nix_program to the deployed Nix executable in agent-run.json")
    })?;
    let source = arguments["source"]
        .as_str()
        .ok_or_else(|| io::Error::other("choose assigned or worktree source"))?;
    let mut task = host.task.clone();
    let worktree = match source {
        "assigned" => None,
        "worktree" => Some(host.launch.workspace.worktree.clone()),
        _ => return Err(io::Error::other("choose assigned or worktree source")),
    };
    let hostid = host.generation.config.preparation.hostid.clone();
    let git = host.generation.config.preparation.git.clone();
    let record = host
        .directory
        .join(format!("build-{}.json", MessageId::new()));
    tokio::task::spawn_blocking(move || {
        if let Some(worktree) = worktree {
            let head = Command::new(git)
                .current_dir(&worktree)
                .args(["rev-parse", "HEAD"])
                .output()?;
            if !head.status.success() {
                return Err(io::Error::other(
                    String::from_utf8_lossy(&head.stderr).into_owned(),
                ));
            }
            task.source_commit = String::from_utf8(head.stdout)
                .map_err(io::Error::other)?
                .trim()
                .parse()
                .map_err(io::Error::other)?;
            task.flake_reference = format!("path:{}", worktree.display());
        }
        let resolver = NixTaskResolver::new(nix, hostid);
        std::fs::write(
            &record,
            serde_json::to_vec_pretty(&json!({"task":task,"status":"resolving"}))?,
        )?;
        let result = (|| -> io::Result<Value> {
            let resolved = resolver.resolve(task.clone())?;
            std::fs::write(
                &record,
                serde_json::to_vec_pretty(&json!({"resolved":resolved,"status":"building"}))?,
            )?;
            let plan = resolved
                .build_plan
                .as_ref()
                .ok_or_else(|| io::Error::other("declared build produced no plan"))?;
            let outputs = resolver.build(plan)?;
            Ok(json!({"resolved":resolved,"outputs":outputs,"status":"completed"}))
        })();
        let value = match &result {
            Ok(value) => value.clone(),
            Err(error) => json!({"task":task,"status":"failed","error":error.to_string()}),
        };
        std::fs::write(&record, serde_json::to_vec_pretty(&value)?)?;
        let value = result?;
        let summary = json!({"record":record,"status":"completed","outputs":value["outputs"]});
        let text = serde_json::to_string(&summary)?;
        if text.len() <= 2400 {
            Ok(text)
        } else {
            Ok(serde_json::to_string(
                &json!({"status":"completed","record":record}),
            )?)
        }
    })
    .await
    .map_err(io::Error::other)?
}
