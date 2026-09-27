use codex_app_server_protocol::DynamicToolFunctionSpec;
use codex_app_server_protocol::DynamicToolSpec;
use serde_json::json;

pub(crate) fn instructions(launch: &codex_infra_runtime::LaunchIntent) -> std::io::Result<String> {
    let role_path = launch
        .generation
        .config_store_path
        .join("roles")
        .join(format!("{}.json", launch.role));
    let role: codex_infra_protocol::RoleDefinition =
        serde_json::from_slice(&std::fs::read(&role_path)?)?;
    let role = serde_json::to_string(&role)?;
    let role = &role[..role.floor_char_boundary(role.len().min(2000))];
    Ok(format!(
        "You are an independent infra Agent. Execution context supplies machine, repo, commit, role and inference identity. TaskSpec identifies the flake and build task. Run compilation within that Nix task. All Agent messages use infra_send through tmux. Use infra_directory and infra_spawn for workers; start with the configured DeepSeek Flash profile, choose Codex after escalation. Yield when waiting for replies. Integrate workers' pushed commits into your worktree with Git before infra_complete. Use infra_escalate when unable to finish. Host hooks commit/push changes. Native in-process subagents are disabled. Role definition (full file {}): {role}",
        role_path.display()
    ))
}

pub(crate) fn specs() -> Vec<DynamicToolSpec> {
    [
        ("infra_spawn", "Start an independent worker in local or remote tmux. Choose a machine and explicit provider/account/model profile from infra_directory. Prefer the DeepSeek Flash profile; choose Codex after an escalation. Task prose is delivered through tmux after this turn.", json!({"machine_id":{"type":"string"},"profile":{"type":"string"},"role":{"type":"string"},"objective":{"type":"string"},"scope":{"type":"string"}}), vec!["machine_id","profile","role","objective","scope"]),
        ("infra_send", "Send a directed message to an Agent. It is checkpointed and sent through tmux when this turn ends. Yield after requesting a reply.", json!({"to":{"type":"object","properties":{"agent_id":{"type":"string"},"machine_id":{"type":"string"},"role":{"type":"string"}},"required":["agent_id","machine_id","role"],"additionalProperties":false},"body":{"type":"string"}}), vec!["to", "body"]),
        ("infra_directory", "Read the role and task directory to choose a relevant recipient.", json!({}), vec![]),
        ("infra_complete", "Declare the assigned task complete after integrating its results. Supply a concise result; final commit/push and tmux publication follow this turn.", json!({"result":{"type":"string"}}), vec!["result"]),
        ("infra_escalate", "Report a problem requiring a Codex worker to the task owner. Include attempted approaches and the remaining problem, then finish this turn.", json!({"result":{"type":"string"}}), vec!["result"]),
    ]
    .into_iter()
    .map(|(name, description, properties, required)| {
        DynamicToolSpec::Function(DynamicToolFunctionSpec {
            name: name.to_owned(), description: description.to_owned(),
            input_schema: json!({"type":"object", "properties":properties, "required":required, "additionalProperties":false}),
            defer_loading: false,
        })
    })
    .collect()
}
