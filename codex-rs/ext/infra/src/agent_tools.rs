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
        "You are an independent infra Agent. Execution context supplies machine, repo, commit, role and inference identity. TaskSpec identifies the flake and build task; infra_build realizes that task. All Agent messages use infra_send through tmux. Use infra_directory and infra_spawn for workers; start with the configured DeepSeek Flash profile, choose Codex after escalation. Yield when waiting for replies. Integrate each contribution in its target repository before infra_complete. Team State knowledge tasks create observations and memory/skill candidates with Session commit, event and author references; team confirmation precedes publication to main. Use infra_escalate when unable to finish. Host hooks commit/push changes. Native in-process subagents are disabled. Role definition (full file {}): {role}",
        role_path.display()
    ))
}

pub(crate) fn specs() -> Vec<DynamicToolSpec> {
    [
        ("infra_build", "Resolve and build this Task's declared Nix attribute. Choose assigned for its declared flake or worktree to snapshot and build your current changes. NoBuild tasks return without compiling. Full resolution/output metadata is retained in the session.", json!({"source":{"type":"string","enum":["assigned","worktree"]}}), vec!["source"]),
        ("infra_spawn", "Start an independent worker in local or remote tmux. Choose machine and provider/account/model profile from infra_directory. Prefer DeepSeek Flash; choose Codex after escalation. Declare the flake and build attribute (null for no build). Task prose travels through tmux after this turn.", json!({"machine_id":{"type":"string"},"profile":{"type":"string"},"role":{"type":"string"},"objective":{"type":"string"},"scope":{"type":"string"},"flake_reference":{"type":"string"},"build_attribute":{"type":["string","null"]}}), vec!["machine_id","profile","role","objective","scope","flake_reference","build_attribute"]),
        ("infra_send", "Send a directed message using your role's output kinds. Optional task/assignment IDs override the current input's correlation. Checkpoint and tmux delivery happen when this turn ends; yield to receive a reply.", json!({"to":{"type":"object","properties":{"agent_id":{"type":"string"},"machine_id":{"type":"string"},"role":{"type":"string"}},"required":["agent_id","machine_id","role"],"additionalProperties":false},"body":{"type":"string"},"kind":{"type":"string","enum":["task","progress","working_context","contribution","escalation","result"]},"task_id":{"type":"string"},"assignment_id":{"type":"string"}}), vec!["to", "body", "kind"]),
        ("infra_directory", "Page through agents, inference profiles or machines. Pass the returned next cursor as after. Agent pages default to related roles; role and task_id narrow the query.", json!({"view":{"type":"string","enum":["agents","profiles","machines"]},"after":{"type":"string"},"role":{"type":"string"},"task_id":{"type":"string"}}), vec!["view"]),
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
