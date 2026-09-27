# Called by the separate Team State flake. All supplied paths become immutable
# generation inputs, including the account/token snapshots.
{
  pkgs,
  sourceFlake,
  teamStateFlake,
  metadata,
  effectiveConfig,
  roles,
  providers,
  accounts,
  skills,
  memory,
  machineRuntime ? null,
  agentHost ? null,
  agentRun ? null,
}:
let
  inputs = pkgs.writeText "codex-generation-inputs.json" (builtins.toJSON (
    builtins.removeAttrs metadata [
      "config_derivation"
      "config_store_path"
      "effective_config_digest"
      "machine_runtime_digest"
      "agent_host_digest"
      "source_flake_lock_hash"
      "state_flake_lock_hash"
    ] // {
      codex_source_commit = sourceFlake.rev;
      team_state_commit = teamStateFlake.rev;
      nix_system = pkgs.stdenv.hostPlatform.system;
    }
  ));
in
pkgs.runCommand "codex-config-generation" {
  nativeBuildInputs = [ pkgs.b3sum pkgs.jq ];
  doCheck = false;
  doInstallCheck = false;
} ''
  mkdir -p "$out"
  cp ${effectiveConfig} "$out/config.toml"
  cp -R ${roles} "$out/roles"
  cp -R ${providers} "$out/providers"
  cp -R ${accounts} "$out/accounts"
  cp -R ${skills} "$out/skills"
  cp -R ${memory} "$out/memory"
  ${pkgs.lib.optionalString (agentRun != null) ''
    cp ${agentRun} "$out/agent-run.json"
  ''}
  ${pkgs.lib.optionalString (machineRuntime != null) ''
    cp ${machineRuntime} "$out/machine-runtime.json"
  ''}
  ${pkgs.lib.optionalString (agentHost != null) ''
    cp ${agentHost} "$out/agent-host.json"
  ''}

  config_digest=$(b3sum --no-names "$out/config.toml")
  source_lock_digest=$(b3sum --no-names ${sourceFlake}/flake.lock)
  state_lock_digest=$(b3sum --no-names ${teamStateFlake}/flake.lock)
  machine_runtime_digest=""
  agent_host_digest=""
  ${pkgs.lib.optionalString (machineRuntime != null) ''
    machine_runtime_digest=$(b3sum --no-names "$out/machine-runtime.json")
  ''}
  ${pkgs.lib.optionalString (agentHost != null) ''
    agent_host_digest=$(b3sum --no-names "$out/agent-host.json")
  ''}
  jq --arg config "blake3:$config_digest" \
     --arg source "blake3:$source_lock_digest" \
     --arg state "blake3:$state_lock_digest" \
     --arg machine "$machine_runtime_digest" \
     --arg agent "$agent_host_digest" \
     '. + {effective_config_digest: $config, source_flake_lock_hash: $source, state_flake_lock_hash: $state}
      | if $machine == "" then . else . + {machine_runtime_digest: ("blake3:" + $machine)} end
      | if $agent == "" then . else . + {agent_host_digest: ("blake3:" + $agent)} end' \
     ${inputs} > "$out/generation.json"
''
