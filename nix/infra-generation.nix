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
}:
let
  inputs = pkgs.writeText "codex-generation-inputs.json" (builtins.toJSON (
    builtins.removeAttrs metadata [
      "config_derivation"
      "config_store_path"
      "effective_config_digest"
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
  ${pkgs.lib.optionalString (machineRuntime != null) ''
    cp ${machineRuntime} "$out/machine-runtime.json"
  ''}

  config_digest=$(b3sum --no-names "$out/config.toml")
  source_lock_digest=$(b3sum --no-names ${sourceFlake}/flake.lock)
  state_lock_digest=$(b3sum --no-names ${teamStateFlake}/flake.lock)
  jq --arg config "blake3:$config_digest" \
     --arg source "blake3:$source_lock_digest" \
     --arg state "blake3:$state_lock_digest" \
     '. + {effective_config_digest: $config, source_flake_lock_hash: $source, state_flake_lock_hash: $state}' \
     ${inputs} > "$out/generation.json"
''
