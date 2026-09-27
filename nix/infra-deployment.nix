# Called by the separate Team State flake after its Agent generations exist.
# Agent run configs refer to the machine's profilesFile, not back to this output.
{
  pkgs,
  codexPackage,
  generations,
  profiles,
  machines,
  leaders ? { },
}:
let
  lib = pkgs.lib;
  q = lib.escapeShellArg;
  binding = name: generation: pkgs.runCommand "codex-binding-${name}" {
    nativeBuildInputs = [ pkgs.jq ];
    doCheck = false;
    doInstallCheck = false;
  } ''
    jq --arg drv ${q generation.drvPath} --arg out ${q (toString generation)} \
      '. + {config_derivation: $drv, config_store_path: $out}' \
      ${generation}/generation.json > "$out"
  '';
  bindings = lib.mapAttrs binding generations;
  profileFiles = lib.mapAttrs (name: profile:
    let
      base = pkgs.writeText "codex-profile-${name}.json" (builtins.toJSON (
        builtins.removeAttrs profile [ "generation" ] // {
          host_program = profile.host_program or "${codexPackage}/bin/codex-agent";
        }
      ));
    in pkgs.runCommand "codex-profile-${name}" {
      nativeBuildInputs = [ pkgs.jq ];
      doCheck = false;
      doInstallCheck = false;
    } ''
      jq --slurpfile generation ${bindings.${profile.generation}} \
        '. + {generation: $generation[0]}' ${base} > "$out"
    ''
  ) profiles;
  catalog = pkgs.runCommand "codex-worker-profiles.json" {
    nativeBuildInputs = [ pkgs.jq ];
    doCheck = false;
    doInstallCheck = false;
  } ''
    echo '{}' > "$out"
    ${lib.concatStringsSep "\n" (lib.mapAttrsToList (name: file: ''
      jq --arg name ${q name} --slurpfile profile ${file} \
        '. + {($name): $profile[0]}' "$out" > next.json
      mv next.json "$out"
    '') profileFiles)}
  '';
  commands = lib.mapAttrs (hostid: machine:
    let
      configuration = "${generations.${machine.generation}}/machine-runtime.json";
      accountDirectoriesFile = machine.accountDirectoriesFile or "${machine.machinesFile}.accounts.json";
      common = ''
        config=${q configuration}
      '';
      refresh = pkgs.writeShellApplication {
        name = "codex-refresh-machines-${hostid}";
        runtimeInputs = [ pkgs.git pkgs.jq pkgs.coreutils ];
        text = common + ''
          repo=$(jq -r .team_state_repository "$config")
          remote=$(jq -r .archive_remote "$config")
          mkdir -p ${q (builtins.dirOf machine.machinesFile)}
          mkdir -p ${q (builtins.dirOf accountDirectoriesFile)}
          temporary=$(mktemp ${q "${machine.machinesFile}.XXXXXX"})
          accounts=$(mktemp ${q "${accountDirectoriesFile}.XXXXXX"})
          echo '{}' > "$temporary"
          echo '{}' > "$accounts"
          ${lib.concatStringsSep "\n" (lib.mapAttrsToList (peer: _: ''
            if git -C "$repo" fetch --no-write-fetch-head "$remote" ${q "refs/heads/codex-machines/${peer}:refs/codex/machine-endpoints/${peer}"}; then
              endpoint=$(git -C "$repo" show ${q "refs/codex/machine-endpoints/${peer}:launch-endpoint.json"})
              jq --arg host ${q peer} --argjson endpoint "$endpoint" \
                '. + {($host): $endpoint}' "$temporary" > "$temporary.next"
              mv "$temporary.next" "$temporary"
              if stream=$(git -C "$repo" show ${q "refs/codex/machine-endpoints/${peer}:account-directory-stream.json"}); then
                jq --arg host ${q peer} --argjson stream "$stream" \
                  '. + {($host): $stream}' "$accounts" > "$accounts.next"
                mv "$accounts.next" "$accounts"
              fi
            fi
          '') machines)}
          mv "$temporary" ${q machine.machinesFile}
          mv "$accounts" ${q accountDirectoriesFile}
        '';
      };
      publish = pkgs.writeShellApplication {
        name = "codex-publish-machine-${hostid}";
        runtimeInputs = [ pkgs.git pkgs.jq pkgs.coreutils ];
        text = common + ''
          spool=$(jq -r .spool_directory "$config")
          repo=$(jq -r .team_state_repository "$config")
          remote=$(jq -r .archive_remote "$config")
          reference=${q "refs/heads/codex-machines/${hostid}"}
          local_reference=${q "refs/codex/machine-endpoints/${hostid}"}
          parent=""
          if git -C "$repo" fetch --no-write-fetch-head "$remote" "$reference:$local_reference"; then
            parent=$(git -C "$repo" rev-parse "$local_reference")
          fi
          endpoint_index=$(mktemp "$spool/endpoint-index.XXXXXX")
          rm "$endpoint_index"
          export GIT_INDEX_FILE="$endpoint_index"
          trap 'rm -f "$endpoint_index"' EXIT
          git -C "$repo" read-tree --empty
          blob=$(git -C "$repo" hash-object -w "$spool/agent-launch-endpoint.json")
          git -C "$repo" update-index --add --cacheinfo "100644,$blob,launch-endpoint.json"
          blob=$(git -C "$repo" hash-object -w "$spool/account-directory-stream.json")
          git -C "$repo" update-index --add --cacheinfo "100644,$blob,account-directory-stream.json"
          tree=$(git -C "$repo" write-tree)
          parents=()
          if [ -n "$parent" ]; then parents=(-p "$parent"); fi
          commit=$(git -C "$repo" commit-tree "$tree" "''${parents[@]}" -m ${q "Update machine ${hostid} launch endpoint"})
          git -C "$repo" push "$remote" "$commit:$reference"
        '';
      };
      start = pkgs.writeShellApplication {
        name = "codex-start-machine-${hostid}";
        runtimeInputs = [ pkgs.jq pkgs.coreutils ];
        text = common + ''
          spool=$(jq -r .spool_directory "$config")
          mkdir -p "$spool" ${q (builtins.dirOf machine.profilesFile)}
          cp ${catalog} ${q machine.profilesFile}
          rm -f "$spool/agent-launch-endpoint.json" "$spool/account-directory-stream.json"
          ${codexPackage}/bin/codex-machine-runtime "$config" < /dev/null &
          runtime_pid=$!
          discovery_pid=""
          trap 'if [ -n "$discovery_pid" ]; then kill "$discovery_pid" 2>/dev/null || true; fi; kill -TERM "$runtime_pid" 2>/dev/null || true; wait "$runtime_pid" || true' EXIT
          until [ -s "$spool/agent-launch-endpoint.json" ] && [ -s "$spool/account-directory-stream.json" ]; do
            kill -0 "$runtime_pid"
            sleep 0.1
          done
          ${publish}/bin/codex-publish-machine-${hostid}
          ${refresh}/bin/codex-refresh-machines-${hostid}
          (
            while kill -0 "$runtime_pid" 2>/dev/null; do
              sleep 10
              ${refresh}/bin/codex-refresh-machines-${hostid}
            done
          ) &
          discovery_pid=$!
          wait "$runtime_pid"
        '';
      };
    in { inherit start publish refresh accountDirectoriesFile; }
  ) machines;
  leaderCommands = lib.mapAttrs (name: leader:
    let
      machine = machines.${leader.machine};
      specification = pkgs.writeText "codex-leader-${name}.json" (builtins.toJSON (
        builtins.removeAttrs leader [ "machine" "generation" ] // {
          parent_agent_id = null;
          host_program = leader.host_program or "${codexPackage}/bin/codex-agent";
        }
      ));
      spawn = pkgs.runCommand "codex-leader-spawn-${name}.json" {
        nativeBuildInputs = [ pkgs.jq ];
        doCheck = false;
        doInstallCheck = false;
      } ''
        jq --slurpfile generation ${bindings.${leader.generation}} \
          '. + {generation: $generation[0]}' ${specification} > "$out"
      '';
    in pkgs.writeShellApplication {
      name = "codex-launch-leader-${name}";
      runtimeInputs = [ pkgs.jq ];
      text = ''
        ${commands.${leader.machine}.refresh}/bin/codex-refresh-machines-${leader.machine}
        endpoint=$(jq -r --arg host ${q leader.machine} '.[$host]' ${q machine.machinesFile})
        exec ${codexPackage}/bin/codex-agent launch "$endpoint" ${spawn}
      '';
    }
  ) leaders;
in {
  inherit catalog bindings;
  machines = commands;
  leaders = leaderCommands;
  package = pkgs.symlinkJoin {
    name = "codex-infra-deployment";
    paths = lib.concatMap (machine: [ machine.start machine.publish machine.refresh ]) (builtins.attrValues commands)
      ++ builtins.attrValues leaderCommands;
  };
}
