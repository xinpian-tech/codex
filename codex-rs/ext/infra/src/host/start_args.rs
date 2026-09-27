use std::io;
use std::sync::Arc;

use codex_app_server::in_process::EmbeddedNetworkPolicy;
use codex_app_server::in_process::InProcessStartArgs;
use codex_app_server_protocol::ClientInfo;
use codex_app_server_protocol::ConfigWarningNotification;
use codex_app_server_protocol::InitializeCapabilities;
use codex_app_server_protocol::InitializeParams;
use codex_arg0::Arg0DispatchPaths;
use codex_config::NoopThreadConfigLoader;
use codex_exec_server::EnvironmentManager;
use codex_exec_server::ExecServerRuntimePaths;
use codex_feedback::CodexFeedback;
use codex_protocol::protocol::SessionSource;

use super::AgentLoadedConfig;
use super::PreparedAgentHost;

impl PreparedAgentHost {
    /// Assembles the embedded service without starting inference. Provider and
    /// account startup consume these arguments through ManagedHostServices.
    /// The local manager starts no work here; services attach the final shared
    /// process/workspace recorder before exposing it to the app-server.
    pub async fn start_args(&self, loaded: AgentLoadedConfig) -> io::Result<InProcessStartArgs> {
        let generation = self.generation.as_ref().ok_or_else(|| {
            io::Error::other("Agent startup requires recorded host configuration")
        })?;
        let programs = &generation.config.programs;
        let AgentLoadedConfig {
            mut config,
            cli_overrides,
            loader_overrides,
            cloud_config_bundle,
        } = loaded;
        if config.codex_home.as_path() != self.home
            || config.cwd.as_path() != self.launch.workspace.worktree
            || config.model.as_deref() != Some(self.launch.generation.inference.model_id.as_str())
            || config.model_provider_id != self.launch.generation.inference.provider_id
        {
            return Err(io::Error::other(
                "loaded config differs from prepared Agent",
            ));
        }
        let arg0_paths = Arg0DispatchPaths {
            codex_self_exe: Some(programs.codex.clone()),
            codex_linux_sandbox_exe: programs.linux_sandbox.clone(),
            main_execve_wrapper_exe: programs.execve_wrapper.clone(),
        };
        let paths =
            ExecServerRuntimePaths::new(programs.codex.clone(), programs.linux_sandbox.clone())?;
        #[cfg(target_os = "macos")]
        let paths =
            paths.with_allowed_symlinked_codex_home(codex_config::allowed_symlinked_codex_home(
                &config.config_layer_stack,
                &config.codex_home,
            ));
        let embedded_network_policy = EmbeddedNetworkPolicy::load(&loader_overrides).await;
        embedded_network_policy.activate(Arc::make_mut(&mut config));
        let environment_manager = EnvironmentManager::recorded_local(
            paths,
            embedded_network_policy.bind(config.http_client_factory()),
            Arc::new(self.processes.clone()),
        )
        .map_err(io::Error::other)?;
        let state_db = codex_core::init_state_db(&config).await;
        let config_warnings = config
            .startup_warnings
            .iter()
            .map(|warning| ConfigWarningNotification {
                summary: warning.clone(),
                details: None,
                path: None,
                range: None,
            })
            .collect();
        Ok(InProcessStartArgs {
            arg0_paths,
            config,
            cli_overrides,
            loader_overrides,
            strict_config: false,
            cloud_config_bundle,
            embedded_network_policy,
            thread_config_loader: Arc::new(NoopThreadConfigLoader),
            feedback: CodexFeedback::new(),
            log_db: None,
            state_db,
            environment_manager: Arc::new(environment_manager),
            config_warnings,
            session_source: SessionSource::Exec,
            enable_codex_api_key_env: false,
            initialize: InitializeParams {
                client_info: ClientInfo {
                    name: "codex_infra".to_owned(),
                    title: None,
                    version: env!("CARGO_PKG_VERSION").to_owned(),
                },
                capabilities: Some(InitializeCapabilities {
                    experimental_api: true,
                    ..Default::default()
                }),
            },
            channel_capacity: generation.config.channel_capacity.get(),
        })
    }
}
