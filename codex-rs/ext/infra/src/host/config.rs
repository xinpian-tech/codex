use std::io;
use std::sync::Arc;

use codex_config::CloudConfigBundleLoader;
use codex_config::LoaderOverrides;
use codex_core::config::Config;
use codex_core::config::ConfigBuilder;
use codex_core::config::ConfigOverrides;

use super::PreparedAgentHost;

/// Keep these inputs with the embedded app-server so thread reloads use the
/// same generation and managed lifecycle choices as initial configuration.
pub struct AgentLoadedConfig {
    pub config: Arc<Config>,
    pub cli_overrides: Vec<(String, toml::Value)>,
    pub loader_overrides: LoaderOverrides,
    pub cloud_config_bundle: CloudConfigBundleLoader,
}

impl PreparedAgentHost {
    /// Uses Codex's existing layered loader with the prepared home and worktree.
    /// The generation's config.toml declares the selected provider; Chat startup
    /// later replaces its transport endpoint with the local Responses frontend.
    /// Native account startup first loads without cloud layers, prepares the
    /// account-bound loader, then calls this again with that loader.
    pub async fn load_config(
        &self,
        cloud_config_bundle: CloudConfigBundleLoader,
    ) -> io::Result<AgentLoadedConfig> {
        let inputs = self.generation.as_ref().ok_or_else(|| {
            io::Error::other("Agent configuration requires a recorded generation")
        })?;
        let generation = &self.launch.generation;
        let directory = &generation.config_store_path;
        let cli_overrides = vec![
            (
                "model".to_owned(),
                toml::Value::String(generation.inference.model_id.clone()),
            ),
            (
                "model_provider".to_owned(),
                toml::Value::String(generation.inference.provider_id.clone()),
            ),
            (
                "features.multi_agent".to_owned(),
                toml::Value::Boolean(false),
            ),
            (
                "features.multi_agent_v2".to_owned(),
                toml::Value::Boolean(false),
            ),
            (
                "features.agent_message_board".to_owned(),
                toml::Value::Boolean(false),
            ),
            (
                "features.external_agent_memory_import".to_owned(),
                toml::Value::Boolean(false),
            ),
            (
                "memories.generate_memories".to_owned(),
                toml::Value::Boolean(false),
            ),
            (
                "cli_auth_credentials_store".to_owned(),
                toml::Value::String("file".to_owned()),
            ),
        ];
        let loader_overrides = LoaderOverrides {
            managed_config_path: Some(directory.join("managed_config.toml")),
            system_config_path: Some(directory.join("system-config.toml")),
            system_requirements_path: Some(directory.join("requirements.toml")),
            ignore_project_config: true,
            #[cfg(target_os = "macos")]
            managed_preferences_base64: Some(String::new()),
            macos_managed_config_requirements_base64: Some(String::new()),
            ..Default::default()
        };
        // A resumed home may contain runtime updates; the launch's configuration
        // remains the exact snapshot consumed and recorded at preparation.
        let config_path = self.home.join("config.toml");
        let config_bytes = tokio::task::spawn_blocking(move || std::fs::read(config_path))
            .await
            .map_err(io::Error::other)??;
        if config_bytes != inputs.effective_config_bytes {
            return Err(io::Error::other(
                "Agent home config differs from recorded generation",
            ));
        }
        let config = ConfigBuilder::default()
            .codex_home(self.home.clone())
            .cli_overrides(cli_overrides.clone())
            .harness_overrides(ConfigOverrides {
                cwd: Some(self.launch.workspace.worktree.clone()),
                ..Default::default()
            })
            .loader_overrides(loader_overrides.clone())
            .cloud_config_bundle(cloud_config_bundle.clone())
            .build()
            .await?;
        Ok(AgentLoadedConfig {
            config: Arc::new(config),
            cli_overrides,
            loader_overrides,
            cloud_config_bundle,
        })
    }
}
