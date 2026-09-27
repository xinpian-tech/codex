use std::io;
use std::sync::Arc;

use codex_app_server::in_process::InProcessStartArgs;
use codex_infra_provider::ChatFrontend;
use codex_infra_provider::ChatFrontendConfig;
use codex_infra_runtime::LaunchIntent;
use serde_json::json;

use super::ManagedHost;
use super::ManagedHostServices;
use crate::ProcessAudit;

impl ManagedHostServices {
    /// Starts the resolved Chat Completions account alongside this Agent's
    /// embedded Codex host. Native Responses accounts use the existing start.
    /// Both initial config and thread reload overrides point at the same local
    /// frontend; credentials remain in its immutable provider binding.
    pub async fn start_with_chat_provider(
        self,
        mut args: InProcessStartArgs,
        process_audit: ProcessAudit,
        launch: &LaunchIntent,
        provider_config: ChatFrontendConfig,
    ) -> io::Result<ManagedHost> {
        if provider_config.binding != launch.generation.inference
            || provider_config.audit.root_session_id != self.audit.identity.root_session_id
            || provider_config.audit.agent_id != self.audit.identity.agent_id
            || provider_config.audit.machine_id != self.audit.identity.machine_id
            || provider_config.audit.launch_id != self.tools.launch_id
            || provider_config.audit.launch_id != launch.launch_id
            || launch.workspace.agent_id != self.audit.identity.agent_id
            || launch.workspace.root_session_id != self.audit.identity.root_session_id
            || launch.machine_id != self.audit.identity.machine_id
        {
            return Err(io::Error::other(
                "provider config differs from Agent launch binding",
            ));
        }
        let binding = provider_config.binding.clone();
        let provider = ChatFrontend::start(provider_config).await?;
        let configured = (|| -> io::Result<()> {
            let config = Arc::make_mut(&mut args.config);
            let info = serde_json::from_value(json!({
                "name": binding.provider_id,
                "base_url": provider.base_url(),
                "wire_api": "responses",
                "requires_openai_auth": false,
                "supports_websockets": false,
            }))?;
            config.model_provider = info;
            config.model_provider_id = binding.provider_id.clone();
            config.model = Some(binding.model_id.clone());
            config
                .model_providers
                .insert(binding.provider_id.clone(), config.model_provider.clone());
            args.cli_overrides.extend([
                (
                    "model".to_owned(),
                    toml::Value::String(binding.model_id.clone()),
                ),
                (
                    "model_provider".to_owned(),
                    toml::Value::String(binding.provider_id.clone()),
                ),
                (
                    "model_providers".to_owned(),
                    toml::Value::try_from(&config.model_providers).map_err(io::Error::other)?,
                ),
            ]);
            args.enable_codex_api_key_env = false;
            Ok(())
        })();
        if let Err(error) = configured {
            let stopped = provider.stop().await;
            return Err(io::Error::other(format!(
                "provider configuration: {error}; shutdown: {stopped:?}"
            )));
        }
        match self.start(args, process_audit).await {
            Ok(mut host) => {
                host.provider = Some(provider);
                Ok(host)
            }
            Err(error) => {
                let stopped = provider.stop().await;
                Err(io::Error::other(format!(
                    "Agent host startup: {error}; provider shutdown: {stopped:?}"
                )))
            }
        }
    }
}
