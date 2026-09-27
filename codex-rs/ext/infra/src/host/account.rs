use std::io;
use std::sync::Arc;

use codex_app_server::in_process::InProcessStartArgs;
use codex_infra_account::AccountCredentialSource;
use codex_infra_account::CodexAccountView;
use codex_infra_account::PublishedAccountAuth;
use codex_infra_runtime::LaunchIntent;
use codex_login::AuthCredentialsStoreMode;
use codex_login::CodexAuth;
use codex_login::ExternalAuth;
use codex_login::ExternalAuthFuture;
use codex_login::ExternalAuthRefreshContext;

use super::ManagedHost;
use super::ManagedHostServices;
use crate::ProcessAudit;

// Managers retain this wrapper while using auth, including bootstrap loaders.
// The account view therefore outlives cancellation of the host startup waiter.
struct NativeHostAccount<S> {
    _view: CodexAccountView,
    external: Arc<PublishedAccountAuth<S>>,
}

impl<S: AccountCredentialSource> ExternalAuth for NativeHostAccount<S> {
    fn resolve(&self) -> ExternalAuthFuture<'_, CodexAuth> {
        self.external.resolve()
    }

    fn refresh(&self, context: ExternalAuthRefreshContext) -> ExternalAuthFuture<'_, CodexAuth> {
        self.external.refresh(context)
    }
}

impl ManagedHostServices {
    /// Starts the generation's native Codex provider with a prepared per-Agent
    /// home and published credential source. Config must have been loaded for
    /// that home and provider; caller-created cloud loaders use the same source.
    pub async fn start_with_native_account<S: AccountCredentialSource + 'static>(
        mut self,
        mut args: InProcessStartArgs,
        process_audit: ProcessAudit,
        launch: &LaunchIntent,
        view: CodexAccountView,
        external: Arc<PublishedAccountAuth<S>>,
    ) -> io::Result<ManagedHost> {
        let binding = &launch.generation.inference;
        if view.binding() != binding
            || external.binding() != binding
            || args.config.codex_home.as_path() != view.home()
            || args.config.model_provider_id != binding.provider_id
            || !args.config.model_provider.requires_openai_auth
            || launch.launch_id != self.tools.launch_id
            || launch.workspace.agent_id != self.audit.identity.agent_id
            || launch.workspace.root_session_id != self.audit.identity.root_session_id
            || launch.machine_id != self.audit.identity.machine_id
        {
            return Err(io::Error::other(
                "native account differs from Agent launch binding",
            ));
        }
        let config = Arc::make_mut(&mut args.config);
        config.model = Some(binding.model_id.clone());
        config.cli_auth_credentials_store_mode = AuthCredentialsStoreMode::File;
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
                "cli_auth_credentials_store".to_owned(),
                toml::Value::String("file".to_owned()),
            ),
            (
                "model_providers".to_owned(),
                toml::Value::try_from(&config.model_providers).map_err(io::Error::other)?,
            ),
        ]);
        args.enable_codex_api_key_env = false;
        self.external_auth = Some(Arc::new(NativeHostAccount {
            _view: view,
            external,
        }));
        self.start(args, process_audit).await
    }
}
