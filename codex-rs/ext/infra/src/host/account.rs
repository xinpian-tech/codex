use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use codex_app_server::in_process::InProcessStartArgs;
use codex_config::CloudConfigBundleLoader;
use codex_core::config::Config;
use codex_infra_account::AccountConnectionConfig;
use codex_infra_account::AccountCredentialSource;
use codex_infra_account::AccountReplicaConfig;
use codex_infra_account::AccountReplicaControl;
use codex_infra_account::CodexAccountView;
use codex_infra_account::PublishedAccountAuth;
use codex_infra_account::ReplicatedAccountSource;
use codex_infra_protocol::ConfigGeneration;
use codex_infra_protocol::InferenceBinding;
use codex_infra_runtime::LaunchIntent;
use codex_login::AuthCredentialsStoreMode;
use codex_login::AuthManager;
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

/// One Agent's native account binding, retained by both startup cloud loaders
/// and serving AuthManagers. Prepare it before loading cloud configuration.
#[derive(Clone)]
pub struct NativeAccountBootstrap {
    home: PathBuf,
    binding: InferenceBinding,
    external: Arc<dyn ExternalAuth>,
    replica: Option<AccountReplicaControl>,
}

impl NativeAccountBootstrap {
    /// Prepares the generation's selected account after its published directory
    /// has synchronized. The shared ExternalAuth owns ongoing replica updates.
    pub async fn prepare_remote(
        generation: ConfigGeneration,
        home: PathBuf,
        replica: AccountReplicaConfig,
        connection: AccountConnectionConfig,
    ) -> io::Result<Self> {
        let source = ReplicatedAccountSource::start(
            generation.inference.provider_id.clone(),
            generation.inference.account_id.clone(),
            replica,
            connection,
        )
        .await?;
        let control = source.control();
        match Self::prepare(generation, home, source).await {
            Ok(mut account) => {
                account.replica = Some(control);
                Ok(account)
            }
            Err(error) => match control.stop().await {
                Ok(()) => Err(error),
                Err(cleanup) => Err(io::Error::other(format!(
                    "account preparation: {error}; replica shutdown: {cleanup}"
                ))),
            },
        }
    }

    pub async fn prepare<S: AccountCredentialSource + 'static>(
        generation: ConfigGeneration,
        home: PathBuf,
        source: S,
    ) -> io::Result<Self> {
        tokio::task::spawn_blocking(move || {
            let view = CodexAccountView::prepare(&generation, &home)?;
            let home = view.home().to_path_buf();
            let binding = view.binding().clone();
            let external = Arc::new(PublishedAccountAuth::open(
                binding.clone(),
                source,
                &home.join("infra-account-observations.journal"),
            )?);
            Ok(Self {
                home,
                binding,
                external: Arc::new(NativeHostAccount {
                    _view: view,
                    external,
                }),
                replica: None,
            })
        })
        .await
        .map_err(io::Error::other)?
    }

    pub fn home(&self) -> &Path {
        &self.home
    }

    pub fn binding(&self) -> &InferenceBinding {
        &self.binding
    }

    /// Uses the locally resolved config's endpoint/routing and this account's
    /// published credential source. Pass the returned loader into the config
    /// builder when resolving cloud layers for this Agent.
    pub async fn cloud_config_bundle(
        &self,
        config: &Config,
    ) -> io::Result<CloudConfigBundleLoader> {
        if config.codex_home.as_path() != self.home {
            return Err(io::Error::other(
                "cloud config home differs from account bootstrap",
            ));
        }
        let mut auth_config = config.auth_config();
        auth_config.auth_credentials_store_mode = AuthCredentialsStoreMode::File;
        let manager = AuthManager::shared_from_auth_config(
            auth_config,
            /*enable_codex_api_key_env*/ false,
        )
        .await
        .map_err(io::Error::other)?;
        manager
            .set_external_auth(Arc::clone(&self.external))
            .await
            .map_err(io::Error::other)?;
        Ok(codex_cloud_config::cloud_config_bundle_loader(
            manager,
            config.chatgpt_base_url.clone(),
            self.home.clone(),
            config.http_client_factory(),
        ))
    }
}

impl ManagedHostServices {
    /// Starts the generation's native Codex provider with a prepared per-Agent
    /// home and published credential source. Initial cloud config loading uses
    /// the supplied bootstrap; startup reloads and serving retain that binding.
    pub async fn start_with_native_account(
        mut self,
        mut args: InProcessStartArgs,
        process_audit: ProcessAudit,
        launch: &LaunchIntent,
        account: NativeAccountBootstrap,
    ) -> io::Result<ManagedHost> {
        let replica = account.replica.clone();
        let result = async {
            let binding = &launch.generation.inference;
            if account.binding() != binding
                || args.config.codex_home.as_path() != account.home()
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
            args.cloud_config_bundle = account.cloud_config_bundle(&args.config).await?;
            args.enable_codex_api_key_env = false;
            self.external_auth = Some(account.external);
            self.start(args, process_audit).await
        }
        .await;
        match result {
            Ok(mut host) => {
                host.account_replica = replica;
                Ok(host)
            }
            Err(error) => {
                if let Some(replica) = replica
                    && let Err(cleanup) = replica.stop().await
                {
                    return Err(io::Error::other(format!(
                        "native host startup: {error}; replica shutdown: {cleanup}"
                    )));
                }
                Err(error)
            }
        }
    }
}
