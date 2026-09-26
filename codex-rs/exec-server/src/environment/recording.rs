use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

use codex_http_client::HttpClientFactory;

use super::Environment;
use super::EnvironmentManager;
use super::LOCAL_ENVIRONMENT_ID;
use crate::ExecServerError;
use crate::ExecServerRuntimePaths;
use crate::ProcessRecorderFactory;

impl EnvironmentManager {
    /// Creates a fresh local execution environment with producer recording
    /// installed before any process can start. Remote hosts construct their own
    /// local manager so recording happens where output is produced.
    pub fn recorded_local(
        runtime_paths: ExecServerRuntimePaths,
        http_client_factory: HttpClientFactory,
        recorder: Arc<dyn ProcessRecorderFactory>,
    ) -> Result<Self, ExecServerError> {
        let mut environment =
            Environment::local(runtime_paths.clone(), http_client_factory.clone());
        environment.exec_backend = environment.exec_backend.recording_backend(recorder)?;
        let environment = Arc::new(environment);
        Ok(Self {
            default_environment: Some(LOCAL_ENVIRONMENT_ID.to_owned()),
            environments: RwLock::new(HashMap::from([(
                LOCAL_ENVIRONMENT_ID.to_owned(),
                Arc::clone(&environment),
            )])),
            local_environment: Some(environment),
            local_runtime_paths: Some(runtime_paths),
            http_client_factory,
        })
    }
}
