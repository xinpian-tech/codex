use std::io;

use codex_protocol::models::ContentItemKind;
use serde::Serialize;

use super::ContextualUserFragment;

/// Host-resolved identity of an independently running Agent.
#[derive(Clone, Debug, Serialize)]
pub struct ExecutionIdentity {
    pub agent_id: String,
    pub root_session_id: String,
    pub parent_agent_id: Option<String>,
    pub machine_id: String,
    pub task_id: String,
    pub role: String,
}

/// Current workspace and pushed revision supplied by the host checkpoint path.
#[derive(Clone, Debug, Serialize)]
pub struct ExecutionWorkspace {
    pub repo: String,
    pub worktree: String,
    pub branch: String,
    pub commit: String,
}

/// Selected inference and configuration references, without credential contents.
#[derive(Clone, Debug, Serialize)]
pub struct ExecutionInference {
    pub provider: String,
    pub account: String,
    pub model_id: String,
    pub credential_revision: String,
    pub config_generation: String,
    pub role_revision: String,
    pub skills_revision: String,
    pub memory_revision: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "section", content = "binding", rename_all = "snake_case")]
pub enum AgentExecutionContext {
    Identity(ExecutionIdentity),
    Workspace(ExecutionWorkspace),
    Inference(ExecutionInference),
}

/// One bounded execution binding. Updates append a new fragment; they do not
/// replace earlier history. Splitting sections keeps each item below 1K tokens.
#[derive(Clone, Debug)]
pub struct AgentExecutionFragment {
    body: String,
}

impl AgentExecutionFragment {
    /// Includes markers and instruction text in the byte limit. A 900-byte
    /// fragment also stays below 1,000 byte-level text tokens.
    pub fn new(context: AgentExecutionContext) -> io::Result<Self> {
        let json = serde_json::to_string(&context)?;
        let fragment = Self {
            body: format!(
                "Current execution binding; use the newest binding for this section.\n{json}"
            ),
        };
        if fragment.render().len() > 900 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Agent execution context exceeds 900 bytes; use shorter reference identifiers",
            ));
        }
        Ok(fragment)
    }
}

impl ContextualUserFragment for AgentExecutionFragment {
    fn role(&self) -> &'static str {
        "user"
    }

    fn content_kind(&self) -> ContentItemKind {
        ContentItemKind("infra.execution_context".to_owned())
    }

    fn markers(&self) -> (&'static str, &'static str) {
        Self::type_markers()
    }

    fn type_markers() -> (&'static str, &'static str) {
        (
            "<agent_execution_context>\n",
            "\n</agent_execution_context>",
        )
    }

    fn body(&self) -> String {
        self.body.clone()
    }
}
