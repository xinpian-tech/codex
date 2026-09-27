use std::collections::BTreeMap;
use std::io;
use std::path::Path;

use codex_infra_extension::StartedAgentHost;
use codex_infra_protocol::*;
use codex_infra_state::InboxEntry;
use codex_infra_state::Journal;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use serde_json::json;

use super::run::LoopState;
use super::run::RunConfig;

#[derive(Clone, Serialize, Deserialize)]
pub struct WorkingContextEntry {
    entry_id: MessageId,
    revision: u64,
    author: MessageAddress,
    root_session_id: RootSessionId,
    task_id: TaskId,
    assignment_id: AssignmentId,
    repo: String,
    commit: CommitId,
    config_commit: CommitId,
    generation: String,
    source_message: MessageId,
    #[serde(flatten)]
    content: Content,
}

#[derive(Clone, Serialize, Deserialize)]
struct Content {
    kind: EntryKind,
    topic: String,
    scope: String,
    body: String,
    source_events: Vec<String>,
    supersedes: Option<MessageId>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum EntryKind {
    Claim,
    Observation,
    Conclusion,
    FailedAttempt,
    ContributionSummary,
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct Filter {
    task_id: Option<TaskId>,
    role: Option<String>,
    topic: Option<String>,
    source_event: Option<String>,
}

impl Filter {
    fn matches(&self, entry: &WorkingContextEntry) -> bool {
        self.task_id.is_none_or(|id| id == entry.task_id)
            && self
                .role
                .as_ref()
                .is_none_or(|role| role == &entry.author.role)
            && self
                .topic
                .as_ref()
                .is_none_or(|topic| topic == &entry.content.topic)
            && self
                .source_event
                .as_ref()
                .is_none_or(|event| entry.content.source_events.contains(event))
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
enum Operation {
    Publish {
        content: Content,
    },
    Query {
        filter: Filter,
        after: Option<u64>,
    },
    Subscribe {
        filter: Filter,
    },
    Unsubscribe,
    Reply {
        entries: Vec<(u64, WorkingContextEntry)>,
        next: Option<u64>,
    },
}

#[derive(Serialize, Deserialize)]
struct WireMessage {
    infra_working_context: Operation,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Stored {
    Entry {
        entry: Box<WorkingContextEntry>,
    },
    Subscription {
        address: MessageAddress,
        filter: Option<Filter>,
    },
}

pub struct WorkingMemory {
    journal: Journal,
    entries: BTreeMap<u64, WorkingContextEntry>,
    subscribers: BTreeMap<AgentId, (MessageAddress, Filter)>,
}

impl WorkingMemory {
    pub fn open(path: &Path) -> io::Result<Self> {
        let mut entries = BTreeMap::new();
        let mut subscribers = BTreeMap::new();
        let journal = Journal::open(path, |record| {
            match serde_json::from_slice(&record.payload)? {
                Stored::Entry { entry } => {
                    entries.insert(record.sequence, *entry);
                }
                Stored::Subscription {
                    address,
                    filter: Some(filter),
                } => {
                    subscribers.insert(address.agent_id, (address, filter));
                }
                Stored::Subscription {
                    address,
                    filter: None,
                } => {
                    subscribers.remove(&address.agent_id);
                }
            }
            Ok(())
        })?;
        Ok(Self {
            journal,
            entries,
            subscribers,
        })
    }
}

pub fn tool(
    host: &StartedAgentHost,
    config: &RunConfig,
    bootstrap: &InboxEntry,
    arguments: &Value,
    state: &mut LoopState,
) -> io::Result<String> {
    let action = arguments["action"]
        .as_str()
        .ok_or_else(|| io::Error::other("memory action required"))?;
    let owner = if action == "publish" {
        host.launch.workspace.agent_id
    } else {
        serde_json::from_value(arguments["owner"].clone())?
    };
    let directory = super::directory::read(&config.directory_file)?;
    let recipient = directory
        .get(&owner)
        .ok_or_else(|| io::Error::other("memory owner missing from directory"))?;
    let operation = match action {
        "publish" => Operation::Publish {
            content: serde_json::from_value(arguments["content"].clone())?,
        },
        "query" => Operation::Query {
            filter: serde_json::from_value(
                arguments
                    .get("filter")
                    .cloned()
                    .unwrap_or_else(|| json!({})),
            )?,
            after: serde_json::from_value(arguments["after"].clone())?,
        },
        "subscribe" => Operation::Subscribe {
            filter: serde_json::from_value(
                arguments
                    .get("filter")
                    .cloned()
                    .unwrap_or_else(|| json!({})),
            )?,
        },
        "unsubscribe" => Operation::Unsubscribe,
        _ => {
            return Err(io::Error::other(
                "choose publish, query, subscribe or unsubscribe",
            ));
        }
    };
    let message = outgoing(
        host,
        bootstrap,
        state,
        MessageAddress {
            agent_id: owner,
            machine_id: recipient.machine_id.clone(),
            role: recipient.role.clone(),
        },
        operation,
    )?;
    let id = message.message_id;
    state.pending.push(message);
    Ok(json!({"message_id":id,"delivery":"queued for tmux at turn end"}).to_string())
}

/// Services semantic requests from accepted tmux inputs without invoking a model.
/// Reply payloads continue through the normal input submission path.
pub fn receive(
    host: &StartedAgentHost,
    bootstrap: &InboxEntry,
    message: &AgentMessage,
    state: &mut LoopState,
) -> io::Result<bool> {
    if message.kind != MessageKind::WorkingContext {
        return Ok(false);
    }
    let Ok(wire) = serde_json::from_str::<WireMessage>(&message.body) else {
        return Ok(false);
    };
    let mut replies = Vec::new();
    match wire.infra_working_context {
        Operation::Publish { content } => {
            if message.from.agent_id != host.launch.workspace.agent_id {
                return Err(io::Error::other(
                    "publish working memory through its author's host",
                ));
            }
            let revision = match content.supersedes {
                Some(id) => {
                    state
                        .memory
                        .entries
                        .values()
                        .find(|entry| entry.entry_id == id)
                        .ok_or_else(|| {
                            io::Error::other("superseded entry is not in this author's memory")
                        })?
                        .revision
                        + 1
                }
                None => 1,
            };
            let entry = WorkingContextEntry {
                entry_id: message.message_id,
                revision,
                author: message.from.clone(),
                root_session_id: message.root_session_id,
                task_id: message.task_id,
                assignment_id: message.assignment_id,
                repo: message.repo.clone(),
                commit: message.commit.clone(),
                config_commit: host.launch.generation.config_commit.clone(),
                generation: host
                    .launch
                    .generation
                    .config_store_path
                    .display()
                    .to_string(),
                source_message: message.reply_to.unwrap_or(bootstrap.message.message_id),
                content,
            };
            if serde_json::to_vec(&entry)?.len() > 1800 {
                return Err(io::Error::other(
                    "working memory entry exceeds 1800 bytes; split the observation",
                ));
            }
            let sequence = state
                .memory
                .journal
                .append(&serde_json::to_vec(&Stored::Entry {
                    entry: Box::new(entry.clone()),
                })?)?;
            for (address, filter) in state.memory.subscribers.values() {
                if filter.matches(&entry) {
                    replies.push((
                        address.clone(),
                        Operation::Reply {
                            entries: vec![(sequence, entry.clone())],
                            next: None,
                        },
                    ));
                }
            }
            state.memory.entries.insert(sequence, entry);
        }
        Operation::Query { filter, after } => {
            let mut selected = state.memory.entries.iter().filter(|(sequence, entry)| {
                after.is_none_or(|after| **sequence > after) && filter.matches(entry)
            });
            let entries: Vec<_> = selected
                .next()
                .map(|(sequence, entry)| (*sequence, entry.clone()))
                .into_iter()
                .collect();
            let next = if selected.next().is_some() {
                entries.last().map(|(sequence, _)| *sequence)
            } else {
                None
            };
            replies.push((message.from.clone(), Operation::Reply { entries, next }));
        }
        operation @ (Operation::Subscribe { .. } | Operation::Unsubscribe) => {
            let filter = match operation {
                Operation::Subscribe { filter } => Some(filter),
                Operation::Unsubscribe => None,
                Operation::Publish { .. } | Operation::Query { .. } | Operation::Reply { .. } => {
                    unreachable!()
                }
            };
            state
                .memory
                .journal
                .append(&serde_json::to_vec(&Stored::Subscription {
                    address: message.from.clone(),
                    filter: filter.clone(),
                })?)?;
            if let Some(filter) = filter {
                state
                    .memory
                    .subscribers
                    .insert(message.from.agent_id, (message.from.clone(), filter));
            } else {
                state.memory.subscribers.remove(&message.from.agent_id);
            }
        }
        Operation::Reply { .. } => return Ok(false),
    }
    for (recipient, operation) in replies {
        let mut reply = outgoing(host, bootstrap, state, recipient, operation)?;
        reply.reply_to = Some(message.message_id);
        reply.task_id = message.task_id;
        reply.assignment_id = message.assignment_id;
        state.pending.push(reply);
    }
    Ok(true)
}

fn outgoing(
    host: &StartedAgentHost,
    bootstrap: &InboxEntry,
    state: &LoopState,
    to: MessageAddress,
    operation: Operation,
) -> io::Result<AgentMessage> {
    if !state.role.routes_to_roles.contains(&to.role)
        || !state
            .role
            .produced_output_kinds
            .contains(&MessageKind::WorkingContext)
    {
        return Err(io::Error::other(
            "working memory needs a WorkingContext role route",
        ));
    }
    let body = serde_json::to_string(&WireMessage {
        infra_working_context: operation,
    })?;
    if body.len() > 2100 {
        return Err(io::Error::other(
            "split working memory content into smaller entries",
        ));
    }
    let mut message =
        super::events::message(host, bootstrap, to, MessageKind::WorkingContext, body);
    message.reply_to = Some(state.active_input.message_id);
    Ok(message)
}
