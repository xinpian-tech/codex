use std::any::Any;
use std::sync::Arc;

use codex_protocol::ThreadId;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_thread_store as store;
use codex_thread_store::ThreadStore;
use codex_thread_store::ThreadStoreFuture;
use codex_thread_store::ThreadStoreResult;
use serde::Serialize;
use serde_json::Value;

use crate::store_audit::StoreAudit;
use crate::store_audit::StoreAuditEvent;
use crate::store_audit::audit_error;

/// Preserves the backing store API while retaining raw writes and lifecycle
/// operations in an append-only journal, before rollout filtering or deletion.
pub struct AuditedThreadStore {
    inner: Arc<dyn ThreadStore>,
    audit: StoreAudit,
}

impl AuditedThreadStore {
    /// Decorates the host's default store with an already prepared audit writer.
    pub fn new(inner: Arc<dyn ThreadStore>, audit: StoreAudit) -> Self {
        Self { inner, audit }
    }

    async fn operation<'a, T>(
        &'a self,
        operation: &'static str,
        payload: Value,
        run: impl FnOnce() -> ThreadStoreFuture<'a, T>,
        output: impl FnOnce(&T) -> serde_json::Result<Value>,
    ) -> ThreadStoreResult<T> {
        let started_sequence = self
            .audit
            .append(StoreAuditEvent::Started {
                operation: operation.to_owned(),
                payload,
            })
            .await?;
        let result = run().await;
        let outcome = match &result {
            Ok(value) => Ok(output(value).map_err(audit_error)?),
            Err(error) => Err(error.to_string()),
        };
        self.audit
            .append(StoreAuditEvent::Finished {
                started_sequence,
                outcome,
            })
            .await?;
        result
    }
}

macro_rules! audited {
    ($($name:ident($param:ident: $input:ty) -> $output:ty;)+) => {$(
        fn $name(&self, $param: $input) -> ThreadStoreFuture<'_, $output> {
            Box::pin(async move {
                let payload = serde_json::to_value(&$param).map_err(audit_error)?;
                self.operation(
                    stringify!($name),
                    payload,
                    || self.inner.$name($param),
                    serialize_value,
                ).await
            })
        }
    )+};
}

macro_rules! delegated {
    ($($name:ident($param:ident: $input:ty) -> $output:ty;)+) => {$(
        fn $name(&self, $param: $input) -> ThreadStoreFuture<'_, $output> {
            self.inner.$name($param)
        }
    )+};
}

macro_rules! mapped {
    ($($name:ident($param:ident: $input:ty) -> $output:ty {
        input: $encode:expr;
        output: |$value:ident| $decode:expr;
    })+) => {$(
        fn $name(&self, $param: $input) -> ThreadStoreFuture<'_, $output> {
            Box::pin(async move {
                let payload = $encode.map_err(audit_error)?;
                self.operation(
                    stringify!($name), payload,
                    || self.inner.$name($param),
                    |$value: &$output| $decode,
                ).await
            })
        }
    )+};
}

impl ThreadStore for AuditedThreadStore {
    fn as_any(&self) -> &dyn Any {
        // Preserve the existing local-store migration escape hatch.
        self.inner.as_any()
    }

    fn default_history_mode(&self) -> ThreadHistoryMode {
        self.inner.default_history_mode()
    }

    fn supports_thread_sections(&self) -> bool {
        self.inner.supports_thread_sections()
    }

    fn supports_thread_attachments(&self) -> bool {
        self.inner.supports_thread_attachments()
    }

    fn supports_projects(&self) -> bool {
        self.inner.supports_projects()
    }

    fn supports_paginated_history_lists(&self) -> bool {
        self.inner.supports_paginated_history_lists()
    }

    audited! {
        create_thread(params: store::CreateThreadParams) -> ();
        resume_thread(params: store::ResumeThreadParams) -> ();
        record_thread_metadata(params: store::UpdateThreadMetadataParams) -> ();
        update_thread_metadata(params: store::UpdateThreadMetadataParams) -> Option<store::StoredThread>;
        remove_pending_thread_metadata(thread_id: ThreadId) -> ();
        flush_thread(thread_id: ThreadId) -> ();
        shutdown_thread(thread_id: ThreadId) -> ();
        discard_thread(thread_id: ThreadId) -> ();
        archive_thread(params: store::ArchiveThreadParams) -> ();
        archive_threads(params: store::ArchiveThreadsParams) -> Vec<ThreadId>;
        unarchive_thread(params: store::ArchiveThreadParams) -> store::StoredThread;
        delete_thread(params: store::DeleteThreadParams) -> ();
        delete_threads(params: store::DeleteThreadsParams) -> ();
        move_thread_to_section(params: store::MoveThreadToSectionParams) -> ();
    }

    delegated! {
        read_pending_thread_metadata(thread_id: ThreadId) -> Option<store::ThreadMetadataPatch>;
        load_history(params: store::LoadThreadHistoryParams) -> store::StoredThreadHistory;
        load_latest_model_context(params: store::LoadThreadHistoryParams) -> store::StoredModelContext;
        read_thread(params: store::ReadThreadParams) -> store::StoredThread;
        read_thread_by_rollout_path(params: store::ReadThreadByRolloutPathParams) -> store::StoredThread;
        list_threads(params: store::ListThreadsParams) -> store::ThreadPage;
        list_thread_sections(params: store::ListThreadSectionsParams) -> store::StoredThreadSectionsPage;
        list_thread_attachments(params: store::ListThreadAttachmentsParams) -> store::ThreadAttachmentPage;
        list_projects(params: store::ListProjectsParams) -> store::StoredProjectsPage;
        read_project(project_id: String) -> Option<store::StoredProject>;
        search_threads(params: store::SearchThreadsParams) -> store::ThreadSearchPage;
        search_thread_occurrences(params: store::SearchThreadOccurrencesParams) -> store::ThreadOccurrenceSearchPage;
        list_turns(params: store::ListTurnsParams) -> store::TurnPage;
        list_items(params: store::ListItemsParams) -> store::ItemPage;
        list_timeline(params: store::ListTimelineParams) -> store::TimelinePage;
    }

    mapped! {
        create_thread_section(params: store::CreateThreadSectionParams) -> store::StoredThreadSection {
            input: serde_json::to_value((&params.name, &params.appearance));
            output: |value| serde_json::to_value((&value.id, &value.name, &value.appearance));
        }
        rename_thread_section(params: store::RenameThreadSectionParams) -> Option<store::StoredThreadSection> {
            input: serde_json::to_value((&params.section_id, &params.name, &params.appearance));
            output: |value| serde_json::to_value(value.as_ref().map(|section| (&section.id, &section.name, &section.appearance)));
        }
        delete_thread_section(params: store::DeleteThreadSectionParams) -> bool {
            input: serde_json::to_value(&params.section_id);
            output: |value| serde_json::to_value(value);
        }
        add_thread_attachment(params: store::AddThreadAttachmentParams) -> store::AddThreadAttachmentOutcome {
            input: serde_json::to_value((params.thread_id, &params.attachment_type, &params.identity_key, &params.payload));
            output: |value| {
                let (kind, attachment) = match value {
                    store::AddThreadAttachmentOutcome::Created(attachment) => ("created", attachment),
                    store::AddThreadAttachmentOutcome::Existing(attachment) => ("existing", attachment),
                };
                serde_json::to_value((kind, &attachment.id, attachment.thread_id, &attachment.attachment_type, &attachment.identity_key, &attachment.payload, attachment.created_at))
            };
        }
        remove_thread_attachment(params: store::RemoveThreadAttachmentParams) -> store::RemoveThreadAttachmentOutcome {
            input: serde_json::to_value((params.thread_id, &params.attachment_type, &params.identity_key));
            output: |value| match value {
                store::RemoveThreadAttachmentOutcome::Removed(attachment) => serde_json::to_value(("removed", &attachment.id, attachment.thread_id, &attachment.attachment_type, &attachment.identity_key, &attachment.payload, attachment.created_at)),
                store::RemoveThreadAttachmentOutcome::NotFound => serde_json::to_value("not_found"),
            };
        }
        create_project(params: store::CreateProjectParams) -> store::CreatedProject {
            input: serde_json::to_value((&params.name, params.roots.iter().map(|root| &root.path).collect::<Vec<_>>(), &params.metadata, &params.thread_ids, &params.idempotency_key));
            output: |value| serde_json::to_value((project_payload(&value.project)?, value.created));
        }
        update_project(params: store::UpdateProjectParams) -> Option<store::UpdatedProject> {
            input: serde_json::to_value((&params.project_id, &params.name, params.roots.as_ref().map(|roots| roots.iter().map(|root| &root.path).collect::<Vec<_>>()), &params.metadata));
            output: |value| serde_json::to_value(value.as_ref().map(|updated| Ok::<_, serde_json::Error>((project_payload(&updated.project)?, updated.changed))).transpose()?);
        }
        move_project(params: store::MoveProjectParams) -> Option<store::ProjectMoveOutcome> {
            input: serde_json::to_value((&params.project_id, &params.before_project_id));
            output: |value| serde_json::to_value(value.as_ref().map(|outcome| match outcome {
                store::ProjectMoveOutcome::Unchanged => "unchanged",
                store::ProjectMoveOutcome::Moved => "moved",
            }));
        }
        delete_project(project_id: String) -> Option<store::DeletedProject> {
            input: serde_json::to_value(&project_id);
            output: |value| serde_json::to_value(value.as_ref().map(|deleted| (&deleted.affected_active_thread_ids, &deleted.affected_archived_thread_ids)));
        }
    }

    fn append_items(&self, params: store::AppendThreadItemsParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            let payload =
                serde_json::to_value((&params.thread_id, &params.items)).map_err(audit_error)?;
            self.operation(
                "append_items",
                payload,
                || self.inner.append_items(params),
                serialize_value,
            )
            .await
        })
    }

    fn stage_pending_thread_metadata(
        &self,
        thread_id: ThreadId,
        patch: store::ThreadMetadataPatch,
    ) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            let payload = serde_json::to_value((thread_id, &patch)).map_err(audit_error)?;
            self.operation(
                "stage_pending_thread_metadata",
                payload,
                || self.inner.stage_pending_thread_metadata(thread_id, patch),
                serialize_value,
            )
            .await
        })
    }

    fn persist_thread(
        &self,
        thread_id: ThreadId,
        context: store::PersistContext,
    ) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            let context_name = match context {
                store::PersistContext::ThreadPreparation => "thread_preparation",
                store::PersistContext::Standard => "standard",
                store::PersistContext::SubagentSpawn => "subagent_spawn",
                store::PersistContext::TurnStart => "turn_start",
                store::PersistContext::SteeredUserInput => "steered_user_input",
            };
            let payload = serde_json::to_value((thread_id, context_name)).map_err(audit_error)?;
            self.operation(
                "persist_thread",
                payload,
                || self.inner.persist_thread(thread_id, context),
                serialize_value,
            )
            .await
        })
    }

    fn prepare_fork(
        &self,
        params: store::PrepareForkParams,
    ) -> ThreadStoreFuture<'_, store::PreparedFork> {
        Box::pin(async move {
            let boundary = match &params.boundary {
                store::ForkBoundary::Latest => ("latest", None),
                store::ForkBoundary::ThroughTurn(turn) => ("through_turn", Some(turn)),
                store::ForkBoundary::BeforeTurn(turn) => ("before_turn", Some(turn)),
            };
            let payload =
                serde_json::to_value((params.thread_id, boundary)).map_err(audit_error)?;
            self.operation(
                "prepare_fork",
                payload,
                || self.inner.prepare_fork(params),
                |fork| {
                    serde_json::to_value((
                        fork.source_thread_id,
                        &fork.history_base,
                        &fork.model_context,
                    ))
                },
            )
            .await
        })
    }

    fn revert_thread(&self, params: store::RevertThreadParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            let payload = serde_json::to_value((
                params.thread_id,
                &params.before_turn_id,
                params.multi_agent_version,
            ))
            .map_err(audit_error)?;
            self.operation(
                "revert_thread",
                payload,
                || self.inner.revert_thread(params),
                serialize_value,
            )
            .await
        })
    }

    fn copy_thread_attachments(
        &self,
        source_thread_id: ThreadId,
        destination_thread_id: ThreadId,
    ) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            let payload = serde_json::to_value((source_thread_id, destination_thread_id))
                .map_err(audit_error)?;
            self.operation(
                "copy_thread_attachments",
                payload,
                || {
                    self.inner
                        .copy_thread_attachments(source_thread_id, destination_thread_id)
                },
                serialize_value,
            )
            .await
        })
    }
}

fn project_payload(project: &store::StoredProject) -> serde_json::Result<Value> {
    serde_json::to_value((
        &project.id,
        &project.name,
        project
            .roots
            .iter()
            .map(|root| &root.path)
            .collect::<Vec<_>>(),
        &project.metadata,
        project.position,
        project.created_at_ms,
        project.updated_at_ms,
        project.recency_at_ms,
    ))
}

fn serialize_value<T: Serialize>(value: &T) -> serde_json::Result<Value> {
    serde_json::to_value(value)
}
