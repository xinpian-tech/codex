use std::io;

use codex_infra_protocol::CommitId;
use codex_infra_protocol::MessageId;
use serde_json::json;

use super::GitAccounts;
use super::PublicationAccount;
use super::PublicationExpectation;
use super::audit::PublicationAudit;

impl GitAccounts {
    /// Resumes one recorded operation while owning its existing journal writer.
    /// Completed operations return their original acknowledged config commit.
    pub fn recover(&self, operation: MessageId) -> io::Result<CommitId> {
        let (mut audit, state) = PublicationAudit::resume(self, operation)?;
        if state.completed {
            return state
                .target
                .ok_or_else(|| io::Error::other("completed publication commit missing"))?
                .parse()
                .map_err(io::Error::other);
        }
        audit.record(json!({"event": "recovery_started", "git": self.git}))?;
        let result = (|| {
            let current = self.run(&["rev-parse", "--verify", &self.config_ref], &[])?;
            if let Some(target) = state.target {
                let _: CommitId = target.parse().map_err(io::Error::other)?;
                let mut local = target.clone();
                if current != target {
                    if state.before.as_ref() == Some(&current) {
                        self.run(&["update-ref", &self.config_ref, &target, &current], &[])?;
                        audit.record(json!({"event": "ref_updated", "config_commit": target}))?;
                    } else {
                        let base = self.run(&["merge-base", &target, &current], &[])?;
                        if base == target {
                            audit.record(json!({"event": "publication_carried_forward", "target": target, "current": current}))?;
                            local = current;
                        } else {
                            local = if base == current {
                                target
                            } else {
                                self.merge_account_trees(&base, &current, &target)?
                            };
                            audit.record(json!({"event": "ref_update_requested", "before": current, "config_commit": local}))?;
                            self.run(&["update-ref", &self.config_ref, &local, &current], &[])?;
                            audit
                                .record(json!({"event": "ref_updated", "config_commit": local}))?;
                        }
                    }
                }
                return self.integrate_and_push(&local, &mut audit);
            }
            match state.details["kind"].as_str() {
                Some("sync") => self.integrate_and_push(&current, &mut audit),
                Some("publish") => {
                    let expected: Option<PublicationExpectation> =
                        serde_json::from_value(state.details["expected"].clone())?;
                    let mut before = state.before.unwrap_or(current.clone());
                    if current != before {
                        if expected.is_none() {
                            return Err(io::Error::other("publication base requires integration"));
                        }
                        self.run(&["merge-base", "--is-ancestor", &before, &current], &[])?;
                        audit.record(json!({
                            "event": "base_reselected", "before": before, "current": current,
                        }))?;
                        before = current;
                    }
                    let provider = state.details["provider_id"]
                        .as_str()
                        .ok_or_else(|| io::Error::other("publication provider missing"))?
                        .to_owned();
                    let account = state.details["account_id"]
                        .as_str()
                        .ok_or_else(|| io::Error::other("publication account missing"))?
                        .to_owned();
                    let authentication =
                        serde_json::from_value(state.details["authentication"].clone())?;
                    self.publish_recorded(
                        PublicationAccount {
                            provider,
                            account,
                            authentication,
                            expected,
                        },
                        operation,
                        &mut audit,
                        before,
                    )
                }
                _ => Err(io::Error::other("publication kind missing")),
            }
        })();
        audit.finish(&result)?;
        result
    }
}
