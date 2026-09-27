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
                let revision: CommitId = target.parse().map_err(io::Error::other)?;
                if current != target {
                    let before = state.before.ok_or_else(|| {
                        io::Error::other("publication ref changed before push recovery")
                    })?;
                    if current != before {
                        return Err(io::Error::other("publication requires ref integration"));
                    }
                    self.run(&["update-ref", &self.config_ref, &target, &before], &[])?;
                    audit.record(json!({"event": "ref_updated", "config_commit": target}))?;
                }
                self.push(&target, &mut audit)?;
                return Ok(revision);
            }
            match state.details["kind"].as_str() {
                Some("sync") => {
                    let revision = current.parse().map_err(io::Error::other)?;
                    self.push(&current, &mut audit)?;
                    Ok(revision)
                }
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
