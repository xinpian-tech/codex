use std::io;

use codex_infra_protocol::CommitId;

use super::GitAccounts;
use crate::AccountOwnerAssignment;

impl GitAccounts {
    pub(crate) fn confirm_owner(
        &self,
        config_commit: &CommitId,
        expected: &AccountOwnerAssignment,
        owner_revision: &CommitId,
    ) -> io::Result<()> {
        let path = expected.path()?;
        let source = format!("{config_commit}:{path}");
        let actual: AccountOwnerAssignment =
            serde_json::from_str(&self.run(&["show", &source], &[])?)?;
        if actual.provider_id != expected.provider_id
            || actual.account_id != expected.account_id
            || actual.machine_id != expected.machine_id
        {
            return Err(io::Error::other(
                "account refresh is assigned to another machine",
            ));
        }
        let revision = self.run(
            &[
                "log",
                "-1",
                "--format=%H",
                &config_commit.to_string(),
                "--",
                &path,
            ],
            &[],
        )?;
        if revision != owner_revision.to_string() {
            return Err(io::Error::other(
                "account refresh assignment revision changed",
            ));
        }
        Ok(())
    }
}
