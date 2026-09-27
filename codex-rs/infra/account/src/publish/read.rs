use std::io;

use codex_infra_protocol::CommitId;
use codex_infra_protocol::MessageId;
use codex_infra_provider::AccountAuthentication;
use codex_infra_provider::AccountCatalog;

use super::GitAccounts;
use crate::PublishedAccount;

struct FetchedRef<'a> {
    git: &'a GitAccounts,
    name: String,
}

impl Drop for FetchedRef<'_> {
    fn drop(&mut self) {
        let _ = self.git.run(&["update-ref", "-d", &self.name], &[]);
    }
}

impl GitAccounts {
    /// Fetches the remote config ref into a unique temporary ref and reads the
    /// exact returned tree. Does not advance the local publication ref or use
    /// shared FETCH_HEAD to identify the snapshot. Run on a blocking worker.
    pub fn read_published_codex(
        &self,
        provider_id: &str,
        account_id: &str,
    ) -> io::Result<PublishedAccount> {
        let fetched = FetchedRef {
            git: self,
            name: format!("refs/codex-infra/account-reads/{}", MessageId::new()),
        };
        let refspec = format!("{}:{}", self.config_ref, fetched.name);
        self.run(
            &[
                "fetch",
                "--no-tags",
                "--no-write-fetch-head",
                "--",
                &self.remote,
                &refspec,
            ],
            &[],
        )?;
        let commit = self.run(&["rev-parse", "--verify", &fetched.name], &[])?;
        let config_commit: CommitId = commit.parse().map_err(io::Error::other)?;
        let catalog_path = format!("{config_commit}:accounts/catalog.json");
        let mut catalog: AccountCatalog =
            serde_json::from_str(&self.run(&["show", &catalog_path], &[])?)?;
        let account = catalog
            .0
            .get_mut(provider_id)
            .and_then(|accounts| accounts.remove(account_id))
            .ok_or_else(|| io::Error::other("account is absent from published catalog"))?;
        // A catalog can refer to an earlier credential snapshot, including one
        // imported from an external login flow, but that snapshot is part of
        // the published history whose tree was just fetched.
        self.run(
            &[
                "merge-base",
                "--is-ancestor",
                &account.credential_revision.to_string(),
                &commit,
            ],
            &[],
        )?;
        let auth = match account.authentication {
            AccountAuthentication::CodexLogin { auth } => serde_json::from_value(auth)?,
            AccountAuthentication::BearerToken { .. }
            | AccountAuthentication::HeaderToken { .. } => {
                return Err(io::Error::other("published account is not a Codex login"));
            }
        };
        Ok(PublishedAccount {
            provider_id: provider_id.to_owned(),
            account_id: account_id.to_owned(),
            credential_revision: account.credential_revision,
            config_commit,
            auth,
        })
    }
}
