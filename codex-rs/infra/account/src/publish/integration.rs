use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io;
use std::path::PathBuf;

use codex_infra_protocol::CommitId;
use codex_infra_protocol::MessageId;
use serde_json::Value;
use serde_json::json;

use super::GitAccounts;
use super::IndexFile;
use super::audit::PublicationAudit;
use super::read::FetchedRef;
use super::run;

type Catalog = BTreeMap<String, BTreeMap<String, Value>>;

impl GitAccounts {
    pub(super) fn integrate_and_push(
        &self,
        local: &str,
        audit: &mut PublicationAudit,
    ) -> io::Result<CommitId> {
        match self.push(local, audit) {
            Ok(()) => return local.parse().map_err(io::Error::other),
            Err(error) => audit
                .record(json!({"event": "push_retry_preparing", "error": error.to_string()}))?,
        }
        let fetched = FetchedRef {
            git: self,
            name: format!("refs/codex-infra/account-integration/{}", MessageId::new()),
        };
        self.run(
            &[
                "fetch",
                "--no-tags",
                "--no-write-fetch-head",
                "--",
                &self.remote,
                &format!("{}:{}", self.config_ref, fetched.name),
            ],
            &[],
        )?;
        let remote = self.run(&["rev-parse", "--verify", &fetched.name], &[])?;
        let base = self.run(&["merge-base", local, &remote], &[])?;
        audit.record(json!({"event": "integration_requested", "local": local, "remote": remote, "base": base}))?;
        let commit = if base == local {
            remote
        } else if base == remote {
            local.to_owned()
        } else {
            self.merge_account_trees(&base, local, &remote)?
        };
        if commit != local {
            audit.record(
                json!({"event": "ref_update_requested", "before": local, "config_commit": commit}),
            )?;
            self.run(&["update-ref", &self.config_ref, &commit, local], &[])?;
            audit.record(json!({"event": "ref_updated", "config_commit": commit}))?;
        }
        self.push(&commit, audit)?;
        commit.parse().map_err(io::Error::other)
    }

    pub(super) fn merge_account_trees(
        &self,
        base: &str,
        local: &str,
        remote: &str,
    ) -> io::Result<String> {
        let path = self.run(
            &[
                "rev-parse",
                "--path-format=absolute",
                "--git-path",
                &format!("infra-account-merge-{}.index", MessageId::new()),
            ],
            &[],
        )?;
        let index = IndexFile(PathBuf::from(path));
        let mut command = self.command();
        command.env("GIT_INDEX_FILE", &index.0);
        run(
            &mut command,
            &["read-tree", "-i", "-m", base, local, remote],
            &[],
        )?;
        let mut command = self.command();
        command.env("GIT_INDEX_FILE", &index.0);
        let conflicts = run(&mut command, &["ls-files", "--unmerged", "-z"], &[])?;
        let mut catalog_conflict = false;
        for entry in conflicts.split('\0').filter(|entry| !entry.is_empty()) {
            let (_, path) = entry
                .split_once('\t')
                .ok_or_else(|| io::Error::other("invalid Git index entry"))?;
            if path != "accounts/catalog.json" {
                return Err(io::Error::other(format!(
                    "config integration conflict: {path}"
                )));
            }
            catalog_conflict = true;
        }
        if catalog_conflict {
            let base = self.catalog_at(base)?;
            let local = self.catalog_at(local)?;
            let remote = self.catalog_at(remote)?;
            let providers: BTreeSet<_> = base
                .keys()
                .chain(local.keys())
                .chain(remote.keys())
                .collect();
            let mut merged = Catalog::new();
            for provider in providers {
                let keys: BTreeSet<_> = base
                    .get(provider)
                    .into_iter()
                    .flat_map(BTreeMap::keys)
                    .chain(local.get(provider).into_iter().flat_map(BTreeMap::keys))
                    .chain(remote.get(provider).into_iter().flat_map(BTreeMap::keys))
                    .collect();
                for account in keys {
                    let before = base
                        .get(provider)
                        .and_then(|accounts| accounts.get(account));
                    let left = local
                        .get(provider)
                        .and_then(|accounts| accounts.get(account));
                    let right = remote
                        .get(provider)
                        .and_then(|accounts| accounts.get(account));
                    let value = if left == before {
                        right
                    } else if right == before || left == right {
                        left
                    } else {
                        return Err(io::Error::other(format!(
                            "account revision conflict: {provider}/{account}"
                        )));
                    };
                    if let Some(value) = value {
                        merged
                            .entry(provider.clone())
                            .or_default()
                            .insert(account.clone(), value.clone());
                    }
                }
            }
            let blob = self.run(
                &["hash-object", "-w", "--stdin"],
                &serde_json::to_vec_pretty(&merged)?,
            )?;
            let mut command = self.command();
            command.env("GIT_INDEX_FILE", &index.0);
            run(
                &mut command,
                &[
                    "update-index",
                    "--add",
                    "--cacheinfo",
                    "100644",
                    &blob,
                    "accounts/catalog.json",
                ],
                &[],
            )?;
        }
        let mut command = self.command();
        command.env("GIT_INDEX_FILE", &index.0);
        let tree = run(&mut command, &["write-tree"], &[])?;
        self.run(
            &["commit-tree", &tree, "-p", local, "-p", remote],
            b"Integrate published account configurations\n",
        )
    }

    fn catalog_at(&self, commit: &str) -> io::Result<Catalog> {
        if self
            .run(
                &[
                    "ls-tree",
                    "--name-only",
                    commit,
                    "--",
                    "accounts/catalog.json",
                ],
                &[],
            )?
            .is_empty()
        {
            return Ok(Catalog::new());
        }
        serde_json::from_str(&self.run(&["show", &format!("{commit}:accounts/catalog.json")], &[])?)
            .map_err(io::Error::other)
    }
}
