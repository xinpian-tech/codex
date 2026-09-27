use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;
use std::process::Stdio;

use codex_infra_protocol::CommitId;
use codex_infra_protocol::MessageId;
use codex_infra_provider::AccountAuthentication;
use codex_infra_provider::AccountCatalog;
use codex_infra_provider::AccountDefinition;
use serde_json::json;

/// Uses an isolated Git index; the Team State checkout and its staged changes
/// are independent of the config ref being published.
pub struct GitAccounts {
    pub git: PathBuf,
    pub repository: PathBuf,
    pub remote: String,
    pub config_ref: String,
}

struct IndexFile(PathBuf);

impl Drop for IndexFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

impl GitAccounts {
    /// Commits the credential first, then records that actual commit in the
    /// catalog. A failed push leaves both commits reachable through config_ref;
    /// sync retries publication without importing the credential again.
    pub fn publish(
        &self,
        provider: String,
        account: String,
        authentication: AccountAuthentication,
    ) -> io::Result<CommitId> {
        let before = self.run(&["rev-parse", "--verify", &self.config_ref], &[])?;
        let _: CommitId = before.parse().map_err(io::Error::other)?;
        let source = format!("{before}:accounts/catalog.json");
        let present = self.run(
            &[
                "ls-tree",
                "--name-only",
                &before,
                "--",
                "accounts/catalog.json",
            ],
            &[],
        )?;
        let mut catalog = if present.is_empty() {
            AccountCatalog(BTreeMap::new())
        } else {
            serde_json::from_str(&self.run(&["show", &source], &[])?)?
        };
        let operation = MessageId::new();
        let git_path = self.run(
            &[
                "rev-parse",
                "--path-format=absolute",
                "--git-path",
                &format!("infra-account-{operation}.index"),
            ],
            &[],
        )?;
        let index = IndexFile(PathBuf::from(git_path));
        let snapshot = serde_json::to_vec(&json!({
            "provider_id": provider,
            "account_id": account,
            "authentication": authentication,
        }))?;
        let credential_blob = self.run(&["hash-object", "-w", "--stdin"], &snapshot)?;
        let mut command = self.command();
        command.env("GIT_INDEX_FILE", &index.0);
        run(&mut command, &["read-tree", &before], &[])?;
        let credential_path = format!("accounts/credentials/{operation}.json");
        let mut command = self.command();
        command.env("GIT_INDEX_FILE", &index.0);
        run(
            &mut command,
            &[
                "update-index",
                "--add",
                "--cacheinfo",
                "100644",
                &credential_blob,
                &credential_path,
            ],
            &[],
        )?;
        let mut command = self.command();
        command.env("GIT_INDEX_FILE", &index.0);
        let tree = run(&mut command, &["write-tree"], &[])?;
        let credential = self.run(
            &["commit-tree", &tree, "-p", &before],
            format!("Account {provider}/{account} credential {operation}\n").as_bytes(),
        )?;
        let credential_revision = credential.parse().map_err(io::Error::other)?;
        let accounts = catalog.0.entry(provider.clone()).or_default();
        let headers = accounts
            .remove(&account)
            .map(|entry| entry.headers)
            .unwrap_or_default();
        accounts.insert(
            account.clone(),
            AccountDefinition {
                credential_revision,
                authentication,
                headers,
            },
        );
        let catalog_blob = self.run(
            &["hash-object", "-w", "--stdin"],
            &serde_json::to_vec_pretty(&catalog)?,
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
                &catalog_blob,
                "accounts/catalog.json",
            ],
            &[],
        )?;
        let mut command = self.command();
        command.env("GIT_INDEX_FILE", &index.0);
        let tree = run(&mut command, &["write-tree"], &[])?;
        let commit = self.run(
            &["commit-tree", &tree, "-p", &credential],
            format!("Bind account {provider}/{account} to credential {credential}\n").as_bytes(),
        )?;
        self.run(&["update-ref", &self.config_ref, &commit, &before], &[])?;
        self.push(&commit)?;
        commit.parse().map_err(io::Error::other)
    }

    pub fn sync(&self) -> io::Result<CommitId> {
        let commit = self.run(&["rev-parse", "--verify", &self.config_ref], &[])?;
        let revision = commit.parse().map_err(io::Error::other)?;
        self.push(&commit)?;
        Ok(revision)
    }

    fn push(&self, commit: &str) -> io::Result<()> {
        let destination = format!("{commit}:{}", self.config_ref);
        self.run(&["push", "--", &self.remote, &destination], &[])?;
        let observed = self.run(
            &["ls-remote", "--refs", "--", &self.remote, &self.config_ref],
            &[],
        )?;
        if !observed.lines().any(|line| {
            line.split_once('\t')
                .is_some_and(|(oid, name)| oid == commit && name == self.config_ref)
        }) {
            return Err(io::Error::other(
                "published account ref differs from commit",
            ));
        }
        Ok(())
    }

    fn command(&self) -> Command {
        let mut command = Command::new(&self.git);
        command.current_dir(&self.repository);
        command
    }

    fn run(&self, args: &[&str], input: &[u8]) -> io::Result<String> {
        run(&mut self.command(), args, input)
    }
}

fn run(command: &mut Command, args: &[&str], input: &[u8]) -> io::Result<String> {
    let mut child = command
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let written = match child.stdin.take() {
        Some(mut stdin) => stdin.write_all(input),
        None => Err(io::Error::other("Git stdin unavailable")),
    };
    let output = child.wait_with_output()?;
    written?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "Git {}: {}",
            args[0],
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok(String::from_utf8(output.stdout)
        .map_err(io::Error::other)?
        .trim()
        .to_owned())
}
