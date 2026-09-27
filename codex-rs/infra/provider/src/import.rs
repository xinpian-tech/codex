use std::collections::BTreeMap;
use std::fs;
use std::fs::OpenOptions;
use std::io;
use std::io::Write;
use std::path::Path;

use codex_infra_protocol::CommitId;

use crate::AccountAuthentication;
use crate::AccountCatalog;
use crate::AccountDefinition;

impl AccountCatalog {
    /// Imports a credential snapshot into a writable Team State catalog. The
    /// revision names the already committed source snapshot; publishing the
    /// updated catalog and realizing a generation belong to the caller.
    /// Existing account headers and all other accounts are preserved.
    pub fn import(
        path: &Path,
        provider_id: String,
        account_id: String,
        credential_revision: CommitId,
        authentication: AccountAuthentication,
    ) -> io::Result<()> {
        let path = std::path::absolute(path)?;
        let parent = path
            .parent()
            .ok_or_else(|| io::Error::other("account catalog parent missing"))?;
        fs::create_dir_all(parent)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path.with_extension("json.lock"))?;
        lock.lock()?;
        let mut catalog = match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice::<Self>(&bytes)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => Self(BTreeMap::new()),
            Err(error) => return Err(error),
        };
        let accounts = catalog.0.entry(provider_id).or_default();
        match accounts.get_mut(&account_id) {
            Some(account) => {
                account.credential_revision = credential_revision;
                account.authentication = authentication;
            }
            None => {
                accounts.insert(
                    account_id,
                    AccountDefinition {
                        credential_revision,
                        authentication,
                        headers: BTreeMap::new(),
                    },
                );
            }
        }
        let temporary = path.with_extension("json.pending");
        let mut output = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)?;
        serde_json::to_writer_pretty(&mut output, &catalog)?;
        output.write_all(b"\n")?;
        output.sync_all()?;
        drop(output);
        fs::rename(temporary, &path)?;
        #[cfg(unix)]
        fs::File::open(parent)?.sync_all()?;
        Ok(())
    }
}
