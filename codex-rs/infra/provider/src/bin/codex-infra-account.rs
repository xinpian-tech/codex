use std::io;
use std::io::Read;
use std::path::Path;

use codex_infra_protocol::CommitId;
use codex_infra_provider::AccountAuthentication;
use codex_infra_provider::AccountCatalog;

const USAGE: &str = "usage: codex-infra-account import <catalog.json> <provider> <account> <credential-commit> <bearer|header:NAME|codex-login> < credential-on-stdin";

fn main() -> io::Result<()> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() != 6 || args[0] != "import" {
        return Err(io::Error::other(USAGE));
    }
    let text = |index: usize| {
        args[index]
            .to_str()
            .ok_or_else(|| io::Error::other("account arguments must be UTF-8"))
    };
    let provider = text(2)?.to_owned();
    let account = text(3)?.to_owned();
    let revision: CommitId = text(4)?.parse().map_err(io::Error::other)?;
    let mode = text(5)?;
    let mut input = String::new();
    io::stdin().read_to_string(&mut input)?;
    let authentication = match mode {
        "bearer" => AccountAuthentication::BearerToken {
            token: input.trim_end_matches(['\r', '\n']).to_owned(),
        },
        "codex-login" => AccountAuthentication::CodexLogin {
            auth: serde_json::from_str(&input)?,
        },
        header if header.starts_with("header:") => AccountAuthentication::HeaderToken {
            name: header[7..].to_owned(),
            value: input.trim_end_matches(['\r', '\n']).to_owned(),
        },
        _ => return Err(io::Error::other(USAGE)),
    };
    AccountCatalog::import(
        Path::new(&args[1]),
        provider,
        account,
        revision,
        authentication,
    )
}
