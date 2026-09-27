use std::io;
use std::io::Read;
use std::path::Path;

use codex_infra_protocol::CommitId;
use codex_infra_provider::AccountAuthentication;
use codex_infra_provider::AccountCatalog;
use codex_login::AuthCredentialsStoreMode;
use codex_login::ServerOptions;

const USAGE: &str = "usage: codex-infra-account login <account-home>\n       codex-infra-account import <catalog.json> <provider> <account> <credential-commit> <bearer|header:NAME|codex-login> < credential-on-stdin";

#[tokio::main(flavor = "current_thread")]
async fn main() -> io::Result<()> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() == 2 && args[0] == "login" {
        let home = std::path::absolute(Path::new(&args[1]))?;
        std::fs::create_dir_all(&home)?;
        let options = ServerOptions::new(
            home,
            codex_login::CLIENT_ID.to_owned(),
            /*forced_chatgpt_workspace_id*/ None,
            AuthCredentialsStoreMode::File,
            Default::default(),
            codex_login::AuthRouteConfig::from_http_client_factory(
                codex_http_client::HttpClientFactory::new(
                    codex_http_client::OutboundProxyPolicy::ReqwestDefault,
                ),
            ),
        );
        codex_login::run_device_code_login(options).await?;
        eprintln!(
            "Account auth.json saved. Commit this snapshot, then import it with its credential commit."
        );
        return Ok(());
    }
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
            auth: {
                let _: codex_login::AuthDotJson = serde_json::from_str(&input)?;
                serde_json::from_str(&input)?
            },
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
