use std::net::SocketAddr;

use codex_infra_protocol::CommitId;
use codex_infra_protocol::MachineId;
use codex_infra_protocol::MessageId;
use serde::Deserialize;
use serde::Serialize;

use crate::PublishedAccount;

/// Published by the machine that currently owns refresh for this account.
/// The endpoint is dynamically allocated; owner_revision identifies the
/// committed ownership assignment used to route a request.
/// instance_id changes on every listener start, even if its port is reused.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountAuthority {
    pub instance_id: MessageId,
    pub machine_id: MachineId,
    pub owner_revision: CommitId,
    pub endpoint: SocketAddr,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct AccountRequest {
    pub request_id: MessageId,
    pub authority: AccountAuthority,
    pub provider_id: String,
    pub account_id: String,
    pub action: AccountAction,
}

/// Control-plane requests only; no Agent task messages travel on this channel.
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AccountAction {
    Current,
    Refresh {
        previous_revision: CommitId,
        previous_chatgpt_account_id: Option<String>,
    },
}

#[derive(Clone, Serialize, Deserialize)]
pub struct AccountResponse {
    pub request_id: MessageId,
    pub authority: AccountAuthority,
    pub result: AccountResult,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AccountResult {
    Published { account: Box<PublishedAccount> },
    Failed { message: String },
}
