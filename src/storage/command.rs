use super::{
    Apply, KvReply, KvRequest,
    auth::{AuthCommand, AuthReply, Principal},
    digest,
    lease::{LeaseCommand, LeaseReply},
    maintenance::{MaintenanceCommand, MaintenanceReply},
    membership::{MembershipCommand, MembershipReply},
};
use crate::{error::Result, wire::etcdserverpb as pb};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Mutation {
    Kv(KvRequest),
    Auth(AuthCommand),
    Lease(LeaseCommand),
    Maintenance(MaintenanceCommand),
    Membership(MembershipCommand),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Reply {
    Kv(KvReply),
    Auth(AuthReply),
    Lease(LeaseReply),
    Maintenance(MaintenanceReply),
    Membership(MembershipReply),
}

impl Reply {
    pub fn header_mut(&mut self) -> Option<&mut pb::ResponseHeader> {
        match self {
            Self::Kv(reply) => reply.header_mut(),
            Self::Auth(reply) => reply.header_mut(),
            Self::Lease(reply) => reply.header_mut(),
            Self::Maintenance(reply) => reply.header_mut(),
            Self::Membership(reply) => reply.header_mut(),
        }
    }
}

impl Mutation {
    pub(crate) fn fingerprint(&self, principal: &Principal) -> Result<Vec<u8>> {
        let mut normalized = self.clone();
        if let Self::Lease(
            LeaseCommand::Grant { now_ms, .. } | LeaseCommand::Renew { now_ms, .. },
        ) = &mut normalized
        {
            *now_ms = 0;
        }
        let bytes = serde_json::to_vec(&normalized)?;
        if principal == &Principal::Anonymous {
            return Ok(Sha256::digest(bytes).to_vec());
        }
        Ok(digest(&[&bytes, &serde_json::to_vec(principal)?]))
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::Kv(KvRequest::Range(_)) => "range",
            Self::Kv(KvRequest::Put(_)) => "put",
            Self::Kv(KvRequest::Delete(_)) => "delete",
            Self::Kv(KvRequest::Txn(_)) => "txn",
            Self::Kv(KvRequest::Compact(_)) => "compact",
            Self::Auth(_) => "auth",
            Self::Lease(_) => "lease",
            Self::Maintenance(_) => "maintenance",
            Self::Membership(_) => "membership",
        }
    }
}

impl Apply<'_> {
    pub(super) fn execute_command(&mut self, request: &Mutation) -> Result<Reply> {
        match request {
            Mutation::Kv(request) => {
                super::mvcc::validate(request)?;
                super::auth::authorize_kv(
                    self.transaction,
                    &self.metadata,
                    &self.principal,
                    request,
                )?;
                super::maintenance::check_alarms(self.transaction, request)?;
                Ok(Reply::Kv(self.execute(request)?))
            }
            Mutation::Auth(request) => Ok(Reply::Auth(self.execute_auth(request)?)),
            Mutation::Lease(request) => Ok(Reply::Lease(self.execute_lease(request)?)),
            Mutation::Maintenance(request) => {
                Ok(Reply::Maintenance(self.execute_maintenance(request)?))
            }
            Mutation::Membership(request) => Ok(Reply::Membership(
                self.execute_membership(request, self.request_id)?,
            )),
        }
    }
}
