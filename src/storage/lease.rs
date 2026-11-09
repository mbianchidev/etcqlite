use super::{Apply, Store, auth, header, signed};
use crate::{
    error::{Result, RpcError},
    wire::etcdserverpb as pb,
};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use tonic::Code;

pub const MIN_LEASE_TTL: i64 = 2;
pub const MAX_LEASE_TTL: i64 = 9_223_372_036;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum LeaseCommand {
    Grant { id: i64, ttl: i64, now_ms: i64 },
    Renew { id: i64, now_ms: i64 },
    Revoke { id: i64 },
    Expire { id: i64, generation: u64 },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum LeaseReply {
    Grant(pb::LeaseGrantResponse),
    Renew(pb::LeaseKeepAliveResponse),
    Revoke(pb::LeaseRevokeResponse),
    Expired { expired: bool },
}

impl LeaseReply {
    pub fn header_mut(&mut self) -> Option<&mut pb::ResponseHeader> {
        match self {
            Self::Grant(reply) => reply.header.as_mut(),
            Self::Renew(reply) => reply.header.as_mut(),
            Self::Revoke(reply) => reply.header.as_mut(),
            Self::Expired { .. } => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    pub id: i64,
    pub ttl: i64,
    pub generation: u64,
    pub renewed_at_ms: i64,
    pub checkpoint_ttl: i64,
}

impl Apply<'_> {
    pub(super) fn execute_lease(&mut self, request: &LeaseCommand) -> Result<LeaseReply> {
        if !matches!(request, LeaseCommand::Expire { .. }) {
            auth::require_user(self.transaction, &self.metadata, &self.principal)?;
        }
        if !matches!(request, LeaseCommand::Renew { .. }) {
            super::maintenance::check_read_health(self.transaction)?;
        }
        if matches!(request, LeaseCommand::Grant { .. }) {
            super::maintenance::check_allocation(self.transaction)?;
        }
        match *request {
            LeaseCommand::Grant { id, ttl, now_ms } => {
                if id == 0 || now_ms < 0 {
                    return Err(RpcError::invalid(
                        "lease ID must be allocated and timestamp must be nonnegative",
                    ));
                }
                if ttl > MAX_LEASE_TTL {
                    return Err(RpcError::new(
                        Code::OutOfRange,
                        "etcdserver: too large lease TTL",
                    ));
                }
                if load(self.transaction, id)?.is_some() {
                    return Err(RpcError::new(
                        Code::FailedPrecondition,
                        "etcdserver: lease already exists",
                    ));
                }
                let ttl = ttl.max(MIN_LEASE_TTL);
                self.transaction.execute(
                    "INSERT INTO leases(id, ttl, generation, renewed_at_ms) VALUES(?1, ?2, 1, ?3)",
                    params![id, ttl, now_ms],
                )?;
                Ok(LeaseReply::Grant(pb::LeaseGrantResponse {
                    header: header(self.metadata.revision),
                    id,
                    ttl,
                    error: String::new(),
                }))
            }
            LeaseCommand::Renew { id, now_ms } => {
                auth::authorize_lease(self.transaction, &self.metadata, &self.principal, id, true)?;
                let Some(lease) = load(self.transaction, id)? else {
                    return Ok(LeaseReply::Renew(pb::LeaseKeepAliveResponse {
                        header: header(self.metadata.revision),
                        id,
                        ttl: 0,
                    }));
                };
                if now_ms < 0 {
                    return Err(RpcError::invalid(
                        "lease renewal timestamp must be nonnegative",
                    ));
                }
                let generation = lease
                    .generation
                    .checked_add(1)
                    .filter(|generation| *generation <= i64::MAX as u64)
                    .ok_or_else(|| {
                        RpcError::new(Code::OutOfRange, "etcdserver: lease generation exhausted")
                    })?;
                self.transaction.execute(
                    "UPDATE leases SET generation = ?2, renewed_at_ms = ?3, checkpoint_ttl = 0 WHERE id = ?1",
                    params![id, signed(generation)?, now_ms],
                )?;
                Ok(LeaseReply::Renew(pb::LeaseKeepAliveResponse {
                    header: header(self.metadata.revision),
                    id,
                    ttl: lease.ttl,
                }))
            }
            LeaseCommand::Revoke { id } => {
                auth::authorize_lease(self.transaction, &self.metadata, &self.principal, id, true)?;
                if load(self.transaction, id)?.is_none() {
                    return Err(not_found());
                }
                self.revoke(id)?;
                Ok(LeaseReply::Revoke(pb::LeaseRevokeResponse {
                    header: header(self.metadata.revision),
                }))
            }
            LeaseCommand::Expire { id, generation } => {
                if self.principal != auth::Principal::Internal {
                    return Err(RpcError::new(
                        Code::PermissionDenied,
                        "lease expiry is an internal authority operation",
                    ));
                }
                let matches =
                    load(self.transaction, id)?.is_some_and(|lease| lease.generation == generation);
                if matches {
                    self.revoke(id)?;
                }
                Ok(LeaseReply::Expired { expired: matches })
            }
        }
    }

    fn revoke(&mut self, id: i64) -> Result<()> {
        for key in keys(self.transaction, id)? {
            self.delete(&pb::DeleteRangeRequest {
                key,
                ..Default::default()
            })?;
        }
        self.transaction
            .execute("DELETE FROM leases WHERE id = ?1", [id])?;
        Ok(())
    }
}

impl Store {
    pub fn lease(&self, id: i64) -> Result<Option<Lease>> {
        self.ready()?;
        self.guard(load(&self.connection, id))
    }

    pub fn leases(&self) -> Result<Vec<Lease>> {
        self.ready()?;
        self.guard((|| {
            let mut statement = self
                .connection
                .prepare("SELECT id FROM leases ORDER BY id")?;
            let ids = statement
                .query_map([], |row| row.get::<_, i64>(0))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            ids.into_iter()
                .map(|id| {
                    load(&self.connection, id)?
                        .ok_or_else(|| RpcError::corrupt("enumerated lease disappeared"))
                })
                .collect()
        })())
    }

    pub fn lease_keys(&self, id: i64) -> Result<Vec<Vec<u8>>> {
        self.ready()?;
        self.guard(keys(&self.connection, id))
    }

    pub fn authorize_lease(&self, principal: &auth::Principal, id: i64, write: bool) -> Result<()> {
        self.ready()?;
        self.guard(auth::authorize_lease(
            &self.connection,
            &self.metadata,
            principal,
            id,
            write,
        ))
    }

    pub fn lease_list(&self, principal: &auth::Principal) -> Result<pb::LeaseLeasesResponse> {
        auth::require_user(&self.connection, &self.metadata, principal)?;
        let leases = self.leases()?;
        for lease in &leases {
            auth::authorize_lease(&self.connection, &self.metadata, principal, lease.id, false)?;
        }
        Ok(pb::LeaseLeasesResponse {
            header: header(self.metadata.revision),
            leases: leases
                .into_iter()
                .map(|lease| pb::LeaseStatus { id: lease.id })
                .collect(),
        })
    }
}

pub(crate) fn load(connection: &Connection, id: i64) -> Result<Option<Lease>> {
    Ok(connection
        .query_row(
            "SELECT ttl, generation, renewed_at_ms, checkpoint_ttl FROM leases WHERE id = ?1",
            [id],
            |row| {
                Ok(Lease {
                    id,
                    ttl: row.get(0)?,
                    generation: super::unsigned_column(row, 1)?,
                    renewed_at_ms: row.get(2)?,
                    checkpoint_ttl: row.get(3)?,
                })
            },
        )
        .optional()?)
}

pub(crate) fn keys(connection: &Connection, id: i64) -> Result<Vec<Vec<u8>>> {
    let mut statement =
        connection.prepare("SELECT key FROM lease_keys WHERE lease = ?1 ORDER BY key")?;
    Ok(statement
        .query_map([id], |row| row.get(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?)
}

fn not_found() -> RpcError {
    RpcError::new(Code::NotFound, "etcdserver: requested lease not found")
}
