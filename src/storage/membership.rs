use super::{
    Apply, Store,
    auth::{self, Principal},
    digest, header, save_metadata, signed,
};
use crate::{
    cluster::{Configuration, Member, Transition},
    error::{Result, RpcError},
    wire::etcdserverpb as pb,
};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use tonic::Code;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum MembershipCommand {
    Add {
        id: u64,
        peer_urls: Vec<String>,
        learner: bool,
    },
    Remove {
        id: u64,
    },
    Update {
        id: u64,
        peer_urls: Vec<String>,
    },
    Promote {
        id: u64,
        configuration_id: u32,
        applied_index: u64,
    },
    Publish {
        id: u64,
        name: String,
        client_url: String,
        incarnation: Option<Uuid>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ClusterReply {
    Add(pb::MemberAddResponse),
    Remove(pb::MemberRemoveResponse),
    Update(pb::MemberUpdateResponse),
    Promote(pb::MemberPromoteResponse),
    Publish,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MembershipReply {
    pub configuration_id: u32,
    pub transition: bool,
    pub response: ClusterReply,
}

impl MembershipReply {
    pub fn header_mut(&mut self) -> Option<&mut pb::ResponseHeader> {
        match &mut self.response {
            ClusterReply::Add(reply) => reply.header.as_mut(),
            ClusterReply::Remove(reply) => reply.header.as_mut(),
            ClusterReply::Update(reply) => reply.header.as_mut(),
            ClusterReply::Promote(reply) => reply.header.as_mut(),
            ClusterReply::Publish => None,
        }
    }
}

impl Store {
    pub fn incarnation(&self) -> Result<Option<Uuid>> {
        if self.schema_version()? == 1 {
            return Ok(None);
        }
        self.guard((|| {
            let row: Option<(Vec<u8>, Vec<u8>)> = self
                .connection
                .query_row(
                    "SELECT incarnation, checksum FROM node_incarnation WHERE id = 1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            row.map(|(bytes, checksum)| {
                if digest(&[
                    &self.identity.cluster_id.to_be_bytes(),
                    &self.identity.member_id.to_be_bytes(),
                    &bytes,
                ]) != checksum
                {
                    return Err(RpcError::corrupt(
                        "local member incarnation checksum mismatch",
                    ));
                }
                Uuid::from_slice(&bytes)
                    .map_err(|_| RpcError::corrupt("local member incarnation is malformed"))
            })
            .transpose()
        })())
    }

    pub fn identity_registered(&self) -> Result<bool> {
        let Some(incarnation) = self.incarnation()? else {
            return Ok(true);
        };
        let configuration = self
            .cluster_configuration()?
            .ok_or_else(|| RpcError::corrupt("claimed peer has no cluster configuration"))?;
        Ok(configuration.member(self.identity.member_id)?.incarnation == Some(incarnation))
    }

    pub fn configuration_history(&self) -> Result<Vec<(Configuration, u64, Option<Uuid>)>> {
        if self.schema_version()? == 1 {
            return Ok(self
                .cluster_configuration()?
                .into_iter()
                .map(|configuration| (configuration, 0, None))
                .collect());
        }
        self.guard(configurations(&self.connection))
    }

    pub fn configuration_at(&self, id: u32) -> Result<Configuration> {
        self.configuration_history()?
            .into_iter()
            .find(|(configuration, _, _)| configuration.configuration_id == id)
            .map(|(configuration, _, _)| configuration)
            .ok_or_else(|| {
                RpcError::new(
                    Code::PermissionDenied,
                    "unknown or stale cluster configuration",
                )
            })
    }

    pub fn transition(&self) -> Result<Option<Transition>> {
        if self.schema_version()? == 1 {
            return Ok(None);
        }
        self.guard(transition(&self.connection))
    }

    pub fn activate_configuration(&mut self, expected: &Transition) -> Result<()> {
        self.ready()?;
        let result = (|| {
            let staged = transition(&self.connection)?.ok_or_else(|| {
                RpcError::corrupt("decided StopSign has no committed membership proposal")
            })?;
            if staged.id != expected.id || staged.configuration != expected.configuration {
                return Err(RpcError::corrupt(
                    "StopSign differs from the committed membership transition",
                ));
            }
            let previous = self
                .cluster_configuration()?
                .ok_or_else(|| RpcError::corrupt("configuration transition has no old cluster"))?;
            let mut metadata = self.metadata.clone();
            metadata.applied_index = metadata
                .applied_index
                .checked_add(1)
                .ok_or_else(|| RpcError::corrupt("configuration activation position exhausted"))?;
            metadata.configuration_base = metadata.applied_index;
            metadata.configuration_id = expected.configuration.configuration_id;
            let transaction = self
                .connection
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            for member in previous.members.iter().filter(|member| {
                !expected
                    .configuration
                    .members
                    .iter()
                    .any(|next| next.id == member.id)
            }) {
                transaction.execute(
                    "INSERT OR IGNORE INTO removed_members(id) VALUES(?1)",
                    [member.id.to_be_bytes()],
                )?;
            }
            replace_configuration(&transaction, &expected.configuration)?;
            save_configuration(
                &transaction,
                &expected.configuration,
                metadata.configuration_base,
                Some(expected.id),
            )?;
            transaction.execute("DELETE FROM configuration_transition", [])?;
            save_metadata(&transaction, &metadata)?;
            transaction.commit()?;
            self.metadata = metadata;
            Ok(())
        })();
        self.guard(result)
    }
}

impl Apply<'_> {
    pub(super) fn execute_membership(
        &mut self,
        command: &MembershipCommand,
        request_id: Uuid,
    ) -> Result<MembershipReply> {
        let version: u32 = self
            .transaction
            .pragma_query_value(None, "user_version", |row| row.get(0))?;
        if version != 2 {
            return Err(RpcError::new(
                Code::FailedPrecondition,
                "dynamic administration requires explicit schema migration to version 2",
            ));
        }
        if matches!(command, MembershipCommand::Publish { .. }) {
            if self.principal != Principal::Internal {
                return Err(RpcError::new(
                    Code::PermissionDenied,
                    "member publication is internal",
                ));
            }
        } else {
            auth::require_admin(self.transaction, &self.metadata, &self.principal)?;
        }
        if transition(self.transaction)?.is_some() {
            return Err(RpcError::new(
                Code::Unavailable,
                "membership transition is already in progress",
            ));
        }
        let bytes: Option<Vec<u8>> = self
            .transaction
            .query_row(
                "SELECT configuration FROM cluster_configuration WHERE id = 1",
                [],
                |row| row.get(0),
            )
            .optional()?;
        let bytes = bytes.ok_or_else(|| {
            RpcError::new(
                Code::FailedPrecondition,
                "local mode requires explicit clustered bootstrap before member administration",
            )
        })?;
        let mut configuration: Configuration = serde_json::from_slice(&bytes)?;
        let mut activates = true;
        let response = match command {
            MembershipCommand::Add {
                id,
                peer_urls,
                learner,
            } => {
                if *id == 0 || configuration.members.iter().any(|member| member.id == *id) {
                    return Err(RpcError::new(
                        Code::AlreadyExists,
                        "etcdserver: member already exists",
                    ));
                }
                let removed: bool = self.transaction.query_row(
                    "SELECT EXISTS(SELECT 1 FROM removed_members WHERE id = ?1)",
                    [id.to_be_bytes()],
                    |row| row.get(0),
                )?;
                if removed {
                    return Err(RpcError::new(
                        Code::FailedPrecondition,
                        "etcdserver: member permanently removed",
                    ));
                }
                let first = peer_urls
                    .first()
                    .ok_or_else(|| RpcError::invalid("member requires at least one peer URL"))?;
                let pid = (1..=128)
                    .find(|pid| {
                        !configuration
                            .members
                            .iter()
                            .any(|member| member.pid == *pid)
                    })
                    .ok_or_else(|| {
                        RpcError::new(
                            Code::ResourceExhausted,
                            "active process identity space exhausted",
                        )
                    })?;
                let member = Member {
                    id: *id,
                    pid,
                    name: String::new(),
                    client_url: String::new(),
                    peer_url: first.clone(),
                    additional_peer_urls: peer_urls[1..].to_vec(),
                    incarnation: None,
                    learner: *learner,
                };
                configuration.members.push(member.clone());
                ClusterReply::Add(pb::MemberAddResponse {
                    header: header(self.metadata.revision),
                    member: Some(member.wire()),
                    members: Vec::new(),
                })
            }
            MembershipCommand::Remove { id } => {
                configuration.member(*id)?;
                configuration.members.retain(|member| member.id != *id);
                ClusterReply::Remove(pb::MemberRemoveResponse {
                    header: header(self.metadata.revision),
                    members: Vec::new(),
                })
            }
            MembershipCommand::Promote {
                id,
                configuration_id,
                applied_index,
            } => {
                if !configuration.member(*id)?.learner {
                    return Err(RpcError::new(
                        Code::FailedPrecondition,
                        "etcdserver: can only promote a learner member",
                    ));
                }
                if *configuration_id != configuration.configuration_id
                    || *applied_index < self.metadata.applied_index
                {
                    return Err(RpcError::new(
                        Code::FailedPrecondition,
                        "etcdserver: can only promote a learner member which is in sync with leader",
                    ));
                }
                configuration
                    .members
                    .iter_mut()
                    .find(|member| member.id == *id)
                    .ok_or_else(|| RpcError::corrupt("validated learner disappeared"))?
                    .learner = false;
                ClusterReply::Promote(pb::MemberPromoteResponse {
                    header: header(self.metadata.revision),
                    members: Vec::new(),
                })
            }
            MembershipCommand::Update { id, peer_urls } => {
                let first = peer_urls
                    .first()
                    .ok_or_else(|| RpcError::invalid("member requires peer URLs"))?;
                let member = configuration
                    .members
                    .iter_mut()
                    .find(|member| member.id == *id)
                    .ok_or_else(|| RpcError::new(Code::NotFound, "etcdserver: member not found"))?;
                member.peer_url = first.clone();
                member.additional_peer_urls = peer_urls[1..].to_vec();
                activates = false;
                ClusterReply::Update(pb::MemberUpdateResponse {
                    header: header(self.metadata.revision),
                    members: Vec::new(),
                })
            }
            MembershipCommand::Publish {
                id,
                name,
                client_url,
                incarnation,
            } => {
                if name.is_empty() || client_url.is_empty() {
                    return Err(RpcError::invalid(
                        "member publication requires name and client URL",
                    ));
                }
                let member = configuration
                    .members
                    .iter_mut()
                    .find(|member| member.id == *id)
                    .ok_or_else(|| RpcError::new(Code::NotFound, "etcdserver: member not found"))?;
                if member.incarnation.is_some() && member.incarnation != *incarnation {
                    return Err(RpcError::new(
                        Code::FailedPrecondition,
                        "member identity is already claimed by another incarnation; replace it with a new member ID",
                    ));
                }
                if configuration.configuration_id != 1
                    && member.name.is_empty()
                    && incarnation.is_none()
                {
                    return Err(RpcError::new(
                        Code::FailedPrecondition,
                        "a newly joined member must claim a durable incarnation",
                    ));
                }
                member.incarnation = *incarnation;
                member.name = name.clone();
                member.client_url = client_url.clone();
                activates = false;
                ClusterReply::Publish
            }
        };
        if activates {
            configuration.configuration_id = configuration
                .configuration_id
                .checked_add(1)
                .ok_or_else(|| {
                    RpcError::new(Code::OutOfRange, "configuration identity exhausted")
                })?;
        }
        configuration.validate()?;
        let members = configuration
            .members
            .iter()
            .map(Member::wire)
            .collect::<Vec<_>>();
        let mut reply = MembershipReply {
            configuration_id: configuration.configuration_id,
            transition: activates,
            response,
        };
        match &mut reply.response {
            ClusterReply::Add(response) => response.members = members,
            ClusterReply::Remove(response) => response.members = members,
            ClusterReply::Update(response) => response.members = members,
            ClusterReply::Promote(response) => response.members = members,
            ClusterReply::Publish => {}
        }
        if activates {
            let bytes = serde_json::to_vec(&configuration)?;
            self.transaction.execute("INSERT INTO configuration_transition(id, request_id, configuration, checksum) VALUES(1, ?1, ?2, ?3)",
                params![request_id.as_bytes(), bytes, digest(&[request_id.as_bytes(), &bytes])])?;
        } else {
            replace_configuration(self.transaction, &configuration)?;
            let activation = configurations(self.transaction)?
                .into_iter()
                .find(|(existing, _, _)| {
                    existing.configuration_id == configuration.configuration_id
                })
                .ok_or_else(|| RpcError::corrupt("active configuration has no durable history"))?
                .2;
            save_configuration(
                self.transaction,
                &configuration,
                self.metadata.configuration_base,
                activation,
            )?;
        }
        Ok(reply)
    }
}

fn transition(connection: &Connection) -> Result<Option<Transition>> {
    let row: Option<(Vec<u8>, Vec<u8>, Vec<u8>)> = connection
        .query_row(
            "SELECT request_id, configuration, checksum FROM configuration_transition WHERE id = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    row.map(|(id, bytes, checksum)| {
        if digest(&[&id, &bytes]) != checksum {
            return Err(RpcError::corrupt("membership transition checksum mismatch"));
        }
        let id = Uuid::from_slice(&id)
            .map_err(|_| RpcError::corrupt("membership request identity is malformed"))?;
        let configuration: Configuration = serde_json::from_slice(&bytes)?;
        configuration.validate()?;
        Ok(Transition { id, configuration })
    })
    .transpose()
}

pub(crate) fn save_configuration(
    connection: &Connection,
    configuration: &Configuration,
    base: u64,
    activation: Option<Uuid>,
) -> Result<()> {
    let bytes = serde_json::to_vec(configuration)?;
    let request = activation.map(|id| id.as_bytes().to_vec());
    let checksum = digest(&[
        &bytes,
        &base.to_be_bytes(),
        request.as_deref().unwrap_or(&[]),
    ]);
    connection.execute(
        "INSERT INTO configuration_history(configuration_id, base, activation_request, configuration, checksum) VALUES(?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(configuration_id) DO UPDATE SET base = excluded.base, activation_request = excluded.activation_request, configuration = excluded.configuration, checksum = excluded.checksum",
        params![configuration.configuration_id, signed(base)?, request, bytes, checksum],
    )?;
    Ok(())
}

fn configurations(connection: &Connection) -> Result<Vec<(Configuration, u64, Option<Uuid>)>> {
    let mut statement = connection.prepare("SELECT configuration_id, base, activation_request, configuration, checksum FROM configuration_history ORDER BY configuration_id")?;
    let mut rows = statement.query([])?;
    let mut result = Vec::new();
    while let Some(row) = rows.next()? {
        let id: u32 = row.get(0)?;
        let base: i64 = row.get(1)?;
        let activation: Option<Vec<u8>> = row.get(2)?;
        let bytes: Vec<u8> = row.get(3)?;
        let checksum: Vec<u8> = row.get(4)?;
        let base =
            u64::try_from(base).map_err(|_| RpcError::corrupt("negative configuration base"))?;
        if digest(&[
            &bytes,
            &base.to_be_bytes(),
            activation.as_deref().unwrap_or(&[]),
        ]) != checksum
        {
            return Err(RpcError::corrupt("configuration history checksum mismatch"));
        }
        let configuration: Configuration = serde_json::from_slice(&bytes)?;
        configuration.validate()?;
        if configuration.configuration_id != id {
            return Err(RpcError::corrupt("configuration history identity mismatch"));
        }
        result.push((
            configuration,
            base,
            activation
                .map(|bytes| {
                    Uuid::from_slice(&bytes)
                        .map_err(|_| RpcError::corrupt("invalid activation request"))
                })
                .transpose()?,
        ));
    }
    Ok(result)
}

fn replace_configuration(connection: &Connection, configuration: &Configuration) -> Result<()> {
    let bytes = serde_json::to_vec(configuration)?;
    connection.execute(
        "UPDATE cluster_configuration SET configuration = ?1, checksum = ?2 WHERE id = 1",
        params![bytes, digest(&[&bytes])],
    )?;
    connection.execute("DELETE FROM cluster_members", [])?;
    super::snapshot::insert_members(connection, configuration)
}
