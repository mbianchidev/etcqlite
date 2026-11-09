use super::{
    Store,
    auth::Principal,
    command::{Mutation, Reply},
    digest,
};
use crate::{
    cluster::{Command, CommandSnapshot},
    error::{Result, RpcError},
};
use rusqlite::params;
use serde::Serialize;
use std::path::Path;
use tonic::Code;

#[derive(Debug, Serialize)]
pub struct MigrationInfo {
    pub from: u32,
    pub to: u32,
    pub backup_sha256: String,
}

impl Store {
    pub fn migrate(&mut self, target: u32, backup: &Path) -> Result<MigrationInfo> {
        self.ready()?;
        let from = self.schema_version()?;
        if !(1..=2).contains(&target) || from == target {
            return Err(RpcError::new(
                Code::FailedPrecondition,
                "migration requires a different supported schema target (1 or 2)",
            ));
        }
        let configuration = self.cluster_configuration()?;
        if target == 1 {
            if self.configuration_base() != 0
                || self.transition()?.is_some()
                || self
                    .configuration_history()?
                    .iter()
                    .any(|(_, _, activation)| activation.is_some())
                || self.incarnation()?.is_some()
                || configuration.as_ref().is_some_and(|configuration| {
                    configuration.bootstrap_digest.is_some()
                        || configuration.members.iter().any(|member| {
                            member.incarnation.is_some() || !member.additional_peer_urls.is_empty()
                        })
                })
            {
                return Err(RpcError::new(
                    Code::FailedPrecondition,
                    "downgrade cannot reinterpret committed reconfiguration or multi-endpoint state; use isolated native restore and reverse migration",
                ));
            }
            let mut statement = self.connection.prepare("SELECT result FROM requests")?;
            let mut rows = statement.query([])?;
            while let Some(row) = rows.next()? {
                let bytes: Vec<u8> = row.get(0)?;
                if matches!(
                    serde_json::from_slice::<Result<Reply>>(&bytes)?,
                    Ok(Reply::Membership(_) | Reply::Maintenance(_))
                ) {
                    return Err(RpcError::new(
                        Code::FailedPrecondition,
                        "old binary cannot decode administrative request outcomes; use isolated native restore before downgrade",
                    ));
                }
            }
            let has_protocol: bool = self.connection.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'protocol_state')", [], |row| row.get(0))?;
            if has_protocol {
                let mut statement = self.connection.prepare("SELECT 0, payload FROM protocol_entries UNION ALL SELECT 1, snapshot FROM protocol_state WHERE snapshot IS NOT NULL")?;
                let mut rows = statement.query([])?;
                while let Some(row) = rows.next()? {
                    let kind: i32 = row.get(0)?;
                    let bytes: Vec<u8> = row.get(1)?;
                    let commands = match kind {
                        0 => vec![serde_json::from_slice::<Command>(&bytes)?],
                        1 => serde_json::from_slice::<CommandSnapshot>(&bytes)?.commands,
                        _ => return Err(RpcError::corrupt("unknown protocol record kind")),
                    };
                    if commands.iter().any(|command| {
                        matches!(
                            command,
                            Command::Mutation {
                                request: Mutation::Maintenance(_) | Mutation::Membership(_),
                                ..
                            }
                        )
                    }) {
                        return Err(RpcError::new(
                            Code::FailedPrecondition,
                            "old binary cannot replay administrative protocol commands",
                        ));
                    }
                }
            }
        }
        let saved = self
            .native_snapshot(&Principal::Internal)?
            .publish(backup)?;
        let result = (|| {
            let transaction = self
                .connection
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            if target == 2 {
                transaction.execute_batch(include_str!("migrations/002-administration.sql"))?;
                if let Some(configuration) = &configuration {
                    super::membership::save_configuration(
                        &transaction,
                        configuration,
                        self.metadata.configuration_base,
                        None,
                    )?;
                }
            } else {
                if let Some(configuration) = &configuration {
                    let mut legacy = serde_json::to_value(configuration)?;
                    let members = legacy
                        .get_mut("members")
                        .and_then(serde_json::Value::as_array_mut)
                        .ok_or_else(|| {
                            RpcError::corrupt("configuration serialization lacks members")
                        })?;
                    for member in members {
                        member
                            .as_object_mut()
                            .ok_or_else(|| {
                                RpcError::corrupt(
                                    "configuration serialization has a non-object member",
                                )
                            })?
                            .remove("additional_peer_urls");
                    }
                    let bytes = serde_json::to_vec(&legacy)?;
                    transaction.execute("UPDATE cluster_configuration SET configuration = ?1, checksum = ?2 WHERE id = 1", params![bytes, digest(&[&bytes])])?;
                }
                transaction
                    .execute_batch(include_str!("migrations/002-administration-down.sql"))?;
            }
            transaction.pragma_update(None, "user_version", target)?;
            transaction.commit()?;
            Ok(MigrationInfo {
                from,
                to: target,
                backup_sha256: saved.sha256,
            })
        })();
        self.guard(result)
    }
}
