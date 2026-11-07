use super::{Apply, KvRequest, Store, auth, decode_id, header};
use crate::{
    error::{Result, RpcError},
    wire::etcdserverpb as pb,
};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use tonic::Code;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum MaintenanceCommand {
    Alarm(pb::AlarmRequest),
    Downgrade(pb::DowngradeRequest),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum MaintenanceReply {
    Alarm(pb::AlarmResponse),
    Downgrade(pb::DowngradeResponse),
}

impl MaintenanceReply {
    pub fn header_mut(&mut self) -> Option<&mut pb::ResponseHeader> {
        match self {
            Self::Alarm(response) => response.header.as_mut(),
            Self::Downgrade(response) => response.header.as_mut(),
        }
    }
}

impl Store {
    pub fn hash_kv(
        &self,
        principal: &auth::Principal,
        revision: i64,
    ) -> Result<pb::HashKvResponse> {
        self.ready()?;
        self.guard((|| {
            self.require_admin(principal)?;
            if revision < 0 {
                return Err(RpcError::invalid("hash revision must be nonnegative"));
            }
            if revision > self.revision() {
                return Err(RpcError::new(
                    Code::OutOfRange,
                    "etcdserver: mvcc: required revision is a future revision",
                ));
            }
            let cached = if revision != 0 {
                self.connection.query_row(
                    "SELECT hash, compacted_revision FROM compaction_hashes WHERE revision = ?1",
                    [revision], |row| Ok((row.get::<_, u32>(0)?, row.get::<_, i64>(1)?)),
                ).optional()?
            } else {
                None
            };
            let target = if revision == 0 {
                self.revision()
            } else {
                revision
            };
            let (hash, compact_revision) = if let Some(cached) = cached {
                cached
            } else {
                if revision != 0 && revision < self.compact_revision() {
                    return Err(RpcError::new(
                        Code::OutOfRange,
                        "etcdserver: mvcc: required revision has been compacted",
                    ));
                }
                (
                    hash_kv_at(&self.connection, self.compact_revision(), target)?,
                    self.compact_revision(),
                )
            };
            Ok(pb::HashKvResponse {
                header: header(self.revision()),
                hash,
                compact_revision,
                hash_revision: target,
            })
        })())
    }

    pub fn alarms(
        &self,
        principal: &auth::Principal,
        request: &pb::AlarmRequest,
    ) -> Result<pb::AlarmResponse> {
        self.ready()?;
        self.guard((|| {
            self.require_admin(principal)?;
            validate_alarm(request)?;
            Ok(pb::AlarmResponse {
                header: header(self.revision()),
                alarms: alarms(&self.connection, request)?,
            })
        })())
    }

    pub fn protocol_version(&self) -> &str {
        self.metadata.downgrade_target.as_deref().unwrap_or("3.7.0")
    }

    pub fn downgrade_validate(
        &self,
        principal: &auth::Principal,
        version: &str,
    ) -> Result<pb::DowngradeResponse> {
        self.require_admin(principal)?;
        validate_downgrade(version)?;
        if self.metadata.downgrade_target.is_some() {
            return Err(RpcError::new(
                Code::FailedPrecondition,
                "etcdserver: cluster has a downgrade job in progress",
            ));
        }
        Ok(pb::DowngradeResponse {
            header: header(self.revision()),
            version: self.protocol_version().into(),
        })
    }
}

impl Apply<'_> {
    pub(super) fn execute_maintenance(
        &mut self,
        request: &MaintenanceCommand,
    ) -> Result<MaintenanceReply> {
        auth::require_admin(self.transaction, &self.metadata, &self.principal)?;
        match request {
            MaintenanceCommand::Alarm(request) => {
                validate_alarm(request)?;
                match request.action {
                    0 => {}
                    1 => {
                        if request.alarm == 0 {
                            return Err(RpcError::invalid("cannot activate the NONE alarm"));
                        }
                        let ids = if request.member_id == 0 {
                            let mut statement = self.transaction.prepare("SELECT member_id FROM node_state UNION SELECT id FROM cluster_members")?;
                            statement
                                .query_map([], |row| row.get::<_, Vec<u8>>(0))?
                                .collect::<rusqlite::Result<Vec<_>>>()?
                        } else {
                            vec![request.member_id.to_be_bytes().to_vec()]
                        };
                        for id in ids {
                            self.transaction.execute(
                                "INSERT OR IGNORE INTO alarms(member_id, alarm) VALUES(?1, ?2)",
                                params![id, request.alarm],
                            )?;
                        }
                    }
                    2 => {
                        if request.alarm == 1
                            && self.metadata.logical_bytes >= self.metadata.quota_bytes
                        {
                            return Err(RpcError::new(
                                Code::ResourceExhausted,
                                "etcdserver: mvcc: database space exceeded",
                            ));
                        }
                        self.transaction.execute(
                            "DELETE FROM alarms WHERE (?1 OR member_id = ?2) AND (?3 = 0 OR alarm = ?3)",
                            params![request.member_id == 0, request.member_id.to_be_bytes(), request.alarm],
                        )?;
                    }
                    _ => return Err(RpcError::invalid("unknown alarm action")),
                }
                Ok(MaintenanceReply::Alarm(pb::AlarmResponse {
                    header: header(self.metadata.revision),
                    alarms: alarms(self.transaction, request)?,
                }))
            }
            MaintenanceCommand::Downgrade(request) => {
                let current = self
                    .metadata
                    .downgrade_target
                    .clone()
                    .unwrap_or_else(|| "3.7.0".into());
                match request.action {
                    0 => validate_downgrade(&request.version)?,
                    1 => {
                        validate_downgrade(&request.version)?;
                        if self.metadata.downgrade_target.is_some() {
                            return Err(RpcError::new(
                                Code::FailedPrecondition,
                                "etcdserver: cluster has a downgrade job in progress",
                            ));
                        }
                        self.metadata.downgrade_target = Some(request.version.clone());
                    }
                    2 => {
                        if self.metadata.downgrade_target.take().is_none() {
                            return Err(RpcError::new(
                                Code::FailedPrecondition,
                                "etcdserver: no inflight downgrade job",
                            ));
                        }
                    }
                    _ => return Err(RpcError::invalid("unknown downgrade action")),
                }
                Ok(MaintenanceReply::Downgrade(pb::DowngradeResponse {
                    header: header(self.metadata.revision),
                    version: current,
                }))
            }
        }
    }
}

fn validate_downgrade(version: &str) -> Result<()> {
    if version != "3.6.0" {
        return Err(RpcError::new(
            Code::FailedPrecondition,
            "etcdserver: invalid downgrade target version; only the 3.6.0 protocol profile is supported",
        ));
    }
    Ok(())
}

fn validate_alarm(request: &pb::AlarmRequest) -> Result<()> {
    if !(0..=2).contains(&request.action) || !(0..=2).contains(&request.alarm) {
        return Err(RpcError::invalid("unknown alarm action or type"));
    }
    Ok(())
}

fn alarms(connection: &Connection, request: &pb::AlarmRequest) -> Result<Vec<pb::AlarmMember>> {
    let mut statement = connection.prepare(
        "SELECT member_id, alarm FROM alarms WHERE (?1 OR member_id = ?2) AND (?3 = 0 OR alarm = ?3) ORDER BY member_id, alarm",
    )?;
    let rows = statement
        .query_map(
            params![
                request.member_id == 0,
                request.member_id.to_be_bytes(),
                request.alarm
            ],
            |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i32>(1)?)),
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    rows.into_iter()
        .map(|(id, alarm)| {
            Ok(pb::AlarmMember {
                member_id: decode_id(&id)?,
                alarm,
            })
        })
        .collect()
}

pub(super) fn check_alarms(connection: &Connection, request: &KvRequest) -> Result<()> {
    check_read_health(connection)?;
    if allocates(request) {
        check_allocation(connection)?;
    }
    Ok(())
}

pub(super) fn check_allocation(connection: &Connection) -> Result<()> {
    let no_space: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM alarms WHERE alarm = 1)",
        [],
        |row| row.get(0),
    )?;
    if no_space {
        return Err(RpcError::new(
            Code::ResourceExhausted,
            "etcdserver: mvcc: database space exceeded",
        ));
    }
    Ok(())
}

pub(super) fn check_read_health(connection: &Connection) -> Result<()> {
    let corrupt: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM alarms WHERE alarm = 2)",
        [],
        |row| row.get(0),
    )?;
    if corrupt {
        return Err(RpcError::alarm_corrupt());
    }
    Ok(())
}

fn allocates(request: &KvRequest) -> bool {
    match request {
        KvRequest::Put(_) => true,
        KvRequest::Txn(request) => {
            request
                .success
                .iter()
                .chain(&request.failure)
                .any(|operation| match &operation.request {
                    Some(pb::request_op::Request::RequestPut(_)) => true,
                    Some(pb::request_op::Request::RequestTxn(request)) => {
                        allocates(&KvRequest::Txn(request.clone()))
                    }
                    _ => false,
                })
        }
        _ => false,
    }
}

pub(super) fn revision_key(main: i64, sub: i64, tombstone: bool) -> Vec<u8> {
    let mut key = Vec::with_capacity(18);
    key.extend_from_slice(&main.to_be_bytes());
    key.push(b'_');
    key.extend_from_slice(&sub.to_be_bytes());
    if tombstone {
        key.push(b't');
    }
    key
}

pub(super) fn hash_kv_at(
    connection: &Connection,
    compact_revision: i64,
    revision: i64,
) -> Result<u32> {
    hash_with_keep(connection, compact_revision, revision, false)
}

pub(super) fn compaction_hash(
    connection: &Connection,
    compact_revision: i64,
    revision: i64,
) -> Result<u32> {
    hash_with_keep(connection, compact_revision, revision, true)
}

fn hash_with_keep(
    connection: &Connection,
    compact_revision: i64,
    revision: i64,
    compacting: bool,
) -> Result<u32> {
    let keep = if compact_revision >= 0 {
        let mut statement = connection.prepare(
        "SELECT main_revision, sub_revision FROM (
         SELECT main_revision, sub_revision, kind,
         ROW_NUMBER() OVER(PARTITION BY key ORDER BY main_revision DESC, sub_revision DESC) AS latest
         FROM mvcc_versions WHERE main_revision <= ?1)
         WHERE latest = 1 AND (kind = 0 OR (?2 AND main_revision = ?1))",
    )?;
        let keep = statement
            .query_map(params![revision, compacting], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
            })?
            .collect::<rusqlite::Result<BTreeSet<_>>>()?;
        drop(statement);
        keep
    } else {
        BTreeSet::new()
    };
    let mut crc = Crc32c::new();
    crc.update(b"key");
    let mut statement = connection.prepare(
        "SELECT main_revision, sub_revision, key, kind, kv, prev_kv, checksum FROM mvcc_versions WHERE main_revision <= ?1 ORDER BY main_revision, sub_revision",
    )?;
    let mut rows = statement.query([revision])?;
    while let Some(row) = rows.next()? {
        let main: i64 = row.get(0)?;
        let sub: i64 = row.get(1)?;
        let key: Vec<u8> = row.get(2)?;
        let kind: i32 = row.get(3)?;
        let kv: Vec<u8> = row.get(4)?;
        let previous: Option<Vec<u8>> = row.get(5)?;
        let checksum: Vec<u8> = row.get(6)?;
        if super::mvcc::history_digest(main, sub, &key, kind, &kv, previous.as_deref()) != checksum
        {
            return Err(RpcError::corrupt("MVCC hash input checksum mismatch"));
        }
        if main == compact_revision && kind == 1 {
            continue;
        }
        if main <= compact_revision && !keep.is_empty() && !keep.contains(&(main, sub)) {
            continue;
        }
        crc.update(&revision_key(main, sub, kind == 1));
        crc.update(&kv);
    }
    Ok(crc.finish())
}

pub(super) struct Crc32c(u32);

impl Crc32c {
    pub fn new() -> Self {
        Self(0)
    }
    pub fn update(&mut self, bytes: &[u8]) {
        self.0 = crc32c::crc32c_append(self.0, bytes);
    }
    pub fn finish(self) -> u32 {
        self.0
    }
}

#[cfg(test)]
mod crc_tests {
    use super::Crc32c;

    #[test]
    fn castagnoli_known_vector_and_incremental_boundaries_match() {
        let mut whole = Crc32c::new();
        whole.update(b"123456789");
        assert_eq!(whole.finish(), 0xe306_9283);
        let mut incremental = Crc32c::new();
        incremental.update(b"1234");
        incremental.update(b"");
        incremental.update(b"56789");
        assert_eq!(incremental.finish(), 0xe306_9283);
        assert_eq!(Crc32c::new().finish(), 0);
    }
}
