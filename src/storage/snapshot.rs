use super::{
    Identity, Store, auth::Principal, identity_digest, load_metadata, private_permissions,
    require_private, save_metadata,
};
use crate::{
    cluster::Configuration,
    error::{Result, RpcError},
};
use rusqlite::{Connection, OpenFlags, backup::Backup, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tempfile::NamedTempFile;
use tonic::Code;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotInfo {
    pub format: u32,
    pub schema_version: u32,
    pub cluster_id: u64,
    pub member_id: u64,
    pub revision: i64,
    pub compact_revision: i64,
    pub applied_index: u64,
    pub configuration_id: u32,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RestoreInfo {
    pub cluster_id: u64,
    pub member_id: u64,
    pub revision: i64,
    pub compact_revision: i64,
    pub applied_index: u64,
    pub configuration_id: u32,
    pub schema_version: u32,
    pub source_sha256: String,
}

pub struct NativeSnapshot {
    pub(crate) file: NamedTempFile,
    pub info: SnapshotInfo,
    pub(crate) configuration: Option<Configuration>,
}

pub(crate) struct SnapshotSource {
    connection: Connection,
    directory: PathBuf,
    identity: Identity,
    revision: i64,
    applied_index: u64,
    configuration: Option<Configuration>,
    healthy: Arc<AtomicBool>,
}

impl NativeSnapshot {
    pub fn path(&self) -> &Path {
        self.file.path()
    }

    pub fn publish(&self, destination: &Path) -> Result<SnapshotInfo> {
        publish_file(self.file.path(), destination)?;
        Ok(self.info.clone())
    }
}

impl Store {
    pub fn native_snapshot(&self, principal: &Principal) -> Result<NativeSnapshot> {
        self.snapshot_source(principal)?.capture()
    }

    pub(crate) fn snapshot_source(&self, principal: &Principal) -> Result<SnapshotSource> {
        self.ready()?;
        self.require_admin(principal)?;
        let connection = Connection::open_with_flags(
            &self.path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        connection
            .execute_batch("PRAGMA trusted_schema=OFF; PRAGMA query_only=ON; BEGIN DEFERRED;")?;
        let metadata = self.guard(load_metadata(&connection))?;
        if metadata.revision != self.revision() || metadata.applied_index != self.applied_index() {
            return self.guard(Err(RpcError::corrupt(
                "snapshot reader differs from the owned writer frontier",
            )));
        }
        Ok(SnapshotSource {
            connection,
            directory: self
                .path
                .parent()
                .ok_or_else(|| RpcError::invalid("node database has no parent"))?
                .to_owned(),
            identity: self.identity.clone(),
            revision: metadata.revision,
            applied_index: metadata.applied_index,
            configuration: self.cluster_configuration()?,
            healthy: Arc::clone(&self.healthy),
        })
    }
}

impl SnapshotSource {
    pub(crate) fn capture(self) -> Result<NativeSnapshot> {
        let healthy = Arc::clone(&self.healthy);
        let result = (|| {
            verify_records(&self.connection)?;
            let file = NamedTempFile::new_in(&self.directory)?;
            {
                let mut destination = Connection::open(file.path())?;
                Backup::new(&self.connection, &mut destination)?.run_to_completion(
                    128,
                    Duration::from_millis(5),
                    None,
                )?;
                destination.execute_batch("PRAGMA journal_mode = DELETE;")?;
                destination
                    .close()
                    .map_err(|(_, error)| RpcError::from(error))?;
            }
            append_digest(file.path())?;
            let info = inspect(file.path())?;
            if info.cluster_id != self.identity.cluster_id
                || info.member_id != self.identity.member_id
                || info.revision != self.revision
                || info.applied_index != self.applied_index
            {
                return Err(RpcError::corrupt(
                    "native backup captured inconsistent application identity or frontier",
                ));
            }
            Ok(NativeSnapshot {
                file,
                info,
                configuration: self.configuration,
            })
        })();
        if result.as_ref().is_err_and(RpcError::is_storage_fault) {
            healthy.store(false, Ordering::Release);
        }
        result
    }
}

pub(crate) fn verified_payload(path: &Path) -> Result<(u64, String)> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(RpcError::invalid(
            "snapshot must be an ordinary file, not a symlink",
        ));
    }
    require_private(path)?;
    if metadata.len() < 544 {
        return Err(RpcError::invalid("native snapshot is truncated"));
    }
    let mut file = File::open(path)?;
    let payload_bytes = metadata.len() - 32;
    let mut hash = Sha256::new();
    let mut buffer = [0_u8; 64 << 10];
    let mut remaining = payload_bytes;
    while remaining != 0 {
        let count = usize::try_from(remaining.min(buffer.len() as u64))
            .map_err(|_| RpcError::invalid("snapshot read size exceeds platform bounds"))?;
        file.read_exact(&mut buffer[..count])?;
        hash.update(&buffer[..count]);
        remaining -= count as u64;
    }
    let mut trailer = [0_u8; 32];
    file.read_exact(&mut trailer)?;
    if hash.finalize().as_slice() != trailer {
        return Err(RpcError::invalid(
            "native snapshot SHA-256 checksum mismatch",
        ));
    }
    Ok((
        metadata.len(),
        trailer.iter().map(|byte| format!("{byte:02x}")).collect(),
    ))
}

pub(crate) fn open_artifact(path: &Path) -> Result<Connection> {
    let absolute = fs::canonicalize(path)?;
    let encoded = absolute
        .as_os_str()
        .as_encoded_bytes()
        .iter()
        .map(|byte| format!("%{byte:02X}"))
        .collect::<String>();
    let connection = Connection::open_with_flags(
        format!("file:{encoded}?immutable=1&mode=ro"),
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_URI,
    )?;
    connection.execute_batch("PRAGMA trusted_schema = OFF; PRAGMA query_only = ON;")?;
    Ok(connection)
}

pub fn inspect(path: &Path) -> Result<SnapshotInfo> {
    let (bytes, sha256) = verified_payload(path)?;
    let connection = open_artifact(path)
        .map_err(|error| RpcError::invalid(format!("native snapshot SQLite: {error}")))?;
    let schema_version: u32 =
        connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if schema_version != 1 && schema_version != 2 {
        return Err(RpcError::invalid("unsupported native snapshot schema"));
    }
    let check: String = connection.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    if check != "ok" {
        return Err(RpcError::invalid(format!(
            "native snapshot integrity: {check}"
        )));
    }
    verify_records(&connection).map_err(|error| {
        RpcError::invalid(format!("native snapshot application integrity: {error}"))
    })?;
    let state = load_metadata(&connection)
        .map_err(|error| RpcError::invalid(format!("native snapshot metadata: {error}")))?;
    let (cluster, member, name, mode, epoch, checksum): (Vec<u8>, Vec<u8>, String, String, i64, Vec<u8>) =
        connection.query_row("SELECT cluster_id, member_id, name, mode, runtime_epoch, checksum FROM node_state WHERE id = 1", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?))
        })?;
    let identity = Identity {
        cluster_id: super::decode_id(&cluster)?,
        member_id: super::decode_id(&member)?,
        name,
    };
    if identity_digest(&identity, &mode, epoch)? != checksum {
        return Err(RpcError::invalid(
            "snapshot node identity checksum mismatch",
        ));
    }
    Ok(SnapshotInfo {
        format: 1,
        schema_version,
        cluster_id: identity.cluster_id,
        member_id: identity.member_id,
        revision: state.revision,
        compact_revision: state.compacted_revision,
        applied_index: state.applied_index,
        configuration_id: state.configuration_id,
        bytes,
        sha256,
    })
}

pub fn restore(
    snapshot: &Path,
    destination: &Path,
    identity: Identity,
    configuration: Option<&Configuration>,
    revision_bump: i64,
    mark_compacted: bool,
) -> Result<RestoreInfo> {
    let info = inspect(snapshot)?;
    let mut restored_configuration = configuration.cloned();
    if let Some(configuration) = &mut restored_configuration {
        if configuration
            .bootstrap_digest
            .as_ref()
            .is_some_and(|digest| digest != &info.sha256)
            || configuration
                .members
                .iter()
                .any(|member| member.incarnation.is_some())
        {
            return Err(RpcError::invalid(
                "restore configuration must use the verified source digest and fresh member incarnations",
            ));
        }
        configuration.bootstrap_digest = Some(info.sha256.clone());
    }
    let configuration = restored_configuration.as_ref();
    if identity.cluster_id == 0
        || identity.member_id == 0
        || identity.name.is_empty()
        || identity.cluster_id == info.cluster_id
        || revision_bump < 0
    {
        return Err(RpcError::invalid(
            "disaster restore requires nonzero NEW cluster/member identities, a name, and a nonnegative revision bump",
        ));
    }
    if let Some(configuration) = configuration {
        configuration.validate()?;
        configuration.member(identity.member_id)?;
        if configuration.cluster_id != identity.cluster_id {
            return Err(RpcError::invalid(
                "restore configuration has a different new cluster identity",
            ));
        }
    }
    if fs::symlink_metadata(destination).is_ok() {
        return Err(RpcError::new(
            Code::AlreadyExists,
            "restore never replaces an existing directory",
        ));
    }
    let parent = destination
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let staging = tempfile::tempdir_in(parent)?;
    private_permissions(staging.path(), 0o700)?;
    let image = staging.path().join("node.db");
    copy_image(snapshot, &image, &info)?;
    let (replayed_revision, replayed_compaction) = {
        let mut recovered = Store::open(staging.path())?;
        if recovered.cluster_configuration()?.is_some() {
            let mut replica = crate::replica::Replica::open(&recovered)?;
            replica.apply(&mut recovered)?;
        }
        (recovered.revision(), recovered.compact_revision())
    };
    {
        let mut connection = Connection::open(&image)?;
        crate::sqlite::configure(&connection)?;
        let mut metadata = load_metadata(&connection)?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let mode = if configuration.is_some() {
            "cluster"
        } else {
            "local"
        };
        transaction.execute("UPDATE node_state SET cluster_id = ?1, member_id = ?2, name = ?3, mode = ?4, runtime_epoch = 0, checksum = ?5 WHERE id = 1",
            params![identity.cluster_id.to_be_bytes(), identity.member_id.to_be_bytes(), identity.name, mode, identity_digest(&identity, mode, 0)?])?;
        transaction.execute_batch("DELETE FROM auth_tokens; DELETE FROM requests; DELETE FROM cluster_members; DELETE FROM removed_members; DELETE FROM cluster_configuration; DELETE FROM alarms; DELETE FROM compaction_hashes;")?;
        let protocol_exists: bool = transaction.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'protocol_state')", [], |row| row.get(0))?;
        if protocol_exists {
            transaction
                .execute_batch("DELETE FROM protocol_entries; DELETE FROM protocol_state;")?;
        }
        if info.schema_version == 2 {
            transaction.execute_batch("DELETE FROM configuration_transition; DELETE FROM configuration_history; DELETE FROM node_incarnation;")?;
        }
        metadata.applied_index = 0;
        metadata.configuration_base = 0;
        metadata.configuration_id =
            configuration.map_or(1, |configuration| configuration.configuration_id);
        metadata.downgrade_target = None;
        metadata.revision = metadata
            .revision
            .checked_add(revision_bump)
            .ok_or_else(|| RpcError::invalid("restore revision bump exceeds signed MVCC range"))?;
        if mark_compacted {
            metadata.compacted_revision = metadata.revision;
            transaction.execute(
                "DELETE FROM mvcc_versions WHERE NOT EXISTS (
                 SELECT 1 FROM (SELECT key, MAX(main_revision) AS main FROM mvcc_versions GROUP BY key) latest
                 WHERE latest.key = mvcc_versions.key AND latest.main = main_revision AND kind = 0)", [],
            )?;
        }
        let logical_bytes: i64 = transaction.query_row(
            "SELECT COALESCE((SELECT SUM(length(kv)) FROM current_kvs), 0) + COALESCE((SELECT SUM(length(kv) + COALESCE(length(prev_kv), 0) + 128) FROM mvcc_versions), 0)",
            [], |row| row.get(0),
        )?;
        metadata.logical_bytes = u64::try_from(logical_bytes)
            .map_err(|_| RpcError::corrupt("restored logical size is negative"))?;
        if let Some(configuration) = configuration {
            let bytes = serde_json::to_vec(configuration)?;
            transaction.execute(
                "INSERT INTO cluster_configuration(id, configuration, checksum) VALUES(1, ?1, ?2)",
                params![bytes, super::digest(&[&bytes])],
            )?;
            insert_members(&transaction, configuration)?;
            if info.schema_version == 2 {
                super::membership::save_configuration(&transaction, configuration, 0, None)?;
            }
        }
        save_metadata(&transaction, &metadata)?;
        transaction.commit()?;
        connection
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode = DELETE;")?;
        connection
            .close()
            .map_err(|(_, error)| RpcError::from(error))?;
    }
    publish_node(&image, destination)?;
    Ok(RestoreInfo {
        cluster_id: identity.cluster_id,
        member_id: identity.member_id,
        revision: replayed_revision + revision_bump,
        applied_index: 0,
        configuration_id: configuration.map_or(1, |configuration| configuration.configuration_id),
        compact_revision: if mark_compacted {
            replayed_revision + revision_bump
        } else {
            replayed_compaction
        },
        schema_version: info.schema_version,
        source_sha256: info.sha256,
    })
}

pub fn join(snapshot: &Path, destination: &Path, identity: Identity) -> Result<RestoreInfo> {
    use crate::{
        cluster::{Command, CommandSnapshot},
        protocol::SqliteStorage,
    };
    use omnipaxos::storage::{Storage, StorageOp};
    let info = inspect(snapshot)?;
    if identity.cluster_id != info.cluster_id
        || identity.member_id == info.member_id
        || identity.name.is_empty()
    {
        return Err(RpcError::invalid(
            "join requires the existing cluster identity and a different newly added member",
        ));
    }
    if fs::symlink_metadata(destination).is_ok() {
        return Err(RpcError::new(
            Code::AlreadyExists,
            "join never replaces an existing directory",
        ));
    }
    let parent = destination
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let staging = tempfile::tempdir_in(parent)?;
    private_permissions(staging.path(), 0o700)?;
    let image = staging.path().join("node.db");
    copy_image(snapshot, &image, &info)?;
    let (configuration, decided, commands) = {
        let store = Store::open(staging.path())?;
        let configuration = store
            .cluster_configuration()?
            .ok_or_else(|| RpcError::invalid("join snapshot is not clustered"))?;
        let member = configuration.member(identity.member_id)?;
        if !member.name.is_empty() || !member.client_url.is_empty() {
            return Err(RpcError::new(
                Code::FailedPrecondition,
                "join cannot reset an established member's votes; add a new identity for replacement",
            ));
        }
        if store.transition()?.is_some() {
            return Err(RpcError::new(
                Code::Unavailable,
                "join snapshot has an in-flight configuration transition",
            ));
        }
        let protocol = SqliteStorage::<Command>::open(store.path(), configuration.configuration_id)
            .map_err(|error| RpcError::invalid(format!("join protocol storage: {error}")))?;
        let decided = protocol
            .get_decided_idx()
            .map_err(|error| RpcError::invalid(format!("join decided frontier: {error}")))?;
        if store.applied_index() != store.configuration_base() + decided as u64 {
            return Err(RpcError::invalid(
                "join snapshot does not bind its exact applied/decided frontier",
            ));
        }
        let compacted = protocol
            .get_compacted_idx()
            .map_err(|error| RpcError::invalid(format!("join compacted frontier: {error}")))?;
        let mut commands = protocol
            .get_snapshot()
            .map_err(|error| RpcError::invalid(format!("join protocol snapshot: {error}")))?
            .map_or_else(Vec::new, |snapshot| snapshot.commands);
        if commands.len() != compacted {
            return Err(RpcError::invalid(
                "join snapshot has an invalid command prefix",
            ));
        }
        commands.extend(
            protocol
                .get_entries(compacted, decided)
                .map_err(|error| RpcError::invalid(format!("join decided suffix: {error}")))?,
        );
        if commands.len() != decided {
            return Err(RpcError::invalid(
                "join snapshot is missing decided commands",
            ));
        }
        (configuration, decided, commands)
    };
    {
        let mut connection = Connection::open(&image)?;
        crate::sqlite::configure(&connection)?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        transaction.execute("UPDATE node_state SET member_id = ?1, name = ?2, runtime_epoch = 0, checksum = ?3 WHERE id = 1",
            params![identity.member_id.to_be_bytes(), identity.name, identity_digest(&identity, "cluster", 0)?])?;
        let incarnation = uuid::Uuid::new_v4();
        transaction.execute("DELETE FROM node_incarnation", [])?;
        transaction.execute(
            "INSERT INTO node_incarnation(id, incarnation, checksum) VALUES(1, ?1, ?2)",
            params![
                incarnation.as_bytes(),
                super::digest(&[
                    &identity.cluster_id.to_be_bytes(),
                    &identity.member_id.to_be_bytes(),
                    incarnation.as_bytes()
                ])
            ],
        )?;
        transaction.execute_batch("DELETE FROM protocol_entries; DELETE FROM protocol_state;")?;
        transaction.commit()?;
    }
    {
        let mut protocol =
            SqliteStorage::<Command>::open(&image, configuration.configuration_id)
                .map_err(|error| RpcError::invalid(format!("fresh join protocol: {error}")))?;
        protocol
            .write_atomically(vec![
                StorageOp::Trim(decided),
                StorageOp::SetCompactedIdx(decided),
                StorageOp::SetSnapshot(Some(CommandSnapshot { commands })),
                StorageOp::SetDecidedIndex(decided),
            ])
            .map_err(|error| RpcError::invalid(format!("seed decided join prefix: {error}")))?;
    }
    let connection = Connection::open(&image)?;
    connection.execute_batch("PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode = DELETE;")?;
    connection
        .close()
        .map_err(|(_, error)| RpcError::from(error))?;
    publish_node(&image, destination)?;
    Ok(RestoreInfo {
        cluster_id: identity.cluster_id,
        member_id: identity.member_id,
        revision: info.revision,
        compact_revision: info.compact_revision,
        applied_index: info.applied_index,
        configuration_id: configuration.configuration_id,
        schema_version: info.schema_version,
        source_sha256: info.sha256,
    })
}

pub(crate) fn insert_members(connection: &Connection, configuration: &Configuration) -> Result<()> {
    use prost::Message;
    for member in &configuration.members {
        let wire = member.wire();
        let bytes = wire.encode_to_vec();
        connection.execute(
            "INSERT INTO cluster_members(id, pid, member, checksum) VALUES(?1, ?2, ?3, ?4)",
            params![
                member.id.to_be_bytes(),
                super::signed(member.pid)?,
                bytes,
                super::digest(&[&member.id.to_be_bytes(), &bytes])
            ],
        )?;
    }
    Ok(())
}

pub(crate) fn verify_records(connection: &Connection) -> Result<()> {
    let mut foreign = connection.prepare("PRAGMA foreign_key_check")?;
    if foreign.query([])?.next()?.is_some() {
        return Err(RpcError::corrupt(
            "application foreign-key integrity failed",
        ));
    }
    for (table, key, value) in [
        ("auth_users", "CAST(name AS BLOB)", "user"),
        ("auth_roles", "CAST(name AS BLOB)", "role"),
        ("cluster_members", "id", "member"),
    ] {
        let mut statement =
            connection.prepare(&format!("SELECT {key}, {value}, checksum FROM {table}"))?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let key: Vec<u8> = row.get(0)?;
            let value: Vec<u8> = row.get(1)?;
            let checksum: Vec<u8> = row.get(2)?;
            if super::digest(&[&key, &value]) != checksum {
                return Err(RpcError::corrupt(format!(
                    "{table} record checksum mismatch"
                )));
            }
        }
    }
    let mut statement = connection.prepare("SELECT key, kv, checksum, create_revision, mod_revision, version, COALESCE(lease, 0) FROM current_kvs")?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let key: Vec<u8> = row.get(0)?;
        let bytes: Vec<u8> = row.get(1)?;
        let checksum: Vec<u8> = row.get(2)?;
        let kv = super::mvcc::decode_current(&key, &bytes, &checksum)?;
        if (kv.create_revision, kv.mod_revision, kv.version, kv.lease)
            != (row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?)
        {
            return Err(RpcError::corrupt(
                "current KV indexes differ from their protected protobuf values",
            ));
        }
    }
    let mismatch: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM current_kvs WHERE lease IS NOT NULL AND NOT EXISTS(
         SELECT 1 FROM lease_keys WHERE lease_keys.key = current_kvs.key AND lease_keys.lease = current_kvs.lease))
         OR EXISTS(SELECT 1 FROM lease_keys JOIN current_kvs ON current_kvs.key = lease_keys.key WHERE current_kvs.lease IS NULL OR current_kvs.lease != lease_keys.lease)",
        [], |row| row.get(0),
    )?;
    if mismatch {
        return Err(RpcError::corrupt(
            "lease ownership index differs from current key state",
        ));
    }
    let version: u32 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if version == 2 {
        let (cluster, member): (Vec<u8>, Vec<u8>) = connection.query_row(
            "SELECT cluster_id, member_id FROM node_state WHERE id = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let mut statement =
            connection.prepare("SELECT incarnation, checksum FROM node_incarnation")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let incarnation: Vec<u8> = row.get(0)?;
            let checksum: Vec<u8> = row.get(1)?;
            if super::digest(&[&cluster, &member, &incarnation]) != checksum {
                return Err(RpcError::corrupt("node incarnation checksum mismatch"));
            }
        }
    }
    let mut statement = connection.prepare(
        "SELECT main_revision, sub_revision, key, kind, kv, prev_kv, checksum FROM mvcc_versions",
    )?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let main: i64 = row.get(0)?;
        let sub: i64 = row.get(1)?;
        let key: Vec<u8> = row.get(2)?;
        let kind: i32 = row.get(3)?;
        let value: Vec<u8> = row.get(4)?;
        let previous: Option<Vec<u8>> = row.get(5)?;
        let checksum: Vec<u8> = row.get(6)?;
        if super::mvcc::history_digest(main, sub, &key, kind, &value, previous.as_deref())
            != checksum
        {
            return Err(RpcError::corrupt("MVCC history checksum mismatch"));
        }
    }
    let mut statement =
        connection.prepare("SELECT id, fingerprint, result, checksum FROM requests")?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let id: Vec<u8> = row.get(0)?;
        let fingerprint: Vec<u8> = row.get(1)?;
        let result: Vec<u8> = row.get(2)?;
        let checksum: Vec<u8> = row.get(3)?;
        if super::digest(&[&id, &fingerprint, &result]) != checksum {
            return Err(RpcError::corrupt("request outcome checksum mismatch"));
        }
    }
    let mut statement =
        connection.prepare("SELECT configuration, checksum FROM cluster_configuration")?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let configuration: Vec<u8> = row.get(0)?;
        let checksum: Vec<u8> = row.get(1)?;
        if super::digest(&[&configuration]) != checksum {
            return Err(RpcError::corrupt("cluster configuration checksum mismatch"));
        }
        let configuration: Configuration = serde_json::from_slice(&configuration)?;
        configuration.validate()?;
    }
    crate::protocol::verify_database(connection)
        .map_err(|error| RpcError::corrupt(format!("native protocol integrity: {error}")))?;
    Ok(())
}

pub(crate) fn publish_node(image: &Path, destination: &Path) -> Result<()> {
    File::open(image)?.sync_all()?;
    fs::create_dir(destination)?;
    private_permissions(destination, 0o700)?;
    let published = destination.join("node.db");
    if let Err(error) = fs::hard_link(image, &published) {
        if let Err(cleanup) = fs::remove_dir(destination) {
            tracing::error!(error = %cleanup, "cannot remove the operation's empty restore directory");
        }
        return Err(error.into());
    }
    private_permissions(&published, 0o600)?;
    File::open(destination)?.sync_all()?;
    File::open(
        destination
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new(".")),
    )?
    .sync_all()?;
    Ok(())
}

pub(crate) fn publish_file(staging: &Path, destination: &Path) -> Result<()> {
    File::open(staging)?.sync_all()?;
    fs::hard_link(staging, destination).map_err(|error| {
        RpcError::new(
            Code::AlreadyExists,
            format!("publish without replacing existing data: {error}"),
        )
    })?;
    File::open(
        destination
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new(".")),
    )?
    .sync_all()?;
    Ok(())
}

pub(crate) fn copy_image(snapshot: &Path, destination: &Path, info: &SnapshotInfo) -> Result<()> {
    let mut input = File::open(snapshot)?.take(info.bytes - 32);
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut output = options.open(destination)?;
    let mut hash = Sha256::new();
    let mut buffer = [0_u8; 64 << 10];
    let mut count = 0_u64;
    loop {
        let length = input.read(&mut buffer)?;
        if length == 0 {
            break;
        }
        hash.update(&buffer[..length]);
        output.write_all(&buffer[..length])?;
        count += length as u64;
    }
    let checksum: String = hash
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    if count != info.bytes - 32 || checksum != info.sha256 {
        return Err(RpcError::invalid(
            "snapshot changed or was truncated while copying",
        ));
    }
    output.sync_all()?;
    Ok(())
}

fn append_digest(path: &Path) -> Result<()> {
    let mut file = fs::OpenOptions::new().read(true).write(true).open(path)?;
    let mut hash = Sha256::new();
    let mut buffer = [0_u8; 64 << 10];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }
    file.seek(SeekFrom::End(0))?;
    file.write_all(&hash.finalize())?;
    file.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod capture_tests {
    use super::*;
    use crate::{storage::KvRequest, wire::etcdserverpb as pb};
    use uuid::Uuid;

    #[test]
    fn pinned_snapshot_remains_consistent_while_the_owned_writer_advances() {
        let directory = tempfile::tempdir().unwrap();
        let mut store = Store::create(
            directory.path().join("node"),
            Identity {
                cluster_id: 42,
                member_id: 1,
                name: "mock-snapshot".into(),
            },
        )
        .unwrap();
        store
            .apply(
                1,
                Uuid::from_u128(1),
                KvRequest::Put(pb::PutRequest {
                    key: b"mock/pinned".to_vec(),
                    value: vec![0, 255],
                    ..Default::default()
                }),
            )
            .unwrap();
        let source = store.snapshot_source(&Principal::Internal).unwrap();
        store
            .apply(
                2,
                Uuid::from_u128(2),
                KvRequest::Put(pb::PutRequest {
                    key: b"mock/later".to_vec(),
                    value: vec![128, 0],
                    ..Default::default()
                }),
            )
            .unwrap();
        let snapshot = source.capture().unwrap();
        assert_eq!(snapshot.info.revision, 2);
        assert_eq!(snapshot.info.applied_index, 1);
        assert_eq!(store.revision(), 3);
        let connection =
            Connection::open_with_flags(snapshot.path(), OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        let count: i64 = connection
            .query_row("SELECT COUNT(*) FROM current_kvs", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }
}
