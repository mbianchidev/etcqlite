mod mvcc;

use crate::{
    error::{Result, RpcError},
    wire::{etcdserverpb as pb, mvccpb},
};
use rusqlite::{Connection, OpenFlags, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tonic::Code;
use uuid::Uuid;

pub const MAX_REQUEST_BYTES: usize = 1_572_864;
pub const MAX_TXN_OPS: usize = 128;
pub const DEFAULT_QUOTA_BYTES: u64 = 2_147_483_648;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Identity {
    pub cluster_id: u64,
    pub member_id: u64,
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum KvRequest {
    Range(pb::RangeRequest),
    Put(pb::PutRequest),
    Delete(pb::DeleteRangeRequest),
    Txn(pb::TxnRequest),
    Compact(pb::CompactionRequest),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum KvReply {
    Range(pb::RangeResponse),
    Put(pb::PutResponse),
    Delete(pb::DeleteRangeResponse),
    Txn(pb::TxnResponse),
    Compact(pb::CompactionResponse),
}

impl KvReply {
    pub fn header_mut(&mut self) -> Option<&mut pb::ResponseHeader> {
        match self {
            Self::Range(reply) => reply.header.as_mut(),
            Self::Put(reply) => reply.header.as_mut(),
            Self::Delete(reply) => reply.header.as_mut(),
            Self::Txn(reply) => reply.header.as_mut(),
            Self::Compact(reply) => reply.header.as_mut(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Metadata {
    pub revision: i64,
    pub compacted_revision: i64,
    pub auth_revision: u64,
    pub auth_enabled: bool,
    pub configuration_id: u32,
    pub configuration_base: u64,
    pub applied_index: u64,
    pub logical_bytes: u64,
    pub quota_bytes: u64,
    pub downgrade_target: Option<String>,
}

impl Default for Metadata {
    fn default() -> Self {
        Self {
            revision: 1,
            compacted_revision: -1,
            auth_revision: 1,
            auth_enabled: false,
            configuration_id: 1,
            configuration_base: 0,
            applied_index: 0,
            logical_bytes: 0,
            quota_bytes: DEFAULT_QUOTA_BYTES,
            downgrade_target: None,
        }
    }
}

pub struct Store {
    pub(crate) connection: Connection,
    pub(crate) identity: Identity,
    pub(crate) metadata: Metadata,
    path: PathBuf,
    healthy: Arc<AtomicBool>,
    _lock: File,
}

pub(crate) struct Apply<'a> {
    pub transaction: &'a Transaction<'a>,
    pub metadata: Metadata,
    pub sub_revision: i64,
    pub modified: bool,
}

impl Store {
    pub fn create(directory: impl AsRef<Path>, identity: Identity) -> Result<Self> {
        if identity.cluster_id == 0 || identity.member_id == 0 || identity.name.is_empty() {
            return Err(RpcError::invalid(
                "cluster/member IDs and node name must be nonzero/nonempty",
            ));
        }
        let directory = directory.as_ref();
        match fs::symlink_metadata(directory) {
            Ok(metadata) => {
                if !metadata.is_dir() || metadata.file_type().is_symlink() {
                    return Err(RpcError::invalid(
                        "node directory is not an ordinary directory",
                    ));
                }
                if let Some(entry) = fs::read_dir(directory)?.next() {
                    entry?;
                    return Err(RpcError::new(
                        Code::AlreadyExists,
                        "node directory is not empty; initialization never replaces data",
                    ));
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(directory)?
            }
            Err(error) => return Err(error.into()),
        }
        private_permissions(directory, 0o700)?;
        let lock = node_lock(directory)?;
        let path = directory.join("node.db");
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        options.open(&path)?.sync_all()?;
        let mut connection = Connection::open_with_flags(
            &path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        crate::sqlite::configure(&connection)?;
        let metadata = Metadata::default();
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        transaction.execute_batch(include_str!("schema.sql"))?;
        transaction.execute(
            "INSERT INTO node_state(id, cluster_id, member_id, name, mode, checksum) VALUES(1, ?1, ?2, ?3, 'local', ?4)",
            params![identity.cluster_id.to_be_bytes(), identity.member_id.to_be_bytes(), identity.name, identity_digest(&identity, "local", 0)?],
        )?;
        insert_metadata(&transaction, &metadata)?;
        transaction.pragma_update(None, "user_version", 1)?;
        transaction.commit()?;
        File::open(directory)?.sync_all()?;
        let parent = directory
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        File::open(parent)?.sync_all()?;
        Ok(Self {
            connection,
            identity,
            metadata,
            path,
            healthy: Arc::new(AtomicBool::new(true)),
            _lock: lock,
        })
    }

    pub fn open(directory: impl AsRef<Path>) -> Result<Self> {
        let directory = directory.as_ref();
        let metadata = fs::symlink_metadata(directory)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(RpcError::invalid(
                "node directory is not an ordinary directory",
            ));
        }
        require_private(directory)?;
        let path = directory.join("node.db");
        let file = fs::symlink_metadata(&path)?;
        if !file.is_file() || file.file_type().is_symlink() || file.len() == 0 {
            return Err(RpcError::corrupt(
                "node database is missing, empty, or a symlink; explicit recovery is required",
            ));
        }
        require_private(&path)?;
        let lock = node_lock(directory)?;
        let connection = Connection::open_with_flags(
            &path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        crate::sqlite::configure(&connection)?;
        let version: u32 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
        if version != 1 {
            return Err(RpcError::corrupt(
                "unsupported SQLite schema; startup never resets or migrates it implicitly",
            ));
        }
        let integrity: String = connection.query_row("PRAGMA quick_check", [], |row| row.get(0))?;
        if integrity != "ok" {
            return Err(RpcError::corrupt(format!(
                "node SQLite integrity: {integrity}"
            )));
        }
        let (cluster, member, name, mode, epoch, checksum): (Vec<u8>, Vec<u8>, String, String, i64, Vec<u8>) =
            connection.query_row(
                "SELECT cluster_id, member_id, name, mode, runtime_epoch, checksum FROM node_state WHERE id = 1",
                [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?)),
            )?;
        let identity = Identity {
            cluster_id: decode_id(&cluster)?,
            member_id: decode_id(&member)?,
            name,
        };
        if identity_digest(&identity, &mode, epoch)? != checksum {
            return Err(RpcError::corrupt("node identity checksum mismatch"));
        }
        let metadata = load_metadata(&connection)?;
        Ok(Self {
            connection,
            identity,
            metadata,
            path,
            healthy: Arc::new(AtomicBool::new(true)),
            _lock: lock,
        })
    }

    pub fn identity(&self) -> &Identity {
        &self.identity
    }
    pub fn revision(&self) -> i64 {
        self.metadata.revision
    }
    pub fn compact_revision(&self) -> i64 {
        self.metadata.compacted_revision
    }
    pub fn applied_index(&self) -> u64 {
        self.metadata.applied_index
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn health_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.healthy)
    }
    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }

    fn ready(&self) -> Result<()> {
        if self.is_healthy() {
            Ok(())
        } else {
            Err(RpcError::new(
                Code::Unavailable,
                "node application storage is poisoned; recover it before serving",
            ))
        }
    }

    fn guard<T>(&self, result: Result<T>) -> Result<T> {
        if result.as_ref().is_err_and(RpcError::is_storage_fault) {
            self.healthy.store(false, Ordering::Release);
        }
        result
    }

    pub fn apply(
        &mut self,
        position: u64,
        request_id: Uuid,
        request: KvRequest,
    ) -> Result<KvReply> {
        self.ready()?;
        let result = self.apply_inner(position, request_id, request);
        self.guard(result)
    }

    fn apply_inner(
        &mut self,
        position: u64,
        request_id: Uuid,
        request: KvRequest,
    ) -> Result<KvReply> {
        let mut metadata = load_metadata(&self.connection)?;
        if position
            != metadata
                .applied_index
                .checked_add(1)
                .ok_or_else(|| RpcError::corrupt("applied position exhausted"))?
        {
            return Err(RpcError::corrupt(
                "application entries must be applied consecutively",
            ));
        }
        signed(position)?;
        let fingerprint = Sha256::digest(serde_json::to_vec(&request)?).to_vec();
        let transaction = self
            .connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let cached: Option<(Vec<u8>, Vec<u8>, Vec<u8>)> = transaction
            .query_row(
                "SELECT fingerprint, result, checksum FROM requests WHERE id = ?1",
                [request_id.as_bytes().as_slice()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let result = if let Some((stored_fingerprint, bytes, checksum)) = cached {
            if stored_fingerprint != fingerprint
                || digest(&[request_id.as_bytes(), &stored_fingerprint, &bytes]) != checksum
            {
                return Err(RpcError::corrupt(
                    "internal request identity conflict or result checksum mismatch",
                ));
            }
            serde_json::from_slice::<Result<KvReply>>(&bytes)?
        } else {
            transaction.execute_batch("SAVEPOINT operation")?;
            let mut context = Apply {
                transaction: &transaction,
                metadata: metadata.clone(),
                sub_revision: 0,
                modified: false,
            };
            let result = context.execute(&request);
            match &result {
                Ok(_) => {
                    transaction.execute_batch("RELEASE operation")?;
                    metadata = context.metadata;
                }
                Err(error) if !error.is_storage_fault() => {
                    transaction.execute_batch("ROLLBACK TO operation; RELEASE operation")?;
                }
                Err(error) => return Err(error.clone()),
            }
            let bytes = serde_json::to_vec(&result)?;
            transaction.execute(
                "INSERT INTO requests(id, fingerprint, result, checksum) VALUES(?1, ?2, ?3, ?4)",
                params![
                    request_id.as_bytes().as_slice(),
                    fingerprint,
                    bytes,
                    digest(&[request_id.as_bytes(), &fingerprint, &bytes])
                ],
            )?;
            result
        };
        metadata.applied_index = position;
        save_metadata(&transaction, &metadata)?;
        transaction.commit()?;
        self.metadata = metadata;
        result
    }

    pub fn range(&self, request: &pb::RangeRequest) -> Result<pb::RangeResponse> {
        self.ready()?;
        let result = (|| {
            mvcc::validate_range(request)?;
            let transaction = self.connection.unchecked_transaction()?;
            let metadata = load_metadata(&transaction)?;
            let response = mvcc::range(&transaction, &metadata, request)?;
            transaction.commit()?;
            Ok(response)
        })();
        self.guard(result)
    }

    pub fn events(
        &self,
        start: i64,
        key: &[u8],
        end: &[u8],
    ) -> Result<Vec<(i64, Vec<mvccpb::Event>)>> {
        self.ready()?;
        let result = (|| {
            let transaction = self.connection.unchecked_transaction()?;
            let metadata = load_metadata(&transaction)?;
            if start <= metadata.compacted_revision {
                return Err(RpcError::new(
                    Code::OutOfRange,
                    "etcdserver: mvcc: required revision has been compacted",
                ));
            }
            let events = mvcc::events(&transaction, start, key, end)?;
            transaction.commit()?;
            Ok(events)
        })();
        self.guard(result)
    }
}

pub(crate) fn header(revision: i64) -> Option<pb::ResponseHeader> {
    Some(pb::ResponseHeader {
        revision,
        ..Default::default()
    })
}

pub(crate) fn digest(parts: &[&[u8]]) -> Vec<u8> {
    let mut hash = Sha256::new();
    for part in parts {
        hash.update((part.len() as u64).to_be_bytes());
        hash.update(part);
    }
    hash.finalize().to_vec()
}

fn identity_digest(identity: &Identity, mode: &str, epoch: i64) -> Result<Vec<u8>> {
    Ok(digest(&[
        &serde_json::to_vec(identity)?,
        mode.as_bytes(),
        &epoch.to_be_bytes(),
    ]))
}

pub(crate) fn decode_id(bytes: &[u8]) -> Result<u64> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| RpcError::corrupt("unsigned identifier must have exactly eight bytes"))?;
    Ok(u64::from_be_bytes(bytes))
}

pub(crate) fn signed(value: u64) -> Result<i64> {
    i64::try_from(value)
        .map_err(|_| RpcError::corrupt("application position or size exceeds signed SQLite range"))
}

fn insert_metadata(transaction: &Transaction<'_>, metadata: &Metadata) -> Result<()> {
    transaction.execute(
        "INSERT INTO app_state(id, schema_version, codec_version, revision, compacted_revision, auth_revision,
         auth_enabled, configuration_id, configuration_base, applied_index, logical_bytes, quota_bytes, downgrade_target, checksum)
         VALUES(1, 1, 1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        params![metadata.revision, metadata.compacted_revision, metadata.auth_revision.to_be_bytes(),
            metadata.auth_enabled, metadata.configuration_id, signed(metadata.configuration_base)?,
            signed(metadata.applied_index)?, signed(metadata.logical_bytes)?, signed(metadata.quota_bytes)?,
            metadata.downgrade_target, Sha256::digest(serde_json::to_vec(metadata)?).to_vec()],
    )?;
    Ok(())
}

pub(crate) fn save_metadata(transaction: &Transaction<'_>, metadata: &Metadata) -> Result<()> {
    transaction.execute(
        "UPDATE app_state SET revision = ?1, compacted_revision = ?2, auth_revision = ?3, auth_enabled = ?4,
         configuration_id = ?5, configuration_base = ?6, applied_index = ?7, logical_bytes = ?8,
         quota_bytes = ?9, downgrade_target = ?10, checksum = ?11 WHERE id = 1",
        params![metadata.revision, metadata.compacted_revision, metadata.auth_revision.to_be_bytes(),
            metadata.auth_enabled, metadata.configuration_id, signed(metadata.configuration_base)?,
            signed(metadata.applied_index)?, signed(metadata.logical_bytes)?, signed(metadata.quota_bytes)?,
            metadata.downgrade_target, Sha256::digest(serde_json::to_vec(metadata)?).to_vec()],
    )?;
    Ok(())
}

pub(crate) fn load_metadata(connection: &Connection) -> Result<Metadata> {
    let (metadata, auth, checksum) = connection.query_row(
        "SELECT revision, compacted_revision, auth_revision, auth_enabled, configuration_id, configuration_base,
         applied_index, logical_bytes, quota_bytes, downgrade_target, checksum FROM app_state
         WHERE id = 1 AND schema_version = 1 AND codec_version = 1",
        [], |row| Ok((
            Metadata {
                revision: row.get(0)?, compacted_revision: row.get(1)?, auth_revision: 0,
                auth_enabled: row.get(3)?, configuration_id: row.get(4)?, configuration_base: unsigned_column(row, 5)?,
                applied_index: unsigned_column(row, 6)?, logical_bytes: unsigned_column(row, 7)?, quota_bytes: unsigned_column(row, 8)?,
                downgrade_target: row.get(9)?,
            },
            row.get::<_, Vec<u8>>(2)?,
            row.get::<_, Vec<u8>>(10)?,
        )),
    )?;
    let metadata = Metadata {
        auth_revision: decode_id(&auth)?,
        ..metadata
    };
    if metadata.revision < 1
        || metadata.compacted_revision > metadata.revision
        || metadata.auth_revision == 0
        || Sha256::digest(serde_json::to_vec(&metadata)?).as_slice() != checksum
    {
        return Err(RpcError::corrupt(
            "application metadata checksum or boundaries are invalid",
        ));
    }
    Ok(metadata)
}

fn node_lock(directory: &Path) -> Result<File> {
    let path = directory.join("node.lock");
    if fs::symlink_metadata(&path).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
        return Err(RpcError::invalid("node lock path is a symlink"));
    }
    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(path)?;
    file.try_lock().map_err(|error| {
        RpcError::new(
            Code::Unavailable,
            format!("node directory is already owned or cannot be locked: {error}"),
        )
    })?;
    Ok(file)
}

fn unsigned_column(row: &rusqlite::Row<'_>, column: usize) -> rusqlite::Result<u64> {
    let value: i64 = row.get(column)?;
    u64::try_from(value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            column,
            rusqlite::types::Type::Integer,
            Box::new(error),
        )
    })
}

fn private_permissions(path: &Path, mode: u32) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(mode))?;
    }
    #[cfg(not(unix))]
    let _ = (path, mode);
    Ok(())
}

fn require_private(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if fs::metadata(path)?.permissions().mode() & 0o077 != 0 {
            return Err(RpcError::new(
                Code::PermissionDenied,
                "node data must not be accessible to group or other users",
            ));
        }
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}
