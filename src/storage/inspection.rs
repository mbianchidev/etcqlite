use super::{Store, auth::Principal, mvcc, snapshot};
use crate::error::{Result, RpcError};
use rusqlite::{Connection, params};
use std::path::Path;

pub struct Projection {
    file: tempfile::NamedTempFile,
}

impl Projection {
    pub fn path(&self) -> &Path {
        self.file.path()
    }
    pub fn publish(&self, destination: &Path) -> Result<()> {
        snapshot::publish_file(self.path(), destination)
    }
}

impl Store {
    pub fn inspection_projection(
        &self,
        principal: &Principal,
        include_data: bool,
    ) -> Result<Projection> {
        self.require_admin(principal)?;
        self.guard(snapshot::verify_records(&self.connection))?;
        let file = tempfile::NamedTempFile::new_in(
            self.path
                .parent()
                .ok_or_else(|| RpcError::invalid("node path has no parent"))?,
        )?;
        let mut output = Connection::open(file.path())?;
        let transaction = output.transaction()?;
        transaction.execute_batch(
            "CREATE TABLE projection_metadata(kind TEXT NOT NULL, format INTEGER NOT NULL, cluster_id BLOB NOT NULL, member_id BLOB NOT NULL,
             revision INTEGER NOT NULL, compact_revision INTEGER NOT NULL, source_schema INTEGER NOT NULL, includes_data INTEGER NOT NULL) STRICT;
             CREATE TABLE key_metadata(ordinal INTEGER PRIMARY KEY, key_bytes INTEGER NOT NULL, value_bytes INTEGER NOT NULL,
             create_revision INTEGER NOT NULL, mod_revision INTEGER NOT NULL, version INTEGER NOT NULL, lease INTEGER NOT NULL) STRICT;
             CREATE TABLE lease_metadata(id INTEGER PRIMARY KEY, granted_ttl INTEGER NOT NULL, generation INTEGER NOT NULL, checkpoint_ttl INTEGER NOT NULL) STRICT;",
        )?;
        transaction.execute("INSERT INTO projection_metadata VALUES('read-only-inspection', 1, ?1, ?2, ?3, ?4, ?5, ?6)",
            params![self.identity.cluster_id.to_be_bytes(), self.identity.member_id.to_be_bytes(), self.revision(), self.compact_revision(), self.schema_version()?, include_data])?;
        if include_data {
            transaction.execute_batch("CREATE TABLE key_values(ordinal INTEGER PRIMARY KEY, key BLOB NOT NULL, value BLOB NOT NULL) STRICT;")?;
        }
        let mut statement = self
            .connection
            .prepare("SELECT key, kv, checksum FROM current_kvs ORDER BY key")?;
        let mut rows = statement.query([])?;
        let mut ordinal = 0_i64;
        while let Some(row) = rows.next()? {
            let key: Vec<u8> = row.get(0)?;
            let bytes: Vec<u8> = row.get(1)?;
            let checksum: Vec<u8> = row.get(2)?;
            let kv = mvcc::decode_current(&key, &bytes, &checksum)?;
            ordinal = ordinal
                .checked_add(1)
                .ok_or_else(|| RpcError::corrupt("projection row count exhausted"))?;
            transaction.execute(
                "INSERT INTO key_metadata VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    ordinal,
                    kv.key.len() as i64,
                    kv.value.len() as i64,
                    kv.create_revision,
                    kv.mod_revision,
                    kv.version,
                    kv.lease
                ],
            )?;
            if include_data {
                transaction.execute(
                    "INSERT INTO key_values VALUES(?1, ?2, ?3)",
                    params![ordinal, kv.key, kv.value],
                )?;
            }
        }
        for lease in self.leases()? {
            transaction.execute(
                "INSERT INTO lease_metadata VALUES(?1, ?2, ?3, ?4)",
                params![
                    lease.id,
                    lease.ttl,
                    super::signed(lease.generation)?,
                    lease.checkpoint_ttl
                ],
            )?;
        }
        transaction.commit()?;
        output.close().map_err(|(_, error)| RpcError::from(error))?;
        file.as_file().sync_all()?;
        super::private_permissions(file.path(), 0o400)?;
        Ok(Projection { file })
    }
}
