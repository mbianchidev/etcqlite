use super::{
    Store,
    auth::Principal,
    decode_id, digest,
    maintenance::{Crc32c, revision_key},
    mvcc::history_digest,
    save_metadata,
    snapshot::publish_file,
};
use crate::{
    error::{Result, RpcError},
    wire::{authpb, etcdserverpb as pb, leasepb, mvccpb},
};
use prost::Message;
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs::File,
    io::{Read, Write},
    path::Path,
};
use tonic::Code;

pub const MAX_MANIFEST_BYTES: u64 = 512 << 20;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub format: u32,
    pub reference: String,
    pub revision: i64,
    pub compact_revision: i64,
    pub buckets: Vec<Bucket>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bucket {
    #[serde(with = "crate::bytes")]
    pub name: Vec<u8>,
    pub entries: Vec<BucketEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BucketEntry {
    #[serde(with = "crate::bytes")]
    pub key: Vec<u8>,
    #[serde(with = "crate::bytes")]
    pub value: Vec<u8>,
}

impl Manifest {
    pub fn read(path: &Path) -> Result<Self> {
        let metadata = std::fs::symlink_metadata(path)?;
        if metadata.file_type().is_symlink() {
            return Err(RpcError::invalid("manifest must not be a symlink"));
        }
        super::require_private(path)?;
        let file = File::open(path)?;
        if !file.metadata()?.is_file() || file.metadata()?.len() > MAX_MANIFEST_BYTES {
            return Err(RpcError::invalid(
                "manifest is not an ordinary bounded file",
            ));
        }
        let manifest: Self = serde_json::from_reader(file.take(MAX_MANIFEST_BYTES + 1))
            .map_err(|error| RpcError::invalid(format!("snapshot manifest: {error}")))?;
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn write(&self, path: &Path) -> Result<()> {
        self.validate()?;
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let mut staging = tempfile::NamedTempFile::new_in(parent)?;
        serde_json::to_writer(&mut staging, self)?;
        staging.write_all(b"\n")?;
        staging.as_file().sync_all()?;
        if staging.as_file().metadata()?.len() > MAX_MANIFEST_BYTES {
            return Err(RpcError::new(
                Code::ResourceExhausted,
                "manifest exceeds the 512-MiB bounded converter limit",
            ));
        }
        publish_file(staging.path(), path)
    }

    pub fn validate(&self) -> Result<()> {
        if self.format != 1
            || self.reference != "3.7.3"
            || self.revision < 1
            || self.compact_revision < -1
            || self.compact_revision > self.revision
        {
            return Err(RpcError::invalid(
                "unsupported manifest profile or revision boundaries",
            ));
        }
        let allowed: &[&[u8]] = &[
            b"alarm",
            b"auth",
            b"authRoles",
            b"authUsers",
            b"cluster",
            b"key",
            b"lease",
            b"members",
            b"members_removed",
            b"meta",
        ];
        let mut previous: Option<&[u8]> = None;
        let mut size = 0_u64;
        for bucket in &self.buckets {
            if !allowed.contains(&bucket.name.as_slice())
                || previous.is_some_and(|previous| previous >= bucket.name.as_slice())
            {
                return Err(RpcError::invalid(
                    "manifest buckets must be known, unique and bytewise sorted",
                ));
            }
            previous = Some(&bucket.name);
            let mut last: Option<&[u8]> = None;
            for entry in &bucket.entries {
                if entry.key.is_empty() || last.is_some_and(|last| last >= entry.key.as_slice()) {
                    return Err(RpcError::invalid(
                        "manifest entries must be nonempty, unique and bytewise sorted",
                    ));
                }
                last = Some(&entry.key);
                size = size
                    .checked_add((entry.key.len() + entry.value.len()) as u64)
                    .ok_or_else(|| RpcError::invalid("manifest size overflow"))?;
                if size > MAX_MANIFEST_BYTES / 4 * 3 {
                    return Err(RpcError::new(
                        Code::ResourceExhausted,
                        "manifest exceeds the bounded interchange limit",
                    ));
                }
            }
        }
        self.bucket(b"key")?;
        self.bucket(b"meta")?;
        Ok(())
    }

    fn bucket(&self, name: &[u8]) -> Result<&[BucketEntry]> {
        self.buckets
            .iter()
            .find(|bucket| bucket.name == name)
            .map(|bucket| bucket.entries.as_slice())
            .ok_or_else(|| RpcError::invalid("manifest lacks a required application bucket"))
    }

    fn entries(&self, name: &[u8]) -> &[BucketEntry] {
        self.buckets
            .iter()
            .find(|bucket| bucket.name == name)
            .map_or(&[], |bucket| bucket.entries.as_slice())
    }

    pub fn hash(&self) -> u32 {
        let mut hash = Crc32c::new();
        for bucket in &self.buckets {
            hash.update(&bucket.name);
            for entry in &bucket.entries {
                if bucket.name == b"meta"
                    && (entry.key == b"consistent_index" || entry.key == b"term")
                {
                    continue;
                }
                hash.update(&entry.key);
                hash.update(&entry.value);
            }
        }
        hash.finish()
    }
}

fn bucket(name: &[u8], mut entries: Vec<BucketEntry>) -> Bucket {
    entries.sort_by(|left, right| left.key.cmp(&right.key));
    Bucket {
        name: name.to_vec(),
        entries,
    }
}

fn entry(key: &[u8], value: &[u8]) -> BucketEntry {
    BucketEntry {
        key: key.to_vec(),
        value: value.to_vec(),
    }
}

#[derive(Serialize)]
struct EtcdMember<'a> {
    id: String,
    #[serde(rename = "peerURLs")]
    peer_urls: &'a [String],
    #[serde(rename = "isLearner", skip_serializing_if = "std::ops::Not::not")]
    learner: bool,
    name: &'a str,
    #[serde(rename = "clientURLs")]
    client_urls: &'a [String],
}

impl Store {
    pub fn application_manifest(&self, principal: &Principal, term: u64) -> Result<Manifest> {
        self.manifest_with_history(principal, term, true)
    }

    fn manifest_with_history(
        &self,
        principal: &Principal,
        term: u64,
        include_history: bool,
    ) -> Result<Manifest> {
        self.require_admin(principal)?;
        self.guard((|| {
            let mut history = Vec::new();
            let mut statement = self.connection.prepare("SELECT main_revision, sub_revision, key, kind, kv, prev_kv, checksum FROM mvcc_versions WHERE ?1 ORDER BY main_revision, sub_revision")?;
            let mut rows = statement.query([include_history])?;
            let mut bytes = 0_u64;
            while let Some(row) = rows.next()? {
                let main: i64 = row.get(0)?; let sub: i64 = row.get(1)?;
                let key: Vec<u8> = row.get(2)?; let kind: i32 = row.get(3)?;
                let value: Vec<u8> = row.get(4)?; let previous: Option<Vec<u8>> = row.get(5)?; let checksum: Vec<u8> = row.get(6)?;
                if history_digest(main, sub, &key, kind, &value, previous.as_deref()) != checksum { return Err(RpcError::corrupt("snapshot history checksum mismatch")); }
                bytes = bytes.checked_add(value.len() as u64).ok_or_else(|| RpcError::corrupt("snapshot size accounting overflow"))?;
                if bytes > MAX_MANIFEST_BYTES / 4 * 3 { return Err(RpcError::new(Code::ResourceExhausted, "snapshot exceeds the bounded converter limit")); }
                history.push(entry(&revision_key(main, sub, kind == 1), &value));
            }
            let leases = self.leases()?.into_iter().map(|lease| entry(&lease.id.to_be_bytes(), &leasepb::Lease {
                id: lease.id, ttl: lease.ttl, remaining_ttl: lease.checkpoint_ttl,
            }.encode_to_vec())).collect();
            let mut users = Vec::new();
            let mut statement = self.connection.prepare("SELECT name, user, checksum FROM auth_users ORDER BY name")?;
            let mut rows = statement.query([])?;
            while let Some(row) = rows.next()? {
                let name: String = row.get(0)?; let value: Vec<u8> = row.get(1)?; let checksum: Vec<u8> = row.get(2)?;
                if digest(&[name.as_bytes(), &value]) != checksum { return Err(RpcError::corrupt("snapshot user checksum mismatch")); }
                users.push(entry(name.as_bytes(), &value));
            }
            let mut roles = Vec::new();
            let mut statement = self.connection.prepare("SELECT name, role, checksum FROM auth_roles ORDER BY name")?;
            let mut rows = statement.query([])?;
            while let Some(row) = rows.next()? {
                let name: String = row.get(0)?; let value: Vec<u8> = row.get(1)?; let checksum: Vec<u8> = row.get(2)?;
                if digest(&[name.as_bytes(), &value]) != checksum { return Err(RpcError::corrupt("snapshot role checksum mismatch")); }
                roles.push(entry(name.as_bytes(), &value));
            }
            let members = self.members(principal)?.members.into_iter().map(|member| {
                Ok(entry(format!("{:x}", member.id).as_bytes(), &serde_json::to_vec(&EtcdMember {
                    id: format!("{:x}", member.id), peer_urls: &member.peer_ur_ls, learner: member.is_learner,
                    name: &member.name, client_urls: &member.client_ur_ls,
                })?))
            }).collect::<Result<Vec<_>>>()?;
            let mut statement = self.connection.prepare("SELECT id FROM removed_members ORDER BY id")?;
            let removed = statement.query_map([], |row| row.get::<_, Vec<u8>>(0))?.collect::<rusqlite::Result<Vec<_>>>()?
                .into_iter().map(|id| Ok(entry(format!("{:x}", decode_id(&id)?).as_bytes(), b"removed"))).collect::<Result<Vec<_>>>()?;
            let alarms = self.alarms(principal, &pb::AlarmRequest::default())?.alarms.into_iter().map(|alarm| entry(&alarm.encode_to_vec(), &[])).collect();
            let mut meta = vec![entry(b"consistent_index", &self.applied_index().to_be_bytes()), entry(b"storageVersion", self.protocol_version().as_bytes()),
                entry(b"term", &term.max(1).to_be_bytes())];
            if self.compact_revision() >= 0 {
                let revision = revision_key(self.compact_revision(), 0, false);
                meta.push(entry(b"finishedCompactRev", &revision)); meta.push(entry(b"scheduledCompactRev", &revision));
            }
            let manifest = Manifest { format: 1, reference: "3.7.3".into(), revision: self.revision(), compact_revision: self.compact_revision(), buckets: vec![
                bucket(b"alarm", alarms), bucket(b"auth", vec![entry(b"authEnabled", &[u8::from(self.metadata.auth_enabled)]), entry(b"authRevision", &self.auth_revision().to_be_bytes())]),
                bucket(b"authRoles", roles), bucket(b"authUsers", users), bucket(b"cluster", vec![entry(b"clusterVersion", self.protocol_version().as_bytes())]),
                bucket(b"key", history), bucket(b"lease", leases), bucket(b"members", members), bucket(b"members_removed", removed), bucket(b"meta", meta),
            ]};
            manifest.validate()?;
            Ok(manifest)
        })())
    }

    pub fn backend_hash(&self, principal: &Principal, term: u64) -> Result<u32> {
        self.guard((|| {
        let manifest = self.manifest_with_history(principal, term, false)?;
        let mut hash = Crc32c::new();
        for bucket in manifest.buckets {
            hash.update(&bucket.name);
            if bucket.name == b"key" {
                let mut statement = self.connection.prepare("SELECT main_revision,sub_revision,key,kind,kv,prev_kv,checksum FROM mvcc_versions ORDER BY main_revision,sub_revision")?;
                let mut rows = statement.query([])?;
                while let Some(row) = rows.next()? {
                    let main: i64 = row.get(0)?;
                    let sub: i64 = row.get(1)?;
                    let key: Vec<u8> = row.get(2)?;
                    let kind: i32 = row.get(3)?;
                    let value: Vec<u8> = row.get(4)?;
                    let previous: Option<Vec<u8>> = row.get(5)?;
                    let checksum: Vec<u8> = row.get(6)?;
                    if history_digest(main, sub, &key, kind, &value, previous.as_deref())
                        != checksum
                    {
                        return Err(RpcError::corrupt("backend hash history checksum mismatch"));
                    }
                    hash.update(&revision_key(main, sub, kind == 1));
                    hash.update(&value);
                }
            } else {
                for entry in bucket.entries {
                    if bucket.name == b"meta"
                        && (entry.key == b"consistent_index" || entry.key == b"term")
                    {
                        continue;
                    }
                    hash.update(&entry.key);
                    hash.update(&entry.value);
                }
            }
        }
        Ok(hash.finish())
        })())
    }

    pub fn import_manifest(
        &mut self,
        manifest: &Manifest,
        revision_bump: i64,
        mark_compacted: bool,
    ) -> Result<()> {
        self.ready()?;
        manifest.validate()?;
        if !manifest.entries(b"alarm").is_empty() {
            return Err(RpcError::new(
                Code::FailedPrecondition,
                "application migration requires an alarm-free source; repair and verify the source before fenced cutover",
            ));
        }
        if self.applied_index() != 0
            || self.revision() != 1
            || self.cluster_configuration()?.is_some()
            || revision_bump < 0
        {
            return Err(RpcError::new(
                Code::FailedPrecondition,
                "application import requires a new, empty, isolated local node and a nonnegative revision bump",
            ));
        }
        let revision = manifest
            .revision
            .checked_add(revision_bump)
            .ok_or_else(|| RpcError::invalid("import revision bump exceeds signed MVCC range"))?;
        let mut metadata = self.metadata.clone();
        metadata.revision = revision;
        metadata.compacted_revision = if mark_compacted {
            revision
        } else {
            manifest.compact_revision
        };
        let auth_entries = manifest.entries(b"auth");
        for item in auth_entries {
            match item.key.as_slice() {
                b"authEnabled" if item.value.as_slice() == [0] || item.value.as_slice() == [1] => {
                    metadata.auth_enabled = item.value[0] == 1
                }
                b"authRevision" => {
                    metadata.auth_revision = decode_id(&item.value)
                        .map_err(|_| RpcError::invalid("invalid imported auth revision"))?
                }
                _ => return Err(RpcError::invalid("unsupported auth metadata record")),
            }
        }
        if metadata.auth_revision == 0 {
            return Err(RpcError::invalid("auth revision must be nonzero"));
        }
        for item in manifest.entries(b"meta") {
            match item.key.as_slice() {
                b"storageVersion" if item.value == b"3.7.0" || item.value == b"3.6.0" => {}
                b"consistent_index" | b"term" if item.value.len() == 8 => {}
                b"finishedCompactRev" | b"scheduledCompactRev" => {
                    let (main, _, tombstone) = decode_revision(&item.value)?;
                    if tombstone || main != manifest.compact_revision {
                        return Err(RpcError::invalid(
                            "compaction metadata differs from the manifest boundary",
                        ));
                    }
                }
                _ => {
                    return Err(RpcError::invalid(
                        "unsupported or malformed etcd metadata record",
                    ));
                }
            }
        }
        self.connection.execute_batch("PRAGMA temp_store=FILE;")?;
        let transaction = self
            .connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        for item in manifest.entries(b"lease") {
            let lease = leasepb::Lease::decode(item.value.as_slice())
                .map_err(|_| RpcError::invalid("invalid imported lease encoding"))?;
            if lease.id == 0
                || lease.id.to_be_bytes().as_slice() != item.key
                || lease.ttl < 1
                || lease.ttl > super::lease::MAX_LEASE_TTL
                || lease.remaining_ttl < 0
                || lease.remaining_ttl > lease.ttl
            {
                return Err(RpcError::invalid("invalid imported lease identity or TTL"));
            }
            transaction.execute("INSERT INTO leases(id, ttl, generation, renewed_at_ms, checkpoint_ttl) VALUES(?1, ?2, 1, 0, ?3)", params![lease.id, lease.ttl, lease.remaining_ttl])?;
        }
        let mut known_roles = BTreeSet::from(["root".to_owned()]);
        for item in manifest.entries(b"authRoles") {
            let role = authpb::Role::decode(item.value.as_slice())
                .map_err(|_| RpcError::invalid("invalid imported role encoding"))?;
            let name = std::str::from_utf8(&item.key)
                .map_err(|_| RpcError::invalid("role name is not UTF-8"))?;
            if role.name != item.key
                || role.key_permission.iter().any(|permission| {
                    !(0..=2).contains(&permission.perm_type) || permission.key.is_empty()
                })
            {
                return Err(RpcError::invalid(
                    "imported role identity or permissions are invalid",
                ));
            }
            known_roles.insert(name.to_owned());
            transaction.execute(
                "INSERT INTO auth_roles(name, role, checksum) VALUES(?1, ?2, ?3)",
                params![name, item.value, digest(&[&item.key, &item.value])],
            )?;
        }
        let mut has_root = false;
        for item in manifest.entries(b"authUsers") {
            let user = authpb::User::decode(item.value.as_slice())
                .map_err(|_| RpcError::invalid("invalid imported user encoding"))?;
            let name = std::str::from_utf8(&item.key)
                .map_err(|_| RpcError::invalid("user name is not UTF-8"))?;
            if user.name != item.key || user.roles.iter().any(|role| !known_roles.contains(role)) {
                return Err(RpcError::invalid(
                    "imported user identity or roles are invalid",
                ));
            }
            if !user
                .options
                .as_ref()
                .is_some_and(|options| options.no_password)
            {
                let hash = std::str::from_utf8(&user.password)
                    .map_err(|_| RpcError::invalid("imported password hash is invalid"))?;
                let hash: bcrypt::HashParts = hash
                    .parse()
                    .map_err(|_| RpcError::invalid("imported bcrypt hash is invalid"))?;
                if hash.get_cost() > 16 {
                    return Err(RpcError::invalid(
                        "imported bcrypt cost exceeds the verification policy",
                    ));
                }
            }
            has_root |= name == "root" && user.roles.iter().any(|role| role == "root");
            transaction.execute(
                "INSERT INTO auth_users(name, user, checksum) VALUES(?1, ?2, ?3)",
                params![name, item.value, digest(&[&item.key, &item.value])],
            )?;
        }
        if metadata.auth_enabled && !has_root {
            return Err(RpcError::invalid(
                "enabled imported auth has no root administrator",
            ));
        }
        import_history(
            &transaction,
            &mut metadata,
            manifest.revision,
            mark_compacted,
            manifest.bucket(b"key")?.iter().cloned().map(Ok),
        )?;
        save_metadata(&transaction, &metadata)?;
        transaction.commit()?;
        self.metadata = metadata;
        Ok(())
    }
}

fn import_history(
    transaction: &rusqlite::Transaction<'_>,
    metadata: &mut super::Metadata,
    revision: i64,
    mark_compacted: bool,
    records: impl Iterator<Item = Result<BucketEntry>>,
) -> Result<()> {
    transaction.execute_batch(
        "CREATE TEMP TABLE import_current(key BLOB PRIMARY KEY,kv BLOB NOT NULL) STRICT;",
    )?;
    let mut insert = transaction.prepare(
        "INSERT INTO mvcc_versions(main_revision, sub_revision, key, kind, kv, prev_kv, checksum) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
    )?;
    for item in records {
        let item = item?;
        let (main, sub, tombstone) = decode_revision(&item.key)?;
        let kv = mvccpb::KeyValue::decode(item.value.as_slice())
            .map_err(|_| RpcError::invalid("invalid imported KV encoding"))?;
        if main < 1
            || main > revision
            || kv.key.is_empty()
            || (!tombstone
                && (kv.create_revision < 1
                    || kv.mod_revision != main
                    || kv.create_revision > main
                    || kv.version < 1))
        {
            return Err(RpcError::invalid(
                "imported KV revision or identity is invalid",
            ));
        }
        let previous: Option<Vec<u8>> = transaction
            .query_row(
                "SELECT kv FROM import_current WHERE key=?1",
                [&kv.key],
                |row| row.get(0),
            )
            .optional()?;
        insert.execute(params![
            main,
            sub,
            kv.key,
            i32::from(tombstone),
            item.value,
            previous,
            history_digest(
                main,
                sub,
                &kv.key,
                i32::from(tombstone),
                &item.value,
                previous.as_deref()
            )
        ])?;
        if tombstone {
            transaction.execute("DELETE FROM import_current WHERE key=?1", [&kv.key])?;
        } else {
            transaction.execute(
                "INSERT INTO import_current(key,kv) VALUES(?1,?2) ON CONFLICT(key) DO UPDATE SET kv=excluded.kv",
                params![kv.key, item.value],
            )?;
        }
    }
    drop(insert);
    let mut current = transaction.prepare("SELECT kv FROM import_current ORDER BY key")?;
    let mut rows = current.query([])?;
    while let Some(row) = rows.next()? {
        let bytes: Vec<u8> = row.get(0)?;
        let kv = mvccpb::KeyValue::decode(bytes.as_slice())
            .map_err(|_| RpcError::corrupt("import staging KV cannot be decoded"))?;
        transaction.execute("INSERT INTO current_kvs(key, kv, create_revision, mod_revision, version, lease, checksum) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![kv.key, bytes, kv.create_revision, kv.mod_revision, kv.version, if kv.lease == 0 { None } else { Some(kv.lease) }, digest(&[&kv.key, &bytes])])?;
        if kv.lease != 0 {
            transaction.execute(
                "INSERT INTO lease_keys(lease, key) VALUES(?1, ?2)",
                params![kv.lease, kv.key],
            )?;
        }
    }
    drop(rows);
    drop(current);
    transaction.execute_batch("DROP TABLE import_current;")?;
    if mark_compacted {
        transaction.execute("DELETE FROM mvcc_versions WHERE NOT EXISTS (SELECT 1 FROM current_kvs WHERE current_kvs.key = mvcc_versions.key AND current_kvs.mod_revision = main_revision AND kind = 0)", [])?;
    }
    let logical: i64 = transaction.query_row("SELECT COALESCE((SELECT SUM(length(kv)) FROM current_kvs),0) + COALESCE((SELECT SUM(length(kv)+COALESCE(length(prev_kv),0)+128) FROM mvcc_versions),0)", [], |row| row.get(0))?;
    metadata.logical_bytes = u64::try_from(logical)
        .map_err(|_| RpcError::invalid("imported storage size is negative"))?;
    if metadata.logical_bytes > metadata.quota_bytes {
        return Err(RpcError::new(
            Code::ResourceExhausted,
            "imported application exceeds the configured quota",
        ));
    }
    Ok(())
}

fn decode_revision(key: &[u8]) -> Result<(i64, i64, bool)> {
    if (key.len() != 17 && key.len() != 18)
        || key[8] != b'_'
        || (key.len() == 18 && key[17] != b't')
    {
        return Err(RpcError::invalid("invalid etcd revision-key encoding"));
    }
    let main = i64::from_be_bytes(
        key[..8]
            .try_into()
            .map_err(|_| RpcError::invalid("invalid main revision width"))?,
    );
    let sub = i64::from_be_bytes(
        key[9..17]
            .try_into()
            .map_err(|_| RpcError::invalid("invalid subrevision width"))?,
    );
    if main < 0 || sub < 0 {
        return Err(RpcError::invalid("etcd revision exceeds signed range"));
    }
    Ok((main, sub, key.len() == 18))
}

fn validate_disk_schema(source: &rusqlite::Connection) -> Result<()> {
    type Columns<'a> = &'a [(&'a str, &'a str, i64)];
    let schema: &[(&str, Columns<'_>)] = &[
        ("interchange_buckets", &[("name", "BLOB", 1)]),
        (
            "interchange_entries",
            &[
                ("bucket", "BLOB", 1),
                ("key", "BLOB", 2),
                ("value", "BLOB", 0),
            ],
        ),
        (
            "interchange_meta",
            &[
                ("kind", "TEXT", 0),
                ("format", "INTEGER", 0),
                ("reference", "TEXT", 0),
                ("revision", "INTEGER", 0),
                ("compact_revision", "INTEGER", 0),
            ],
        ),
    ];
    let mut statement = source.prepare(
        "SELECT name,type FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY name",
    )?;
    let objects = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if objects.len() != schema.len()
        || objects
            .iter()
            .zip(schema)
            .any(|((name, kind), (expected, _))| name != expected || kind != "table")
    {
        return Err(RpcError::invalid(
            "interchange must contain only the three fixed tables",
        ));
    }
    for (table, expected) in schema {
        let strict: bool = source.query_row(
            "SELECT strict FROM pragma_table_list WHERE schema='main' AND name=?1",
            [table],
            |row| row.get(0),
        )?;
        let mut statement = source.prepare(&format!("PRAGMA main.table_info({table})"))?;
        let columns = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, bool>(3)?,
                    row.get::<_, i64>(5)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if !strict
            || columns.len() != expected.len()
            || columns.iter().zip(*expected).any(
                |(
                    (name, kind, required, primary),
                    (expected_name, expected_type, expected_primary),
                )| {
                    name != expected_name
                        || kind != expected_type
                        || !required
                        || primary != expected_primary
                },
            )
        {
            return Err(RpcError::invalid(
                "interchange table columns, types or primary keys are invalid",
            ));
        }
    }
    let count: i64 = source.query_row("SELECT COUNT(*) FROM interchange_meta", [], |row| {
        row.get(0)
    })?;
    let invalid: bool = source.query_row(
        "SELECT EXISTS(SELECT 1 FROM interchange_entries AS e WHERE length(key)=0 OR length(key)>?1 OR length(value)>?1 OR NOT EXISTS(SELECT 1 FROM interchange_buckets AS b WHERE b.name=e.bucket))",
        [16 << 20], |row| row.get(0),
    )?;
    if count != 1 || invalid {
        return Err(RpcError::invalid(
            "interchange requires one metadata record, owned entries and at most 16 MiB per key/value record",
        ));
    }
    Ok(())
}

pub fn restore(
    input: &Path,
    destination: &Path,
    identity: super::Identity,
    configuration: Option<&crate::cluster::Configuration>,
    revision_bump: i64,
    mark_compacted: bool,
) -> Result<super::snapshot::RestoreInfo> {
    restore_with_quota(
        input,
        destination,
        identity,
        configuration,
        revision_bump,
        mark_compacted,
        None,
    )
}

fn bind_import_configuration(
    store: &mut Store,
    configuration: &crate::cluster::Configuration,
) -> Result<()> {
    let identity = store.identity().clone();
    let transaction = store
        .connection
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    transaction.execute(
        "UPDATE node_state SET mode='cluster',checksum=?1 WHERE id=1",
        [super::identity_digest(&identity, "cluster", 0)?],
    )?;
    let bytes = serde_json::to_vec(configuration)?;
    transaction.execute(
        "INSERT INTO cluster_configuration(id,configuration,checksum) VALUES(1,?1,?2)",
        params![bytes, digest(&[&bytes])],
    )?;
    super::snapshot::insert_members(&transaction, configuration)?;
    super::membership::save_configuration(&transaction, configuration, 0, None)?;
    let mut metadata = store.metadata.clone();
    metadata.configuration_id = configuration.configuration_id;
    save_metadata(&transaction, &metadata)?;
    transaction.commit()?;
    store.metadata = metadata;
    Ok(())
}

pub fn restore_with_quota(
    input: &Path,
    destination: &Path,
    identity: super::Identity,
    configuration: Option<&crate::cluster::Configuration>,
    revision_bump: i64,
    mark_compacted: bool,
    quota_bytes: Option<u64>,
) -> Result<super::snapshot::RestoreInfo> {
    let source_sha256 = hash_file(input)?;
    let manifest = Manifest::read(input)?;
    if source_sha256 != hash_file(input)? {
        return Err(RpcError::invalid("source manifest changed while reading"));
    }
    if std::fs::symlink_metadata(destination).is_ok() {
        return Err(RpcError::new(
            Code::AlreadyExists,
            "import never replaces an existing directory",
        ));
    }
    let mut configuration = configuration.cloned();
    if let Some(configuration) = &mut configuration {
        configuration.validate()?;
        configuration.member(identity.member_id)?;
        if configuration.cluster_id != identity.cluster_id
            || configuration
                .members
                .iter()
                .any(|member| member.incarnation.is_some())
            || configuration
                .bootstrap_digest
                .as_ref()
                .is_some_and(|digest| digest != &source_sha256)
        {
            return Err(RpcError::invalid(
                "import requires a fresh configuration matching the new identity and verified source digest",
            ));
        }
        configuration.bootstrap_digest = Some(source_sha256.clone());
    }
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let staging = tempfile::tempdir_in(parent)?;
    let node = staging.path().join("node");
    let mut store = Store::create(&node, identity.clone())?;
    if let Some(quota) = quota_bytes {
        store.bootstrap_quota(quota)?;
    }
    store.import_manifest(&manifest, revision_bump, mark_compacted)?;
    if let Some(configuration) = &configuration {
        bind_import_configuration(&mut store, configuration)?;
    }
    let result = super::snapshot::RestoreInfo {
        cluster_id: identity.cluster_id,
        member_id: identity.member_id,
        revision: store.revision(),
        compact_revision: store.compact_revision(),
        applied_index: 0,
        configuration_id: store.metadata.configuration_id,
        schema_version: store.schema_version()?,
        source_sha256,
    };
    store
        .connection
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode = DELETE;")?;
    drop(store);
    super::snapshot::publish_node(&node.join("node.db"), destination)?;
    Ok(result)
}

pub fn restore_disk(
    input: &Path,
    destination: &Path,
    identity: super::Identity,
    configuration: Option<&crate::cluster::Configuration>,
    revision_bump: i64,
    mark_compacted: bool,
    quota_bytes: Option<u64>,
) -> Result<super::snapshot::RestoreInfo> {
    let (bytes, source_sha256) = super::snapshot::verified_payload(input)?;
    if bytes > 16 << 30 {
        return Err(RpcError::new(
            Code::ResourceExhausted,
            "disk-backed interchange exceeds the 16-GiB file limit",
        ));
    }
    let source = super::snapshot::open_artifact(input)?;
    let integrity: String = source.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    if integrity != "ok" {
        return Err(RpcError::invalid(format!(
            "interchange integrity: {integrity}"
        )));
    }
    validate_disk_schema(&source)?;
    let (kind, format, reference, revision, compact): (String, u32, String, i64, i64) = source
        .query_row(
            "SELECT kind,format,reference,revision,compact_revision FROM interchange_meta",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )?;
    if kind != "bbolt-interchange" || format != 2 || reference != "3.7.3" {
        return Err(RpcError::invalid(
            "unsupported disk-backed interchange profile",
        ));
    }
    let mut manifest = Manifest {
        format: 1,
        reference,
        revision,
        compact_revision: compact,
        buckets: Vec::new(),
    };
    let mut buckets = source.prepare("SELECT name FROM interchange_buckets ORDER BY name")?;
    let names = buckets
        .query_map([], |row| row.get::<_, Vec<u8>>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut metadata_bytes = 0_u64;
    for name in names {
        let mut entries = Vec::new();
        if name != b"key" {
            let mut statement = source.prepare(
                "SELECT key,value FROM interchange_entries WHERE bucket=?1 ORDER BY key",
            )?;
            let mut rows = statement.query([&name])?;
            while let Some(row) = rows.next()? {
                let key: Vec<u8> = row.get(0)?;
                let value: Vec<u8> = row.get(1)?;
                metadata_bytes = metadata_bytes
                    .checked_add((key.len() + value.len()) as u64)
                    .ok_or_else(|| RpcError::invalid("interchange metadata size overflow"))?;
                if metadata_bytes > MAX_MANIFEST_BYTES / 4 * 3 {
                    return Err(RpcError::new(
                        Code::ResourceExhausted,
                        "interchange auth/administrative metadata exceeds the bounded memory limit",
                    ));
                }
                entries.push(BucketEntry { key, value });
            }
        }
        manifest.buckets.push(Bucket { name, entries });
    }
    manifest.validate()?;
    if std::fs::symlink_metadata(destination).is_ok() {
        return Err(RpcError::new(
            Code::AlreadyExists,
            "import never replaces an existing directory",
        ));
    }
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let staging = tempfile::tempdir_in(parent)?;
    let node = staging.path().join("node");
    let mut store = Store::create(&node, identity.clone())?;
    if let Some(quota) = quota_bytes {
        store.bootstrap_quota(quota)?;
    }
    store.import_manifest(&manifest, revision_bump, mark_compacted)?;
    {
        let mut metadata = store.metadata.clone();
        let transaction = store
            .connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let mut statement = source
            .prepare("SELECT key,value FROM interchange_entries WHERE bucket=?1 ORDER BY key")?;
        let entries = statement.query_map([b"key"], |row| {
            Ok(BucketEntry {
                key: row.get(0)?,
                value: row.get(1)?,
            })
        })?;
        import_history(
            &transaction,
            &mut metadata,
            revision,
            mark_compacted,
            entries.map(|entry| entry.map_err(RpcError::from)),
        )?;
        save_metadata(&transaction, &metadata)?;
        transaction.commit()?;
        store.metadata = metadata;
    }
    if let Some(configuration) = configuration {
        configuration.validate()?;
        configuration.member(identity.member_id)?;
        if configuration.cluster_id != identity.cluster_id
            || configuration
                .members
                .iter()
                .any(|member| member.incarnation.is_some())
            || configuration
                .bootstrap_digest
                .as_ref()
                .is_some_and(|digest| digest != &source_sha256)
        {
            return Err(RpcError::invalid(
                "disk import requires a fresh configuration matching the verified source digest",
            ));
        }
        let mut configuration = configuration.clone();
        configuration.bootstrap_digest = Some(source_sha256.clone());
        bind_import_configuration(&mut store, &configuration)?;
    }
    let result = super::snapshot::RestoreInfo {
        cluster_id: identity.cluster_id,
        member_id: identity.member_id,
        revision: store.revision(),
        compact_revision: store.compact_revision(),
        applied_index: 0,
        configuration_id: store.metadata.configuration_id,
        schema_version: store.schema_version()?,
        source_sha256,
    };
    store
        .connection
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode=DELETE;")?;
    drop(store);
    if super::snapshot::verified_payload(input)? != (bytes, result.source_sha256.clone()) {
        return Err(RpcError::invalid(
            "source interchange changed while importing",
        ));
    }
    super::snapshot::publish_node(&node.join("node.db"), destination)?;
    Ok(result)
}

fn hash_file(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    if file.metadata()?.len() > MAX_MANIFEST_BYTES {
        return Err(RpcError::invalid(
            "manifest exceeds the bounded converter limit",
        ));
    }
    let mut hash = Sha256::new();
    let mut buffer = [0_u8; 64 << 10];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }
    Ok(hash
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}
