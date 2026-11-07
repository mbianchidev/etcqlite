CREATE TABLE node_state (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    cluster_id BLOB NOT NULL CHECK(length(cluster_id) = 8),
    member_id BLOB NOT NULL CHECK(length(member_id) = 8),
    name TEXT NOT NULL,
    mode TEXT NOT NULL CHECK(mode IN ('local', 'cluster')),
    runtime_epoch INTEGER NOT NULL DEFAULT 0 CHECK(runtime_epoch >= 0),
    checksum BLOB NOT NULL
) STRICT;

CREATE TABLE app_state (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    schema_version INTEGER NOT NULL CHECK(schema_version = 1),
    codec_version INTEGER NOT NULL CHECK(codec_version = 1),
    revision INTEGER NOT NULL CHECK(revision >= 1),
    compacted_revision INTEGER NOT NULL CHECK(compacted_revision >= -1),
    auth_revision BLOB NOT NULL CHECK(length(auth_revision) = 8),
    auth_enabled INTEGER NOT NULL CHECK(auth_enabled IN (0, 1)),
    configuration_id INTEGER NOT NULL CHECK(configuration_id > 0),
    configuration_base INTEGER NOT NULL CHECK(configuration_base >= 0),
    applied_index INTEGER NOT NULL CHECK(applied_index >= 0),
    logical_bytes INTEGER NOT NULL CHECK(logical_bytes >= 0),
    quota_bytes INTEGER NOT NULL CHECK(quota_bytes > 0),
    downgrade_target TEXT,
    checksum BLOB NOT NULL
) STRICT;

CREATE TABLE leases (
    id INTEGER PRIMARY KEY CHECK(id != 0),
    ttl INTEGER NOT NULL CHECK(ttl > 0),
    generation INTEGER NOT NULL CHECK(generation > 0),
    renewed_at_ms INTEGER NOT NULL,
    checkpoint_ttl INTEGER NOT NULL DEFAULT 0 CHECK(checkpoint_ttl >= 0)
) STRICT;

CREATE TABLE current_kvs (
    key BLOB PRIMARY KEY CHECK(length(key) > 0),
    kv BLOB NOT NULL,
    create_revision INTEGER NOT NULL CHECK(create_revision > 0),
    mod_revision INTEGER NOT NULL CHECK(mod_revision >= create_revision),
    version INTEGER NOT NULL CHECK(version > 0),
    lease INTEGER REFERENCES leases(id),
    checksum BLOB NOT NULL
) STRICT;

CREATE TABLE lease_keys (
    lease INTEGER NOT NULL REFERENCES leases(id) ON DELETE CASCADE,
    key BLOB NOT NULL REFERENCES current_kvs(key) ON DELETE CASCADE,
    PRIMARY KEY(lease, key),
    UNIQUE(key)
) STRICT;

CREATE TABLE mvcc_versions (
    main_revision INTEGER NOT NULL CHECK(main_revision > 0),
    sub_revision INTEGER NOT NULL CHECK(sub_revision >= 0),
    key BLOB NOT NULL CHECK(length(key) > 0),
    kind INTEGER NOT NULL CHECK(kind IN (0, 1)),
    kv BLOB NOT NULL,
    prev_kv BLOB,
    checksum BLOB NOT NULL,
    PRIMARY KEY(main_revision, sub_revision)
) STRICT;

CREATE INDEX mvcc_key_history ON mvcc_versions(key, main_revision DESC, sub_revision DESC);

CREATE TABLE requests (
    id BLOB PRIMARY KEY CHECK(length(id) = 16),
    fingerprint BLOB NOT NULL,
    result BLOB NOT NULL,
    checksum BLOB NOT NULL
) STRICT;

CREATE TABLE auth_users (
    name TEXT PRIMARY KEY,
    user BLOB NOT NULL,
    checksum BLOB NOT NULL
) STRICT;

CREATE TABLE auth_roles (
    name TEXT PRIMARY KEY,
    role BLOB NOT NULL,
    checksum BLOB NOT NULL
) STRICT;

CREATE TABLE auth_tokens (
    token_hash BLOB PRIMARY KEY CHECK(length(token_hash) = 32),
    user TEXT NOT NULL REFERENCES auth_users(name) ON DELETE CASCADE,
    auth_revision BLOB NOT NULL CHECK(length(auth_revision) = 8),
    expires_at_ms INTEGER NOT NULL
) STRICT;

CREATE TABLE cluster_members (
    id BLOB PRIMARY KEY CHECK(length(id) = 8),
    pid INTEGER NOT NULL UNIQUE CHECK(pid > 0),
    member BLOB NOT NULL,
    checksum BLOB NOT NULL
) STRICT;

CREATE TABLE removed_members (
    id BLOB PRIMARY KEY CHECK(length(id) = 8)
) STRICT;

CREATE TABLE cluster_configuration (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    configuration BLOB NOT NULL,
    checksum BLOB NOT NULL
) STRICT;

CREATE TABLE alarms (
    member_id BLOB NOT NULL CHECK(length(member_id) = 8),
    alarm INTEGER NOT NULL CHECK(alarm IN (1, 2)),
    PRIMARY KEY(member_id, alarm)
) STRICT;

CREATE TABLE compaction_hashes (
    revision INTEGER PRIMARY KEY,
    compacted_revision INTEGER NOT NULL,
    hash INTEGER NOT NULL
) STRICT;
