use etcqlite::storage::{Identity, KvRequest, Store, auth::Principal, snapshot};
use etcqlite::wire::etcdserverpb as pb;
use tempfile::TempDir;
use uuid::Uuid;

fn identity(cluster: u64, member: u64) -> Identity {
    Identity {
        cluster_id: cluster,
        member_id: member,
        name: "mock-recovery".into(),
    }
}

#[test]
fn native_backup_restores_binary_history_under_a_new_fenced_identity() {
    let directory = TempDir::new().unwrap();
    let mut source = Store::create(directory.path().join("source"), identity(11, 22)).unwrap();
    for value in [vec![0, 255, 128], vec![128, 0, 255]] {
        source
            .apply(
                source.applied_index() + 1,
                Uuid::new_v4(),
                KvRequest::Put(pb::PutRequest {
                    key: vec![0, 255, 128],
                    value,
                    ..Default::default()
                }),
            )
            .unwrap();
    }
    let artifact = directory.path().join("verified.snapshot");
    source
        .native_snapshot(&Principal::Anonymous)
        .unwrap()
        .publish(&artifact)
        .unwrap();
    let restored = directory.path().join("restored");
    snapshot::restore(&artifact, &restored, identity(33, 44), None, 0, false).unwrap();
    let recovered = Store::open(&restored).unwrap();
    assert_eq!(recovered.identity().cluster_id, 33);
    assert_eq!(recovered.applied_index(), 0);
    assert_eq!(recovered.revision(), 3);
    for (revision, value) in [(2, vec![0, 255, 128]), (3, vec![128, 0, 255])] {
        assert_eq!(
            recovered
                .range(&pb::RangeRequest {
                    key: vec![0, 255, 128],
                    revision,
                    ..Default::default()
                })
                .unwrap()
                .kvs[0]
                .value,
            value
        );
    }
    assert!(snapshot::restore(&artifact, &restored, identity(55, 66), None, 0, false).is_err());
    assert!(
        snapshot::restore(
            &artifact,
            &directory.path().join("unsafe"),
            identity(11, 99),
            None,
            0,
            false
        )
        .is_err()
    );
    assert_eq!(
        Store::open(directory.path().join("source"))
            .err()
            .unwrap()
            .status()
            .code(),
        tonic::Code::Unavailable
    );
}

#[test]
fn application_translation_preserves_binary_history_leases_auth_and_post_write_rollback() {
    use etcqlite::storage::{auth::AuthCommand, command::Mutation, lease::LeaseCommand};
    let directory = TempDir::new().unwrap();
    let mut source = Store::create(directory.path().join("source"), identity(11, 22)).unwrap();
    for command in [
        Mutation::Lease(LeaseCommand::Grant {
            id: 7,
            ttl: 60,
            now_ms: 0,
        }),
        Mutation::Auth(AuthCommand::UserAdd {
            name: "root".into(),
            password_hash: bcrypt::hash("mock-password", 4).unwrap().into_bytes(),
            options: Default::default(),
        }),
        Mutation::Auth(AuthCommand::UserGrantRole {
            user: "root".into(),
            role: "root".into(),
        }),
        Mutation::Kv(KvRequest::Put(pb::PutRequest {
            key: vec![0, 255, 128],
            value: vec![128, 255, 0],
            lease: 7,
            ..Default::default()
        })),
        Mutation::Kv(KvRequest::Put(pb::PutRequest {
            key: vec![0, 255, 128],
            value: vec![0, 255, 128],
            lease: 7,
            ..Default::default()
        })),
        Mutation::Auth(AuthCommand::Enable),
    ] {
        source
            .command(
                source.applied_index() + 1,
                Uuid::new_v4(),
                Principal::Anonymous,
                command,
            )
            .unwrap();
    }
    let root = Principal::User {
        name: "root".into(),
        revision: source.auth_revision(),
    };
    let manifest = source.application_manifest(&root, 1).unwrap();
    let mut destination =
        Store::create(directory.path().join("destination"), identity(33, 44)).unwrap();
    destination.import_manifest(&manifest, 0, false).unwrap();
    assert_eq!(destination.revision(), source.revision());
    assert_eq!(destination.auth_revision(), source.auth_revision());
    assert_eq!(destination.lease(7).unwrap().unwrap().ttl, 60);
    for revision in [2, 3] {
        let request = pb::RangeRequest {
            key: vec![0, 255, 128],
            revision,
            ..Default::default()
        };
        assert_eq!(
            destination.authorized_range(&root, &request).unwrap(),
            source.authorized_range(&root, &request).unwrap()
        );
    }
    destination
        .command(
            1,
            Uuid::new_v4(),
            root.clone(),
            Mutation::Kv(KvRequest::Put(pb::PutRequest {
                key: b"mock/post-cutover".to_vec(),
                value: vec![0, 255],
                ..Default::default()
            })),
        )
        .unwrap();
    let reverse = destination.application_manifest(&root, 1).unwrap();
    let mut rollback = Store::create(directory.path().join("rollback"), identity(55, 66)).unwrap();
    rollback.import_manifest(&reverse, 1_000, true).unwrap();
    let request = pb::RangeRequest {
        key: b"mock/post-cutover".to_vec(),
        ..Default::default()
    };
    assert_eq!(
        rollback.authorized_range(&root, &request).unwrap().kvs[0].value,
        vec![0, 255]
    );
    assert_eq!(rollback.revision(), destination.revision() + 1_000);
    assert_eq!(rollback.compact_revision(), rollback.revision());
}

#[test]
fn disk_backed_etcd_interchange_preserves_binary_history_and_leases() {
    use etcqlite::storage::{auth::AuthCommand, command::Mutation, lease::LeaseCommand};
    let directory = TempDir::new().unwrap();
    let mut source = Store::create(directory.path().join("source"), identity(11, 22)).unwrap();
    source
        .command(
            1,
            Uuid::new_v4(),
            Principal::Anonymous,
            Mutation::Lease(LeaseCommand::Grant {
                id: 7,
                ttl: 60,
                now_ms: 0,
            }),
        )
        .unwrap();
    for command in [
        AuthCommand::UserAdd {
            name: "mock-user".into(),
            password_hash: bcrypt::hash("mock-password", 4).unwrap().into_bytes(),
            options: Default::default(),
        },
        AuthCommand::RoleAdd {
            name: "mock-role".into(),
        },
        AuthCommand::UserGrantRole {
            user: "mock-user".into(),
            role: "mock-role".into(),
        },
    ] {
        source
            .command(
                source.applied_index() + 1,
                Uuid::new_v4(),
                Principal::Anonymous,
                Mutation::Auth(command),
            )
            .unwrap();
    }
    for value in [vec![0, 255, 128], vec![128, 0, 255]] {
        source
            .apply(
                source.applied_index() + 1,
                Uuid::new_v4(),
                KvRequest::Put(pb::PutRequest {
                    key: vec![0, 255, 128],
                    value,
                    lease: 7,
                    ..Default::default()
                }),
            )
            .unwrap();
    }
    let native = source.native_snapshot(&Principal::Internal).unwrap();
    let tool = directory.path().join("converter");
    let result = std::process::Command::new("go")
        .args(["build", "-o"])
        .arg(&tool)
        .arg("./snapshot")
        .current_dir(format!("{}/tools", env!("CARGO_MANIFEST_DIR")))
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let upstream = directory.path().join("upstream.db");
    let staged = directory.path().join("interchange ?%.sqlite");
    for (operation, input, output) in [
        ("export-native", native.path(), upstream.as_path()),
        ("stage", upstream.as_path(), staged.as_path()),
    ] {
        let result = std::process::Command::new(&tool)
            .arg(operation)
            .arg(input)
            .arg(output)
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
    }
    let restored = directory.path().join("restored");
    etcqlite::storage::interchange::restore_disk(
        &staged,
        &restored,
        identity(33, 44),
        None,
        0,
        false,
        None,
    )
    .unwrap();
    let recovered = Store::open(restored).unwrap();
    assert_eq!(recovered.revision(), source.revision());
    assert_eq!(recovered.lease(7).unwrap().unwrap().ttl, 60);
    assert_eq!(recovered.auth_revision(), source.auth_revision());
    let original_db = rusqlite::Connection::open_with_flags(
        source.path(),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let recovered_db = rusqlite::Connection::open_with_flags(
        recovered.path(),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let original_user: Vec<u8> = original_db
        .query_row(
            "SELECT user FROM auth_users WHERE name='mock-user'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let recovered_user: Vec<u8> = recovered_db
        .query_row(
            "SELECT user FROM auth_users WHERE name='mock-user'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(original_user, recovered_user);
    for revision in [2, 3] {
        let request = pb::RangeRequest {
            key: vec![0, 255, 128],
            revision,
            ..Default::default()
        };
        assert_eq!(
            source.range(&request).unwrap(),
            recovered.range(&request).unwrap()
        );
    }
    let configuration = etcqlite::cluster::Configuration {
        cluster_id: 55,
        configuration_id: 1,
        bootstrap_digest: None,
        members: (66..=68)
            .map(|id| etcqlite::cluster::Member {
                id,
                pid: id - 65,
                name: format!("mock-import-{id}"),
                learner: false,
                client_url: format!("https://127.0.0.1:{}", 2379 + id),
                peer_url: format!("https://127.0.0.1:{}", 2380 + id),
                additional_peer_urls: Vec::new(),
                incarnation: None,
            })
            .collect(),
    };
    let clustered = directory.path().join("clustered-import");
    let imported = etcqlite::storage::interchange::restore_disk(
        &staged,
        &clustered,
        identity(55, 66),
        Some(&configuration),
        0,
        false,
        None,
    )
    .unwrap();
    let clustered = Store::open(clustered).unwrap();
    let installed = clustered.cluster_configuration().unwrap().unwrap();
    assert_eq!(
        installed.bootstrap_digest.as_deref(),
        Some(imported.source_sha256.as_str())
    );
    assert_eq!(installed.members, configuration.members);
    assert_eq!(clustered.revision(), source.revision());
    for (index, mutation) in [
        "INSERT INTO interchange_meta SELECT * FROM interchange_meta",
        "INSERT INTO interchange_entries VALUES(x'6f727068616e',x'01',x'02')",
        "CREATE VIEW hidden_values AS SELECT value FROM interchange_entries",
        "ALTER TABLE interchange_meta ADD COLUMN hidden TEXT",
        "UPDATE interchange_meta SET compact_revision=-2",
        "UPDATE interchange_entries SET value=zeroblob(16777217) WHERE bucket=x'6b6579'",
        "DROP TABLE interchange_entries; CREATE TABLE interchange_entries(bucket BLOB NOT NULL,key BLOB NOT NULL,value BLOB NOT NULL,PRIMARY KEY(bucket,key))",
    ].iter().enumerate() {
        use sha2::{Digest, Sha256};
        use std::io::Write;
        let corrupt = directory.path().join(format!("invalid-{index}.sqlite"));
        std::fs::copy(&staged, &corrupt).unwrap();
        let file = std::fs::OpenOptions::new().write(true).open(&corrupt).unwrap();
        file.set_len(file.metadata().unwrap().len()-32).unwrap();
        drop(file);
        let connection = rusqlite::Connection::open(&corrupt).unwrap();
        connection.execute_batch(mutation).unwrap();
        drop(connection);
        let digest = Sha256::digest(std::fs::read(&corrupt).unwrap());
        let mut file = std::fs::OpenOptions::new().append(true).open(&corrupt).unwrap();
        file.write_all(&digest).unwrap();
        file.sync_all().unwrap();
        drop(file);
        let destination = directory.path().join(format!("rejected-{index}"));
        assert!(etcqlite::storage::interchange::restore_disk(
            &corrupt, &destination, identity(55,66), None,0,false,None,
        ).is_err(), "invalid schema/record was accepted: {mutation}");
        assert!(!destination.exists(), "failed import published an incomplete node");
    }
}

#[test]
fn logical_corruption_is_rejected_before_backup_or_voting_restart() {
    let directory = TempDir::new().unwrap();
    let node = directory.path().join("node");
    let mut store = Store::create(&node, identity(11, 22)).unwrap();
    store
        .apply(
            1,
            Uuid::new_v4(),
            KvRequest::Put(pb::PutRequest {
                key: b"mock/checksum".to_vec(),
                value: b"verified".to_vec(),
                ..Default::default()
            }),
        )
        .unwrap();
    let connection = rusqlite::Connection::open(node.join("node.db")).unwrap();
    connection
        .execute("UPDATE current_kvs SET checksum = zeroblob(32)", [])
        .unwrap();
    assert!(store.native_snapshot(&Principal::Anonymous).is_err());
    drop(store);
    assert!(Store::open(node).is_err());
}
