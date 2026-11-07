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
