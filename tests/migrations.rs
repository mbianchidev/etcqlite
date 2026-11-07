use etcqlite::storage::{Identity, KvRequest, Store};
use etcqlite::wire::etcdserverpb as pb;
use tempfile::TempDir;
use uuid::Uuid;

#[test]
fn explicit_bootstrap_quota_is_persisted_and_cannot_reconfigure_an_active_node() {
    let directory = TempDir::new().unwrap();
    let node = directory.path().join("quota-node");
    let mut store = Store::create(
        &node,
        Identity {
            cluster_id: 11,
            member_id: 22,
            name: "mock-quota".into(),
        },
    )
    .unwrap();
    store.bootstrap_quota(8_589_934_592).unwrap();
    assert_eq!(store.quota_bytes(), 8_589_934_592);
    store
        .apply(
            1,
            Uuid::new_v4(),
            KvRequest::Put(pb::PutRequest {
                key: b"mock/quota".to_vec(),
                value: vec![0, 255],
                ..Default::default()
            }),
        )
        .unwrap();
    assert!(store.bootstrap_quota(1).is_err());
    drop(store);
    assert_eq!(Store::open(node).unwrap().quota_bytes(), 8_589_934_592);
}

#[test]
fn schema_down_and_up_are_backed_up_and_preserve_post_upgrade_writes() {
    let directory = TempDir::new().unwrap();
    let node = directory.path().join("node");
    let mut store = Store::create(
        &node,
        Identity {
            cluster_id: 11,
            member_id: 22,
            name: "mock-migration".into(),
        },
    )
    .unwrap();
    store
        .apply(
            1,
            Uuid::new_v4(),
            KvRequest::Put(pb::PutRequest {
                key: b"mock/migration".to_vec(),
                value: vec![0, 255],
                ..Default::default()
            }),
        )
        .unwrap();
    store
        .migrate(1, &directory.path().join("before-down.snapshot"))
        .unwrap();
    assert_eq!(store.schema_version().unwrap(), 1);
    store
        .apply(
            2,
            Uuid::new_v4(),
            KvRequest::Put(pb::PutRequest {
                key: b"mock/migration".to_vec(),
                value: vec![255, 0],
                ..Default::default()
            }),
        )
        .unwrap();
    store
        .migrate(2, &directory.path().join("before-up.snapshot"))
        .unwrap();
    assert_eq!(store.schema_version().unwrap(), 2);
    assert_eq!(store.revision(), 3);
    drop(store);
    let recovered = Store::open(node).unwrap();
    assert_eq!(
        recovered
            .range(&pb::RangeRequest {
                key: b"mock/migration".to_vec(),
                ..Default::default()
            })
            .unwrap()
            .kvs[0]
            .value,
        vec![255, 0]
    );
}
