use etcqlite::storage::{Identity, KvRequest, Store};
use etcqlite::wire::etcdserverpb as pb;
use tempfile::TempDir;
use uuid::Uuid;

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
