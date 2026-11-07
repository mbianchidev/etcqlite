use etcqlite::storage::{Identity, KvRequest, Store, auth::Principal};
use etcqlite::wire::etcdserverpb as pb;
use tempfile::TempDir;
use uuid::Uuid;

#[test]
fn inspection_projection_is_metadata_only_and_never_a_writable_internal_database() {
    let directory = TempDir::new().unwrap();
    let mut store = Store::create(
        directory.path().join("node"),
        Identity {
            cluster_id: 11,
            member_id: 22,
            name: "mock-inspection".into(),
        },
    )
    .unwrap();
    store
        .apply(
            1,
            Uuid::new_v4(),
            KvRequest::Put(pb::PutRequest {
                key: b"mock-private-key".to_vec(),
                value: b"mock-sensitive-value".to_vec(),
                ..Default::default()
            }),
        )
        .unwrap();
    let projection = store
        .inspection_projection(&Principal::Anonymous, false)
        .unwrap();
    let connection = rusqlite::Connection::open_with_flags(
        projection.path(),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let columns: (i64, i64, i64) = connection
        .query_row(
            "SELECT key_bytes, value_bytes, mod_revision FROM key_metadata",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(columns, (16, 20, 2));
    assert!(
        connection
            .execute("UPDATE key_metadata SET mod_revision = 99", [])
            .is_err()
    );
    let bytes = std::fs::read(projection.path()).unwrap();
    assert!(
        !bytes
            .windows(20)
            .any(|bytes| bytes == b"mock-sensitive-value")
    );
    assert!(!bytes.windows(16).any(|bytes| bytes == b"mock-private-key"));
    assert_eq!(store.revision(), 2);
    let raw = store
        .inspection_projection(&Principal::Anonymous, true)
        .unwrap();
    let connection = rusqlite::Connection::open_with_flags(
        raw.path(),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let value: Vec<u8> = connection
        .query_row("SELECT value FROM key_values", [], |row| row.get(0))
        .unwrap();
    assert_eq!(value, b"mock-sensitive-value");
}
