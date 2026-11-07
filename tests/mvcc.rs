use etcqlite::{
    storage::{Identity, KvReply, KvRequest, Store},
    wire::{etcdserverpb as pb, mvccpb::KeyValue},
};
use tempfile::TempDir;
use tonic::Code;
use uuid::Uuid;

fn database() -> (TempDir, Store) {
    let directory = TempDir::new().unwrap();
    let store = Store::create(
        directory.path().join("node"),
        Identity {
            cluster_id: u64::MAX,
            member_id: 1 << 63,
            name: "mock-node".into(),
        },
    )
    .unwrap();
    (directory, store)
}

fn apply(store: &mut Store, request: KvRequest) -> Result<KvReply, etcqlite::error::RpcError> {
    store.apply(store.applied_index() + 1, Uuid::new_v4(), request)
}

fn put(store: &mut Store, key: &[u8], value: &[u8]) -> pb::PutResponse {
    match apply(
        store,
        KvRequest::Put(pb::PutRequest {
            key: key.to_vec(),
            value: value.to_vec(),
            ..Default::default()
        }),
    )
    .unwrap()
    {
        KvReply::Put(response) => response,
        other => panic!("unexpected put reply: {other:?}"),
    }
}

fn range(store: &Store, key: &[u8], end: &[u8], revision: i64) -> pb::RangeResponse {
    store
        .range(&pb::RangeRequest {
            key: key.to_vec(),
            range_end: end.to_vec(),
            revision,
            ..Default::default()
        })
        .unwrap()
}

fn request_put(key: &[u8], value: &[u8]) -> pb::RequestOp {
    pb::RequestOp {
        request: Some(pb::request_op::Request::RequestPut(pb::PutRequest {
            key: key.to_vec(),
            value: value.to_vec(),
            ..Default::default()
        })),
    }
}

fn request_range(key: &[u8]) -> pb::RequestOp {
    pb::RequestOp {
        request: Some(pb::request_op::Request::RequestRange(pb::RangeRequest {
            key: key.to_vec(),
            ..Default::default()
        })),
    }
}

fn txn(store: &mut Store, request: pb::TxnRequest) -> pb::TxnResponse {
    match apply(store, KvRequest::Txn(request)).unwrap() {
        KvReply::Txn(response) => response,
        other => panic!("unexpected txn reply: {other:?}"),
    }
}

#[test]
fn initializes_explicitly_with_reference_counters_and_binary_unsigned_ids() {
    let (directory, store) = database();
    assert_eq!(store.revision(), 1);
    assert_eq!(store.compact_revision(), -1);
    assert_eq!(store.identity().cluster_id, u64::MAX);
    assert_eq!(store.identity().member_id, 1 << 63);
    assert!(Store::open(directory.path().join("node")).is_err());
    drop(store);
    let reopened = Store::open(directory.path().join("node")).unwrap();
    assert_eq!(reopened.identity().cluster_id, u64::MAX);
    assert_eq!(reopened.revision(), 1);
    assert!(Store::open(directory.path().join("missing")).is_err());
}

#[test]
fn puts_preserve_bytes_previous_values_and_key_generations() {
    let (_directory, mut store) = database();
    let key = [0, 255, 128, 0];
    put(&mut store, &key, &[255, 0, 128]);
    let old = range(&store, &key, &[], 0).kvs.remove(0);
    assert_eq!(old.create_revision, 2);
    assert_eq!(old.mod_revision, 2);
    assert_eq!(old.version, 1);
    let response = apply(
        &mut store,
        KvRequest::Put(pb::PutRequest {
            key: key.to_vec(),
            value: vec![],
            prev_kv: true,
            ..Default::default()
        }),
    )
    .unwrap();
    match response {
        KvReply::Put(response) => assert_eq!(response.prev_kv, Some(old.clone())),
        other => panic!("wrong response: {other:?}"),
    }
    let current = range(&store, &key, &[], 0).kvs.remove(0);
    assert_eq!(current.value, Vec::<u8>::new());
    assert_eq!(current.version, 2);
    assert_eq!(current.create_revision, 2);
    assert_eq!(current.mod_revision, 3);
    apply(
        &mut store,
        KvRequest::Delete(pb::DeleteRangeRequest {
            key: key.to_vec(),
            ..Default::default()
        }),
    )
    .unwrap();
    put(&mut store, &key, &[1]);
    let recreated = range(&store, &key, &[], 0).kvs.remove(0);
    assert_eq!(recreated.create_revision, 5);
    assert_eq!(recreated.version, 1);
    assert_eq!(range(&store, &key, &[], 2).kvs, vec![old]);
    assert!(range(&store, &key, &[], 4).kvs.is_empty());
}

#[test]
fn empty_deletion_and_read_only_transactions_do_not_allocate_revisions() {
    let (_directory, mut store) = database();
    let result = apply(
        &mut store,
        KvRequest::Delete(pb::DeleteRangeRequest {
            key: b"mock/missing".to_vec(),
            ..Default::default()
        }),
    )
    .unwrap();
    match result {
        KvReply::Delete(response) => assert_eq!(response.deleted, 0),
        other => panic!("wrong response: {other:?}"),
    }
    let result = txn(
        &mut store,
        pb::TxnRequest {
            success: vec![request_range(b"mock/missing")],
            ..Default::default()
        },
    );
    assert!(result.succeeded);
    assert_eq!(result.header.unwrap().revision, 1);
    assert_eq!(store.revision(), 1);
}

#[test]
fn one_modifying_transaction_has_one_revision_and_read_your_writes() {
    let (_directory, mut store) = database();
    let response = txn(
        &mut store,
        pb::TxnRequest {
            success: vec![
                request_range(b"mock/a"),
                request_put(b"mock/a", b"a"),
                request_put(b"mock/b", b"b"),
                request_range(b"mock/b"),
            ],
            ..Default::default()
        },
    );
    assert_eq!(response.header.as_ref().unwrap().revision, 2);
    assert_eq!(
        response.responses[0].response.as_ref().unwrap(),
        &pb::response_op::Response::ResponseRange(pb::RangeResponse {
            header: Some(pb::ResponseHeader {
                revision: 1,
                ..Default::default()
            }),
            ..Default::default()
        })
    );
    let last = match response.responses[3].response.as_ref().unwrap() {
        pb::response_op::Response::ResponseRange(range) => range,
        other => panic!("wrong last operation: {other:?}"),
    };
    assert_eq!(last.header.as_ref().unwrap().revision, 2);
    assert_eq!(last.kvs[0].mod_revision, 2);
    let keys = range(&store, b"mock/", &[0], 0).kvs;
    assert_eq!(keys.len(), 2);
    assert!(
        keys.iter()
            .all(|key| key.create_revision == 2 && key.version == 1)
    );
    let events = store.events(2, b"mock/", &[0]).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].0, 2);
    assert_eq!(events[0].1.len(), 2);
}

#[test]
fn nested_comparisons_use_the_pre_mutation_view_but_operations_read_writes() {
    let (_directory, mut store) = database();
    let nested = pb::TxnRequest {
        compare: vec![pb::Compare {
            key: b"mock/a".to_vec(),
            target: pb::compare::CompareTarget::Version as i32,
            target_union: Some(pb::compare::TargetUnion::Version(0)),
            ..Default::default()
        }],
        success: vec![
            request_put(b"mock/b", b"selected-before-writes"),
            request_range(b"mock/a"),
        ],
        failure: vec![request_put(b"mock/c", b"wrong")],
    };
    let response = txn(
        &mut store,
        pb::TxnRequest {
            success: vec![
                request_put(b"mock/a", b"a"),
                pb::RequestOp {
                    request: Some(pb::request_op::Request::RequestTxn(nested)),
                },
            ],
            ..Default::default()
        },
    );
    let nested = match response.responses[1].response.as_ref().unwrap() {
        pb::response_op::Response::ResponseTxn(response) => response,
        other => panic!("wrong nested response: {other:?}"),
    };
    assert!(nested.succeeded);
    assert_eq!(nested.header.as_ref().unwrap().revision, 0);
    assert_eq!(
        range(&store, b"mock/b", &[], 0).kvs[0].value,
        b"selected-before-writes"
    );
    assert!(range(&store, b"mock/c", &[], 0).kvs.is_empty());
}

#[test]
fn duplicate_puts_and_put_delete_overlap_are_rejected_on_both_paths() {
    let (_directory, mut store) = database();
    for request in [
        pb::TxnRequest {
            success: vec![request_put(b"mock/a", b"1"), request_put(b"mock/a", b"2")],
            ..Default::default()
        },
        pb::TxnRequest {
            failure: vec![
                request_put(b"mock/a", b"1"),
                pb::RequestOp {
                    request: Some(pb::request_op::Request::RequestDeleteRange(
                        pb::DeleteRangeRequest {
                            key: b"mock/".to_vec(),
                            range_end: vec![0],
                            ..Default::default()
                        },
                    )),
                },
            ],
            ..Default::default()
        },
    ] {
        let error = apply(&mut store, KvRequest::Txn(request)).unwrap_err();
        assert_eq!(error.status().code(), Code::InvalidArgument);
        assert_eq!(
            error.status().message(),
            "etcdserver: duplicate key given in txn request"
        );
    }
    assert_eq!(store.revision(), 1);
    assert!(range(&store, b"mock/", &[0], 0).kvs.is_empty());
}

#[test]
fn selected_path_errors_roll_back_and_unselected_lease_errors_are_not_applied() {
    let (_directory, mut store) = database();
    let invalid = pb::RequestOp {
        request: Some(pb::request_op::Request::RequestPut(pb::PutRequest {
            key: b"mock/b".to_vec(),
            lease: 123456,
            ..Default::default()
        })),
    };
    let error = apply(
        &mut store,
        KvRequest::Txn(pb::TxnRequest {
            success: vec![request_put(b"mock/a", b"would-be-partial"), invalid.clone()],
            ..Default::default()
        }),
    )
    .unwrap_err();
    assert_eq!(error.status().code(), Code::NotFound);
    assert_eq!(store.revision(), 1);
    assert!(range(&store, b"mock/a", &[], 0).kvs.is_empty());
    let response = txn(
        &mut store,
        pb::TxnRequest {
            success: vec![request_put(b"mock/a", b"valid")],
            failure: vec![invalid],
            ..Default::default()
        },
    );
    assert!(response.succeeded);
    assert_eq!(store.revision(), 2);
}

#[test]
fn missing_value_comparisons_fail_and_numeric_missing_versions_are_zero() {
    let (_directory, mut store) = database();
    for result in [
        pb::compare::CompareResult::Equal,
        pb::compare::CompareResult::NotEqual,
    ] {
        let response = txn(
            &mut store,
            pb::TxnRequest {
                compare: vec![pb::Compare {
                    key: b"mock/missing".to_vec(),
                    result: result as i32,
                    target: pb::compare::CompareTarget::Value as i32,
                    target_union: Some(pb::compare::TargetUnion::Value(vec![])),
                    ..Default::default()
                }],
                ..Default::default()
            },
        );
        assert!(!response.succeeded);
    }
    let response = txn(
        &mut store,
        pb::TxnRequest {
            compare: vec![pb::Compare {
                key: b"mock/missing".to_vec(),
                target: pb::compare::CompareTarget::Version as i32,
                target_union: Some(pb::compare::TargetUnion::Version(0)),
                ..Default::default()
            }],
            ..Default::default()
        },
    );
    assert!(response.succeeded);
    assert_eq!(store.revision(), 1);
}

#[test]
fn range_count_is_unfiltered_and_none_target_sort_honors_the_reference_limit_order() {
    let (_directory, mut store) = database();
    for (index, value) in [b"z", b"y", b"a"].into_iter().enumerate() {
        put(
            &mut store,
            format!("mock/sort/{index:02}").as_bytes(),
            value,
        );
    }
    let response = store
        .range(&pb::RangeRequest {
            key: b"mock/sort/".to_vec(),
            range_end: vec![0],
            limit: 1,
            sort_target: pb::range_request::SortTarget::Value as i32,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(response.count, 3);
    assert!(response.more);
    assert_eq!(response.kvs[0].key, b"mock/sort/01");
    let filtered = store
        .range(&pb::RangeRequest {
            key: b"mock/sort/".to_vec(),
            range_end: vec![0],
            min_mod_revision: 4,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(filtered.count, 3);
    assert_eq!(filtered.kvs.len(), 1);
    let counted = store
        .range(&pb::RangeRequest {
            key: b"mock/sort/".to_vec(),
            range_end: vec![0],
            count_only: true,
            min_mod_revision: 4,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(counted.count, 3);
    assert!(counted.kvs.is_empty());
}

#[test]
fn compaction_retains_live_floors_without_resurrecting_deleted_keys() {
    let (_directory, mut store) = database();
    put(&mut store, b"mock/live", b"old");
    put(&mut store, b"mock/deleted", b"deleted");
    apply(
        &mut store,
        KvRequest::Delete(pb::DeleteRangeRequest {
            key: b"mock/deleted".to_vec(),
            ..Default::default()
        }),
    )
    .unwrap();
    apply(
        &mut store,
        KvRequest::Compact(pb::CompactionRequest {
            revision: 4,
            ..Default::default()
        }),
    )
    .unwrap();
    put(&mut store, b"mock/new", b"new");
    assert_eq!(store.compact_revision(), 4);
    assert_eq!(range(&store, b"mock/", &[0], 5).kvs.len(), 2);
    assert_eq!(range(&store, b"mock/live", &[], 5).kvs[0].value, b"old");
    assert!(range(&store, b"mock/deleted", &[], 5).kvs.is_empty());
    assert_eq!(
        store
            .range(&pb::RangeRequest {
                key: b"mock/live".to_vec(),
                revision: 4,
                ..Default::default()
            })
            .unwrap_err()
            .status()
            .code(),
        Code::OutOfRange
    );
    assert!(store.events(4, b"mock/", &[0]).is_err());
    assert_eq!(range(&store, b"mock/", &[0], 0).header.unwrap().revision, 5);
}

#[test]
fn replaying_an_internal_request_does_not_repeat_its_mutation() {
    let (directory, mut store) = database();
    let identity = Uuid::new_v4();
    let request = KvRequest::Put(pb::PutRequest {
        key: b"mock/retry".to_vec(),
        value: b"value".to_vec(),
        ..Default::default()
    });
    let original = store.apply(1, identity, request.clone()).unwrap();
    let duplicate = store.apply(2, identity, request.clone()).unwrap();
    assert_eq!(duplicate, original);
    assert_eq!(store.revision(), 2);
    assert_eq!(store.applied_index(), 2);
    drop(store);
    let mut store = Store::open(directory.path().join("node")).unwrap();
    assert_eq!(store.apply(3, identity, request).unwrap(), original);
    assert_eq!(range(&store, b"mock/retry", &[], 0).kvs[0].version, 1);
}

#[test]
fn ignore_flags_require_existing_keys_and_reject_supplied_fields() {
    let (_directory, mut store) = database();
    let error = apply(
        &mut store,
        KvRequest::Put(pb::PutRequest {
            key: b"mock/missing".to_vec(),
            ignore_value: true,
            ..Default::default()
        }),
    )
    .unwrap_err();
    assert_eq!(error.status().message(), "etcdserver: key not found");
    let error = apply(
        &mut store,
        KvRequest::Put(pb::PutRequest {
            key: b"mock/a".to_vec(),
            ignore_value: true,
            value: vec![1],
            ..Default::default()
        }),
    )
    .unwrap_err();
    assert_eq!(error.status().message(), "etcdserver: value is provided");
    assert_eq!(store.revision(), 1);
}

#[test]
fn binary_open_ranges_keys_only_and_historical_headers_match_the_reference() {
    let (_directory, mut store) = database();
    let keys = [vec![0], vec![0, 0], vec![128], vec![255], vec![255, 255]];
    for key in &keys {
        put(&mut store, key, &[0, 255]);
    }
    let response = range(&store, &[0], &[0], 0);
    assert_eq!(
        response.kvs.iter().map(|kv| &kv.key).collect::<Vec<_>>(),
        keys.iter().collect::<Vec<_>>()
    );
    let historic = range(&store, &[0], &[0], 3);
    assert_eq!(historic.kvs.len(), 2);
    assert_eq!(historic.header.unwrap().revision, 6);
    let only = store
        .range(&pb::RangeRequest {
            key: vec![0],
            range_end: vec![0],
            keys_only: true,
            ..Default::default()
        })
        .unwrap();
    assert!(only.kvs.iter().all(|kv| kv.value.is_empty()));
    assert!(
        store
            .range(&pb::RangeRequest {
                key: vec![0],
                revision: 7,
                ..Default::default()
            })
            .is_err()
    );
}

#[test]
fn watch_history_has_correct_delete_metadata_and_previous_values() {
    let (_directory, mut store) = database();
    put(&mut store, b"mock/watch", b"first");
    put(&mut store, b"mock/watch", b"second");
    apply(
        &mut store,
        KvRequest::Delete(pb::DeleteRangeRequest {
            key: b"mock/watch".to_vec(),
            ..Default::default()
        }),
    )
    .unwrap();
    let history = store.events(2, b"mock/watch", &[]).unwrap();
    assert_eq!(history.len(), 3);
    assert_eq!(history[1].1[0].prev_kv.as_ref().unwrap().value, b"first");
    let deletion = &history[2].1[0];
    assert_eq!(
        deletion.r#type,
        etcqlite::wire::mvccpb::event::EventType::Delete as i32
    );
    assert_eq!(
        deletion.kv,
        Some(KeyValue {
            key: b"mock/watch".to_vec(),
            mod_revision: 4,
            ..Default::default()
        })
    );
    assert_eq!(deletion.prev_kv.as_ref().unwrap().value, b"second");
}

#[test]
fn request_size_and_nested_operation_budgets_are_enforced_atomically() {
    use prost::Message;

    let (_directory, mut store) = database();
    let error = apply(
        &mut store,
        KvRequest::Put(pb::PutRequest {
            key: b"mock/oversize".to_vec(),
            value: vec![0; etcqlite::storage::MAX_REQUEST_BYTES],
            ..Default::default()
        }),
    )
    .unwrap_err();
    assert_eq!(error.status().message(), "etcdserver: request is too large");
    let error = apply(
        &mut store,
        KvRequest::Txn(pb::TxnRequest {
            success: (0..=etcqlite::storage::MAX_TXN_OPS)
                .map(|_| request_range(b"mock/a"))
                .collect(),
            ..Default::default()
        }),
    )
    .unwrap_err();
    assert_eq!(
        error.status().message(),
        "etcdserver: too many operations in txn request"
    );
    assert_eq!(store.revision(), 1);
    let mut boundary = pb::PutRequest {
        key: b"mock/boundary".to_vec(),
        value: vec![0; etcqlite::storage::MAX_REQUEST_BYTES],
        ..Default::default()
    };
    let overhead = boundary.encoded_len() - boundary.value.len();
    boundary
        .value
        .truncate(etcqlite::storage::MAX_REQUEST_BYTES - overhead);
    assert_eq!(boundary.encoded_len(), etcqlite::storage::MAX_REQUEST_BYTES);
    apply(&mut store, KvRequest::Put(boundary)).unwrap();
    assert_eq!(store.revision(), 2);
}

#[test]
fn failed_sqlite_apply_never_publishes_a_partial_mutation_or_position() {
    let (directory, mut store) = database();
    let connection = rusqlite::Connection::open(store.path()).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER fail_history BEFORE INSERT ON mvcc_versions
         BEGIN SELECT RAISE(ABORT, 'injected application persistence failure'); END;",
        )
        .unwrap();
    let error = apply(
        &mut store,
        KvRequest::Put(pb::PutRequest {
            key: b"mock/fault".to_vec(),
            value: b"must-not-leak".to_vec(),
            ..Default::default()
        }),
    )
    .unwrap_err();
    assert_eq!(error.status().code(), Code::Internal);
    assert!(!store.is_healthy());
    assert_eq!(store.applied_index(), 0);
    drop(store);
    connection
        .execute_batch("DROP TRIGGER fail_history")
        .unwrap();
    let store = Store::open(directory.path().join("node")).unwrap();
    assert!(range(&store, b"mock/fault", &[], 0).kvs.is_empty());
    assert_eq!(store.revision(), 1);
    assert_eq!(store.applied_index(), 0);
}

#[test]
fn process_exit_without_destructors_keeps_state_and_apply_ledger_together() {
    const PATH: &str = "ETCQLITE_MVCC_CRASH_PROBE";
    let request_id = Uuid::from_u128(123456);
    let request = KvRequest::Put(pb::PutRequest {
        key: b"mock/crash".to_vec(),
        value: b"durable".to_vec(),
        ..Default::default()
    });
    if let Some(directory) = std::env::var_os(PATH) {
        let mut store = Store::create(
            directory,
            Identity {
                cluster_id: 1,
                member_id: 2,
                name: "mock-crash-node".into(),
            },
        )
        .unwrap();
        store.apply(1, request_id, request).unwrap();
        std::process::exit(73);
    }
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("node");
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "process_exit_without_destructors_keeps_state_and_apply_ledger_together",
        ])
        .env(PATH, &path)
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(73));
    let mut store = Store::open(path).unwrap();
    assert_eq!(store.revision(), 2);
    assert_eq!(store.applied_index(), 1);
    store.apply(2, request_id, request).unwrap();
    assert_eq!(range(&store, b"mock/crash", &[], 0).kvs[0].version, 1);
}
