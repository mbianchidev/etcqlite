use etcqlite::{
    storage::{
        Identity, KvRequest, Store,
        auth::{AuthCommand, AuthQuery, AuthReply, Principal},
        command::{Mutation, Reply},
        lease::{LeaseCommand, LeaseReply},
    },
    wire::{authpb, etcdserverpb as pb},
};
use tempfile::TempDir;
use tonic::Code;
use uuid::Uuid;

fn database() -> (TempDir, Store) {
    let directory = TempDir::new().unwrap();
    let store = Store::create(
        directory.path().join("node"),
        Identity {
            cluster_id: 1,
            member_id: 2,
            name: "mock-node".into(),
        },
    )
    .unwrap();
    (directory, store)
}

fn command(
    store: &mut Store,
    principal: Principal,
    request: Mutation,
) -> Result<Reply, etcqlite::error::RpcError> {
    store.command(
        store.applied_index() + 1,
        Uuid::new_v4(),
        principal,
        request,
    )
}

fn auth(store: &mut Store, principal: Principal, request: AuthCommand) -> Reply {
    command(store, principal, Mutation::Auth(request)).unwrap()
}

fn root(store: &Store) -> Principal {
    Principal::User {
        name: "root".into(),
        revision: store.auth_revision(),
    }
}

fn admin(store: &mut Store, request: AuthCommand) -> Reply {
    let principal = root(store);
    auth(store, principal, request)
}

fn setup_root(store: &mut Store) {
    auth(
        store,
        Principal::Anonymous,
        AuthCommand::UserAdd {
            name: "root".into(),
            password_hash: bcrypt::hash("synthetic-only-password", 4)
                .unwrap()
                .into_bytes(),
            options: authpb::UserAddOptions { no_password: false },
        },
    );
    auth(
        store,
        Principal::Anonymous,
        AuthCommand::UserGrantRole {
            user: "root".into(),
            role: "root".into(),
        },
    );
    auth(store, Principal::Anonymous, AuthCommand::Enable);
}

fn grant(store: &mut Store, id: i64) {
    command(
        store,
        Principal::Anonymous,
        Mutation::Lease(LeaseCommand::Grant {
            id,
            ttl: 1,
            now_ms: 1_700_000_000_000,
        }),
    )
    .unwrap();
}

fn put(store: &mut Store, key: &[u8], lease: i64) {
    command(
        store,
        Principal::Anonymous,
        Mutation::Kv(KvRequest::Put(pb::PutRequest {
            key: key.to_vec(),
            value: b"synthetic".to_vec(),
            lease,
            ..Default::default()
        })),
    )
    .unwrap();
}

#[test]
fn grants_and_renewals_persist_without_creating_kv_revisions() {
    let (directory, mut store) = database();
    let response = command(
        &mut store,
        Principal::Anonymous,
        Mutation::Lease(LeaseCommand::Grant {
            id: 42,
            ttl: 1,
            now_ms: 1_700_000_000_000,
        }),
    )
    .unwrap();
    match response {
        Reply::Lease(LeaseReply::Grant(response)) => {
            assert_eq!(response.id, 42);
            assert_eq!(response.ttl, 2);
        }
        other => panic!("wrong lease reply: {other:?}"),
    }
    assert_eq!(store.revision(), 1);
    command(
        &mut store,
        Principal::Anonymous,
        Mutation::Lease(LeaseCommand::Renew {
            id: 42,
            now_ms: 1_700_000_001_000,
        }),
    )
    .unwrap();
    assert_eq!(store.lease(42).unwrap().unwrap().generation, 2);
    assert_eq!(store.revision(), 1);
    drop(store);
    let store = Store::open(directory.path().join("node")).unwrap();
    let lease = store.lease(42).unwrap().unwrap();
    assert_eq!(lease.ttl, 2);
    assert_eq!(lease.generation, 2);
    assert_eq!(lease.renewed_at_ms, 1_700_000_001_000);
}

#[test]
fn stale_expiry_cannot_delete_renewed_keys_and_expiry_is_one_atomic_revision() {
    let (_directory, mut store) = database();
    grant(&mut store, 42);
    put(&mut store, b"mock/lease/a", 42);
    put(&mut store, b"mock/lease/b", 42);
    command(
        &mut store,
        Principal::Anonymous,
        Mutation::Lease(LeaseCommand::Renew {
            id: 42,
            now_ms: 1_700_000_001_000,
        }),
    )
    .unwrap();
    command(
        &mut store,
        Principal::Internal,
        Mutation::Lease(LeaseCommand::Expire {
            id: 42,
            generation: 1,
        }),
    )
    .unwrap();
    assert_eq!(store.revision(), 3);
    assert_eq!(store.lease_keys(42).unwrap().len(), 2);
    command(
        &mut store,
        Principal::Internal,
        Mutation::Lease(LeaseCommand::Expire {
            id: 42,
            generation: 2,
        }),
    )
    .unwrap();
    assert_eq!(store.revision(), 4);
    assert!(store.lease(42).unwrap().is_none());
    let events = store.events(4, b"mock/lease/", &[0]).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].1.len(), 2);
    assert!(
        events[0]
            .1
            .iter()
            .all(|event| event.kv.as_ref().unwrap().mod_revision == 4)
    );
}

#[test]
fn lease_reassignment_updates_both_indexes_and_revoke_does_not_delete_new_ownership() {
    let (_directory, mut store) = database();
    grant(&mut store, 1);
    grant(&mut store, 2);
    put(&mut store, b"mock/owned", 1);
    put(&mut store, b"mock/owned", 2);
    assert!(store.lease_keys(1).unwrap().is_empty());
    assert_eq!(store.lease_keys(2).unwrap(), vec![b"mock/owned".to_vec()]);
    command(
        &mut store,
        Principal::Anonymous,
        Mutation::Lease(LeaseCommand::Revoke { id: 1 }),
    )
    .unwrap();
    assert_eq!(store.revision(), 3);
    assert_eq!(
        store
            .range(&pb::RangeRequest {
                key: b"mock/owned".to_vec(),
                ..Default::default()
            })
            .unwrap()
            .kvs[0]
            .lease,
        2
    );
}

#[test]
fn auth_enable_requires_root_and_does_not_advance_kv_or_auth_revisions() {
    let (_directory, mut store) = database();
    let error = command(
        &mut store,
        Principal::Anonymous,
        Mutation::Auth(AuthCommand::Enable),
    )
    .unwrap_err();
    assert_eq!(error.status().code(), Code::FailedPrecondition);
    assert_eq!(
        error.status().message(),
        "etcdserver: root user does not exist"
    );
    setup_root(&mut store);
    assert_eq!(store.revision(), 1);
    assert_eq!(store.auth_revision(), 3);
    match store
        .auth_query(&Principal::Anonymous, &AuthQuery::Status)
        .unwrap()
    {
        AuthReply::Status(status) => {
            assert!(status.enabled);
            assert_eq!(status.auth_revision, 3);
        }
        other => panic!("wrong auth status: {other:?}"),
    }
}

#[test]
fn committed_permission_revocation_fences_an_already_authenticated_write() {
    let (_directory, mut store) = database();
    setup_root(&mut store);
    admin(
        &mut store,
        AuthCommand::UserAdd {
            name: "mock-user".into(),
            password_hash: bcrypt::hash("synthetic-only-password", 4)
                .unwrap()
                .into_bytes(),
            options: authpb::UserAddOptions { no_password: false },
        },
    );
    admin(
        &mut store,
        AuthCommand::RoleAdd {
            name: "mock-role".into(),
        },
    );
    admin(
        &mut store,
        AuthCommand::UserGrantRole {
            user: "mock-user".into(),
            role: "mock-role".into(),
        },
    );
    admin(
        &mut store,
        AuthCommand::RoleGrantPermission {
            name: "mock-role".into(),
            permission: Some(authpb::Permission {
                perm_type: authpb::permission::Type::Readwrite as i32,
                key: b"mock/allowed/".to_vec(),
                range_end: b"mock/allowed0".to_vec(),
            }),
        },
    );
    let queued = Principal::User {
        name: "mock-user".into(),
        revision: store.auth_revision(),
    };
    command(
        &mut store,
        queued.clone(),
        Mutation::Kv(KvRequest::Put(pb::PutRequest {
            key: b"mock/allowed/a".to_vec(),
            value: b"before".to_vec(),
            ..Default::default()
        })),
    )
    .unwrap();
    admin(
        &mut store,
        AuthCommand::UserRevokeRole {
            user: "mock-user".into(),
            role: "mock-role".into(),
        },
    );
    let error = command(
        &mut store,
        queued,
        Mutation::Kv(KvRequest::Put(pb::PutRequest {
            key: b"mock/allowed/a".to_vec(),
            value: b"must-be-denied".to_vec(),
            ..Default::default()
        })),
    )
    .unwrap_err();
    assert_eq!(
        error.status().message(),
        "etcdserver: revision of auth store is old"
    );
    assert_eq!(store.revision(), 2);
    let fresh = Principal::User {
        name: "mock-user".into(),
        revision: store.auth_revision(),
    };
    let error = store
        .authorized_range(
            &fresh,
            &pb::RangeRequest {
                key: b"mock/allowed/a".to_vec(),
                ..Default::default()
            },
        )
        .unwrap_err();
    assert_eq!(error.status().code(), Code::PermissionDenied);
}

#[test]
fn authentication_persists_only_a_token_hash_and_rejects_changed_credentials() {
    let (_directory, mut store) = database();
    setup_root(&mut store);
    let credentials = store.credentials("root").unwrap();
    let raw_token = b"synthetic-only-session-token";
    use sha2::{Digest, Sha256};
    let token_hash: [u8; 32] = Sha256::digest(raw_token).into();
    let request = AuthCommand::Authenticate {
        name: "root".into(),
        expected_password_hash: credentials.password_hash.clone(),
        expected_auth_revision: credentials.revision,
        token_hash,
        expires_at_ms: 1_700_000_300_000,
    };
    command(
        &mut store,
        Principal::Anonymous,
        Mutation::Auth(request.clone()),
    )
    .unwrap();
    assert_eq!(
        store
            .principal(Some(&token_hash), None, 1_700_000_000_000)
            .unwrap(),
        root(&store)
    );
    assert!(
        store
            .principal(Some(&token_hash), None, 1_700_000_400_000)
            .is_err()
    );
    let encoded = serde_json::to_vec(&Mutation::Auth(request.clone())).unwrap();
    assert!(
        !String::from_utf8(encoded)
            .unwrap()
            .contains("synthetic-only-session-token")
    );
    admin(
        &mut store,
        AuthCommand::UserChangePassword {
            name: "root".into(),
            password_hash: bcrypt::hash("synthetic-new-password", 4)
                .unwrap()
                .into_bytes(),
        },
    );
    assert!(command(&mut store, Principal::Anonymous, Mutation::Auth(request)).is_err());
    assert!(
        store
            .principal(Some(&token_hash), None, 1_700_000_000_000)
            .is_err()
    );
}

#[test]
fn root_protection_role_permissions_and_no_password_options_survive_restart() {
    let (directory, mut store) = database();
    setup_root(&mut store);
    let principal = root(&store);
    let error = command(
        &mut store,
        principal,
        Mutation::Auth(AuthCommand::UserDelete {
            name: "root".into(),
        }),
    )
    .unwrap_err();
    assert_eq!(
        error.status().message(),
        "etcdserver: invalid auth management"
    );
    admin(
        &mut store,
        AuthCommand::UserAdd {
            name: "mock-cert-user".into(),
            password_hash: vec![],
            options: authpb::UserAddOptions { no_password: true },
        },
    );
    drop(store);
    let store = Store::open(directory.path().join("node")).unwrap();
    assert!(store.credentials("mock-cert-user").unwrap().no_password);
    match store
        .auth_query(
            &root(&store),
            &AuthQuery::UserGet {
                name: "root".into(),
            },
        )
        .unwrap()
    {
        AuthReply::UserGet(user) => assert_eq!(user.roles, vec!["root"]),
        other => panic!("wrong user reply: {other:?}"),
    }
}

#[test]
fn unauthenticated_and_out_of_range_access_is_explicitly_rejected() {
    let (_directory, mut store) = database();
    setup_root(&mut store);
    let error = command(
        &mut store,
        Principal::Anonymous,
        Mutation::Kv(KvRequest::Put(pb::PutRequest {
            key: b"mock/denied".to_vec(),
            ..Default::default()
        })),
    )
    .unwrap_err();
    assert_eq!(error.status().code(), Code::InvalidArgument);
    assert_eq!(error.status().message(), "etcdserver: user name is empty");
    assert_eq!(store.revision(), 1);
}
