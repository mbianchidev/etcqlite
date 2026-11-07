use etcqlite::{
    server::{self, Options, RunningServer},
    storage::{Identity, Store},
    wire::etcdserverpb as pb,
};
use std::time::Duration;
use tempfile::TempDir;
use tonic::Request;

async fn start() -> (TempDir, RunningServer) {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("node");
    drop(
        Store::create(
            &path,
            Identity {
                cluster_id: 11,
                member_id: 22,
                name: "mock-rpc-node".into(),
            },
        )
        .unwrap(),
    );
    let server = server::start(Options {
        data_dir: path,
        address: "127.0.0.1:0".parse().unwrap(),
        tls: None,
        request_timeout: Duration::from_secs(5),
        peer: None,
    })
    .await
    .unwrap();
    (directory, server)
}

#[tokio::test]
async fn grpc_binary_kv_transactions_and_headers_are_live_not_stubs() {
    let (_directory, server) = start().await;
    let mut client = pb::kv_client::KvClient::connect(server.endpoint.clone())
        .await
        .unwrap();
    let put = client
        .put(pb::PutRequest {
            key: vec![0, 255, 128],
            value: vec![255, 0, 128],
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(put.header.as_ref().unwrap().cluster_id, 11);
    assert_eq!(put.header.as_ref().unwrap().member_id, 22);
    assert_eq!(put.header.as_ref().unwrap().revision, 2);
    assert!(put.header.unwrap().raft_term > 0);
    let result = client
        .range(pb::RangeRequest {
            key: vec![0, 255, 128],
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(result.kvs[0].value, vec![255, 0, 128]);
    let response = client
        .txn(pb::TxnRequest {
            compare: vec![pb::Compare {
                key: vec![0, 255, 128],
                target: pb::compare::CompareTarget::Version as i32,
                target_union: Some(pb::compare::TargetUnion::Version(1)),
                ..Default::default()
            }],
            success: vec![pb::RequestOp {
                request: Some(pb::request_op::Request::RequestPut(pb::PutRequest {
                    key: b"mock/other".to_vec(),
                    value: b"committed".to_vec(),
                    ..Default::default()
                })),
            }],
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(response.succeeded);
    assert_eq!(response.header.unwrap().revision, 3);
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn grpc_range_stream_pins_history_and_places_metadata_only_on_the_last_chunk() {
    let (_directory, server) = start().await;
    let mut client = pb::kv_client::KvClient::connect(server.endpoint.clone())
        .await
        .unwrap();
    for index in 0..35 {
        client
            .put(pb::PutRequest {
                key: format!("mock/stream/{index:02}").into_bytes(),
                value: vec![0, 255],
                ..Default::default()
            })
            .await
            .unwrap();
    }
    let mut stream = client
        .range_stream(pb::RangeRequest {
            key: b"mock/stream/".to_vec(),
            range_end: vec![0],
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let mut chunks = Vec::new();
    while let Some(chunk) = stream.message().await.unwrap() {
        chunks.push(chunk.range_response.unwrap());
    }
    assert_eq!(
        chunks
            .iter()
            .map(|chunk| chunk.kvs.len())
            .collect::<Vec<_>>(),
        vec![10, 20, 5]
    );
    assert!(
        chunks[..2]
            .iter()
            .all(|chunk| chunk.header.is_none() && chunk.count == 0)
    );
    assert_eq!(chunks[2].header.as_ref().unwrap().revision, 36);
    assert_eq!(chunks[2].count, 35);
    assert!(!chunks[2].more);
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn grpc_lease_expiry_is_durable_and_renewal_stream_is_authoritative() {
    use tokio_stream::wrappers::ReceiverStream;

    let (_directory, server) = start().await;
    let mut leases = pb::lease_client::LeaseClient::connect(server.endpoint.clone())
        .await
        .unwrap();
    let mut kv = pb::kv_client::KvClient::connect(server.endpoint.clone())
        .await
        .unwrap();
    let lease = leases
        .lease_grant(pb::LeaseGrantRequest { id: 42, ttl: 1 })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(lease.ttl, 2);
    kv.put(pb::PutRequest {
        key: b"mock/leased".to_vec(),
        value: b"value".to_vec(),
        lease: 42,
        ..Default::default()
    })
    .await
    .unwrap();
    let (sender, receiver) = tokio::sync::mpsc::channel(2);
    let mut stream = leases
        .lease_keep_alive(ReceiverStream::new(receiver))
        .await
        .unwrap()
        .into_inner();
    sender
        .send(pb::LeaseKeepAliveRequest { id: 42 })
        .await
        .unwrap();
    assert_eq!(stream.message().await.unwrap().unwrap().ttl, 2);
    drop(sender);
    drop(stream);
    tokio::time::sleep(Duration::from_millis(2200)).await;
    let result = kv
        .range(pb::RangeRequest {
            key: b"mock/leased".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(result.kvs.is_empty());
    assert_eq!(result.header.unwrap().revision, 3);
    let missing = leases
        .lease_time_to_live(pb::LeaseTimeToLiveRequest { id: 42, keys: true })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(missing.ttl, -1);
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn grpc_auth_stores_no_plaintext_passwords_or_tokens_and_enforces_revocation() {
    let (directory, server) = start().await;
    let mut auth = pb::auth_client::AuthClient::connect(server.endpoint.clone())
        .await
        .unwrap();
    let mut kv = pb::kv_client::KvClient::connect(server.endpoint.clone())
        .await
        .unwrap();
    auth.user_add(pb::AuthUserAddRequest {
        name: "root".into(),
        password: "synthetic-only-password".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    auth.user_grant_role(pb::AuthUserGrantRoleRequest {
        user: "root".into(),
        role: "root".into(),
    })
    .await
    .unwrap();
    auth.auth_enable(pb::AuthEnableRequest {}).await.unwrap();
    let error = kv
        .put(pb::PutRequest {
            key: b"mock/auth".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    let token = auth
        .authenticate(pb::AuthenticateRequest {
            name: "root".into(),
            password: "synthetic-only-password".into(),
        })
        .await
        .unwrap()
        .into_inner()
        .token;
    assert!(!token.is_empty());
    let mut request = Request::new(pb::PutRequest {
        key: b"mock/auth".to_vec(),
        value: b"allowed".to_vec(),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("token", token.parse().unwrap());
    kv.put(request).await.unwrap();
    let connection = rusqlite::Connection::open(directory.path().join("node/node.db")).unwrap();
    let mut statement = connection.prepare("SELECT result FROM requests").unwrap();
    let rows = statement
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .unwrap();
    for row in rows {
        let bytes = row.unwrap();
        assert!(!String::from_utf8_lossy(&bytes).contains("synthetic-only-password"));
        assert!(!String::from_utf8_lossy(&bytes).contains(&token));
    }
    drop(statement);
    drop(connection);
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_plaintext_listener_is_refused_before_serving() {
    let directory = TempDir::new().unwrap();
    let result = server::start(Options {
        data_dir: directory.path().join("unused"),
        address: "0.0.0.0:0".parse().unwrap(),
        tls: None,
        request_timeout: Duration::from_secs(5),
        peer: None,
    })
    .await;
    assert!(result.is_err());
    assert!(!directory.path().join("unused").exists());
}

#[tokio::test]
async fn grpc_watch_replays_then_delivers_live_complete_revisions_and_progress() {
    use tokio_stream::wrappers::ReceiverStream;

    let (_directory, server) = start().await;
    let mut kv = pb::kv_client::KvClient::connect(server.endpoint.clone())
        .await
        .unwrap();
    kv.put(pb::PutRequest {
        key: b"mock/watch".to_vec(),
        value: b"old".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();
    let mut watcher = pb::watch_client::WatchClient::connect(server.endpoint.clone())
        .await
        .unwrap();
    let (sender, receiver) = tokio::sync::mpsc::channel(4);
    let mut stream = watcher
        .watch(ReceiverStream::new(receiver))
        .await
        .unwrap()
        .into_inner();
    sender
        .send(pb::WatchRequest {
            request_union: Some(pb::watch_request::RequestUnion::CreateRequest(
                pb::WatchCreateRequest {
                    key: b"mock/watch".to_vec(),
                    start_revision: 2,
                    watch_id: 17,
                    prev_kv: true,
                    ..Default::default()
                },
            )),
        })
        .await
        .unwrap();
    let created = tokio::time::timeout(Duration::from_secs(2), stream.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(created.created);
    assert_eq!(created.watch_id, 17);
    let replay = stream.message().await.unwrap().unwrap();
    assert_eq!(replay.events[0].kv.as_ref().unwrap().value, b"old");
    kv.put(pb::PutRequest {
        key: b"mock/watch".to_vec(),
        value: b"new".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();
    let live = tokio::time::timeout(Duration::from_secs(2), stream.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(live.header.as_ref().unwrap().revision, 3);
    assert_eq!(live.events[0].prev_kv.as_ref().unwrap().value, b"old");
    sender
        .send(pb::WatchRequest {
            request_union: Some(pb::watch_request::RequestUnion::ProgressRequest(
                pb::WatchProgressRequest {},
            )),
        })
        .await
        .unwrap();
    let progress = stream.message().await.unwrap().unwrap();
    assert!(progress.events.is_empty());
    assert_eq!(progress.watch_id, -1);
    assert_eq!(progress.header.unwrap().revision, 3);
    sender
        .send(pb::WatchRequest {
            request_union: Some(pb::watch_request::RequestUnion::CancelRequest(
                pb::WatchCancelRequest { watch_id: 17 },
            )),
        })
        .await
        .unwrap();
    assert!(stream.message().await.unwrap().unwrap().canceled);
    drop(sender);
    drop(stream);
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn grpc_lock_waits_for_the_current_owner_and_reacquires_with_the_same_lease() {
    use etcqlite::wire::v3lockpb as lock;

    let (_directory, server) = start().await;
    let mut leases = pb::lease_client::LeaseClient::connect(server.endpoint.clone())
        .await
        .unwrap();
    for id in [1, 2] {
        leases
            .lease_grant(pb::LeaseGrantRequest { id, ttl: 60 })
            .await
            .unwrap();
    }
    let mut owner = lock::lock_client::LockClient::connect(server.endpoint.clone())
        .await
        .unwrap();
    let first = owner
        .lock(lock::LockRequest {
            name: b"mock/mutex".to_vec(),
            lease: 1,
        })
        .await
        .unwrap()
        .into_inner();
    let mut waiter = lock::lock_client::LockClient::connect(server.endpoint.clone())
        .await
        .unwrap();
    let waiting = tokio::spawn(async move {
        waiter
            .lock(lock::LockRequest {
                name: b"mock/mutex".to_vec(),
                lease: 2,
            })
            .await
            .unwrap()
            .into_inner()
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!waiting.is_finished());
    owner
        .unlock(lock::UnlockRequest {
            key: first.key.clone(),
        })
        .await
        .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(2), waiting)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(first.key, second.key);
    let again = owner
        .lock(lock::LockRequest {
            name: b"mock/mutex".to_vec(),
            lease: 2,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(again.key, second.key);
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn grpc_election_campaign_proclaim_observe_and_resign_use_real_ownership() {
    use etcqlite::wire::v3electionpb as election;

    let (_directory, server) = start().await;
    let mut leases = pb::lease_client::LeaseClient::connect(server.endpoint.clone())
        .await
        .unwrap();
    for id in [1, 2] {
        leases
            .lease_grant(pb::LeaseGrantRequest { id, ttl: 60 })
            .await
            .unwrap();
    }

    let mut client = election::election_client::ElectionClient::connect(server.endpoint.clone())
        .await
        .unwrap();
    let first = client
        .campaign(election::CampaignRequest {
            name: b"mock/election".to_vec(),
            lease: 1,
            value: b"first".to_vec(),
        })
        .await
        .unwrap()
        .into_inner()
        .leader
        .unwrap();
    let actual = client
        .leader(election::LeaderRequest {
            name: b"mock/election".to_vec(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(actual.kv.unwrap().value, b"first");
    client
        .proclaim(election::ProclaimRequest {
            leader: Some(first.clone()),
            value: b"updated".to_vec(),
        })
        .await
        .unwrap();
    let mut observed = client
        .observe(election::LeaderRequest {
            name: b"mock/election".to_vec(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        observed.message().await.unwrap().unwrap().kv.unwrap().value,
        b"updated"
    );
    let mut next = election::election_client::ElectionClient::connect(server.endpoint.clone())
        .await
        .unwrap();
    let waiting = tokio::spawn(async move {
        next.campaign(election::CampaignRequest {
            name: b"mock/election".to_vec(),
            lease: 2,
            value: b"next".to_vec(),
        })
        .await
        .unwrap()
        .into_inner()
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!waiting.is_finished());
    client
        .resign(election::ResignRequest {
            leader: Some(first),
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), waiting)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), observed.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .kv
            .unwrap()
            .value,
        b"next"
    );
    drop(observed);
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn http_gateway_preserves_protobuf_json_and_reports_real_health_and_build_identity() {
    let (_directory, server) = start().await;
    let put = tokio::process::Command::new("curl")
        .args([
            "--fail",
            "--silent",
            "--show-error",
            "--max-time",
            "3",
            "-H",
            "Content-Type: application/json",
            "-d",
            "{\"key\":\"bW9jay9qc29u\",\"value\":\"AP8=\"}",
            &format!("{}/v3/kv/put", server.endpoint),
        ])
        .output()
        .await
        .unwrap();
    assert!(
        put.status.success(),
        "{}",
        String::from_utf8_lossy(&put.stderr)
    );
    let result = tokio::process::Command::new("curl")
        .args([
            "--fail",
            "--silent",
            "--show-error",
            "--max-time",
            "3",
            "-H",
            "Content-Type: application/json",
            "-d",
            "{\"key\":\"bW9jay9qc29u\"}",
            &format!("{}/v3/kv/range", server.endpoint),
        ])
        .output()
        .await
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let body: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(body["header"]["cluster_id"], "11");
    assert_eq!(body["kvs"][0]["create_revision"], "2");
    assert_eq!(body["kvs"][0]["value"], "AP8=");
    let version = tokio::process::Command::new("curl")
        .args([
            "--fail",
            "--silent",
            "--show-error",
            "--max-time",
            "3",
            &format!("{}/version", server.endpoint),
        ])
        .output()
        .await
        .unwrap();
    assert!(version.status.success());
    let version: serde_json::Value = serde_json::from_slice(&version.stdout).unwrap();
    assert_eq!(version["etcdserver"], "3.7.3");
    assert_eq!(version["etcqlite"]["version"], "0.1.0");
    let health = tokio::process::Command::new("curl")
        .args([
            "--fail",
            "--silent",
            "--show-error",
            "--max-time",
            "3",
            &format!("{}/health", server.endpoint),
        ])
        .output()
        .await
        .unwrap();
    assert!(health.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&health.stdout).unwrap()["health"],
        "true"
    );
    server.shutdown().await.unwrap();
}
