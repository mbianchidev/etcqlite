use etcqlite::{
    cluster::{Configuration, Member},
    peer,
    server::{self, Options, RunningServer, TlsOptions},
    storage::{Identity, Store},
    wire::etcdserverpb as pb,
};
use std::{
    net::{SocketAddr, TcpListener},
    path::PathBuf,
    time::Duration,
};
use tempfile::TempDir;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity as TlsIdentity};

fn address() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

struct Cluster {
    _directory: TempDir,
    pki: PathBuf,
    servers: Vec<RunningServer>,
}

impl Cluster {
    async fn new() -> Self {
        Self::with_learner(false).await
    }

    async fn with_learner(learner: bool) -> Self {
        let count = if learner { 4 } else { 3 };
        let directory = TempDir::new().unwrap();
        let pki = directory.path().join("pki");
        let result = tokio::process::Command::new("go")
            .args([
                "run",
                "./certs",
                "--out",
                pki.to_str().unwrap(),
                "--cluster-id",
                "42",
                "--members",
                "1,2,3,4",
            ])
            .current_dir(format!("{}/tools", env!("CARGO_MANIFEST_DIR")))
            .output()
            .await
            .unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let clients: Vec<_> = (0..count).map(|_| address()).collect();
        let peers: Vec<_> = (0..count).map(|_| address()).collect();
        let configuration = Configuration {
            cluster_id: 42,
            configuration_id: 1,
            bootstrap_digest: None,
            members: (0..count)
                .map(|index| Member {
                    id: index as u64 + 1,
                    pid: index as u64 + 1,
                    name: format!("mock-{}", index + 1),
                    client_url: format!("https://{}", clients[index]),
                    peer_url: format!("https://{}", peers[index]),
                    additional_peer_urls: Vec::new(),
                    incarnation: None,
                    learner: index == 3,
                })
                .collect(),
        };
        let mut servers = Vec::new();
        for index in 0..count {
            let id = index as u64 + 1;
            let path = directory.path().join(format!("node-{id}"));
            let mut store = Store::create(
                &path,
                Identity {
                    cluster_id: 42,
                    member_id: id,
                    name: format!("mock-{id}"),
                },
            )
            .unwrap();
            store.configure_cluster(&configuration).unwrap();
            drop(store);
            let certificate = pki.join(format!("member-{id}.crt"));
            let key = pki.join(format!("member-{id}.key"));
            let ca = pki.join("ca.crt");
            servers.push(
                server::start(Options {
                    data_dir: path,
                    address: clients[index],
                    request_timeout: Duration::from_secs(5),
                    snapshot_tool: None,
                    tls: Some(TlsOptions {
                        certificate: certificate.clone(),
                        key: key.clone(),
                        client_ca: Some(ca.clone()),
                    }),
                    peer: Some(peer::Options {
                        address: peers[index],
                        certificate,
                        key,
                        ca,
                    }),
                })
                .await
                .unwrap(),
            );
        }
        Self {
            _directory: directory,
            pki,
            servers,
        }
    }

    async fn channel(&self, index: usize) -> tonic::transport::Channel {
        let endpoint = Endpoint::from_shared(self.servers[index].endpoint.clone())
            .unwrap()
            .tls_config(
                ClientTlsConfig::new()
                    .ca_certificate(Certificate::from_pem(
                        std::fs::read(self.pki.join("ca.crt")).unwrap(),
                    ))
                    .identity(TlsIdentity::from_pem(
                        std::fs::read(self.pki.join("member-1.crt")).unwrap(),
                        std::fs::read(self.pki.join("member-1.key")).unwrap(),
                    )),
            )
            .unwrap();
        endpoint.connect().await.unwrap()
    }

    async fn client(&self, index: usize) -> pb::kv_client::KvClient<tonic::transport::Channel> {
        pb::kv_client::KvClient::new(self.channel(index).await)
    }

    async fn shutdown(self) {
        for server in self.servers {
            server.shutdown().await.unwrap();
        }
    }
}

#[tokio::test]
async fn leadership_transfer_requires_the_target_to_decide_in_its_new_term() {
    let cluster = Cluster::new().await;
    cluster
        .client(0)
        .await
        .put(pb::PutRequest {
            key: b"mock/transfer".to_vec(),
            value: b"acknowledged".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let (target, response) = loop {
        let mut first = pb::maintenance_client::MaintenanceClient::new(cluster.channel(0).await);
        let status = first
            .status(pb::StatusRequest {})
            .await
            .unwrap()
            .into_inner();
        let leader = usize::try_from(status.leader - 1).unwrap();
        let target = (leader + 1) % 3;
        let mut current =
            pb::maintenance_client::MaintenanceClient::new(cluster.channel(leader).await);
        match current
            .move_leader(pb::MoveLeaderRequest {
                target_id: target as u64 + 1,
            })
            .await
        {
            Ok(response) => break (target, response.into_inner()),
            Err(error)
                if error.code() == tonic::Code::FailedPrecondition
                    && error.message() == "etcdserver: not leader"
                    && tokio::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(error) => panic!("leadership transfer failed: {error}"),
        }
    };
    let mut moved = pb::maintenance_client::MaintenanceClient::new(cluster.channel(target).await);
    let status = moved
        .status(pb::StatusRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(status.leader, target as u64 + 1);
    assert_eq!(status.raft_term, response.header.unwrap().raft_term);
    assert_eq!(status.raft_applied_index, status.raft_index);
    let response = cluster
        .client(target)
        .await
        .range(pb::RangeRequest {
            key: b"mock/transfer".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.kvs[0].value, b"acknowledged");
    cluster.shutdown().await;
}

#[tokio::test]
async fn learner_membership_is_a_committed_configuration_not_a_static_list() {
    let cluster = Cluster::new().await;
    let mut members = pb::cluster_client::ClusterClient::new(cluster.channel(0).await);
    let added = members
        .member_add(pb::MemberAddRequest {
            peer_ur_ls: vec![format!("https://{}", address())],
            is_learner: true,
        })
        .await
        .unwrap()
        .into_inner();
    let id = added.member.unwrap().id;
    assert!(id != 0 && added.members.len() == 4);
    assert!(
        added
            .members
            .iter()
            .find(|member| member.id == id)
            .unwrap()
            .is_learner
    );
    let listed = members
        .member_list(pb::MemberListRequest { linearizable: true })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listed.members, added.members);
    let mut kv = cluster.client(1).await;
    kv.put(pb::PutRequest {
        key: b"mock/after-add".to_vec(),
        value: b"safe".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();
    assert_eq!(
        members
            .member_promote(pb::MemberPromoteRequest { id })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    let removed = members
        .member_remove(pb::MemberRemoveRequest { id })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(removed.members.len(), 3);
    let read = kv
        .range(pb::RangeRequest {
            key: b"mock/after-add".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(read.kvs[0].value, b"safe");
    cluster.shutdown().await;
}

#[tokio::test]
async fn a_real_non_voting_learner_catches_up_forwards_requests_and_promotes_safely() {
    let cluster = Cluster::with_learner(true).await;
    let mut voter = cluster.client(0).await;
    voter
        .put(pb::PutRequest {
            key: b"mock/learner".to_vec(),
            value: vec![0, 255],
            ..Default::default()
        })
        .await
        .unwrap();
    let mut learner = cluster.client(3).await;
    let read = learner
        .range(pb::RangeRequest {
            key: b"mock/learner".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(read.kvs[0].value, vec![0, 255]);
    learner
        .put(pb::PutRequest {
            key: b"mock/from-learner".to_vec(),
            value: b"forwarded".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    let mut members = pb::cluster_client::ClusterClient::new(cluster.channel(0).await);
    let promoted = members
        .member_promote(pb::MemberPromoteRequest { id: 4 })
        .await
        .unwrap()
        .into_inner();
    assert!(
        !promoted
            .members
            .iter()
            .find(|member| member.id == 4)
            .unwrap()
            .is_learner
    );
    learner
        .range(pb::RangeRequest {
            key: b"mock/from-learner".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    let mut maintenance = pb::maintenance_client::MaintenanceClient::new(cluster.channel(3).await);
    assert!(
        !maintenance
            .status(pb::StatusRequest {})
            .await
            .unwrap()
            .into_inner()
            .is_learner
    );
    members
        .member_remove(pb::MemberRemoveRequest { id: 2 })
        .await
        .unwrap();
    learner
        .put(pb::PutRequest {
            key: b"mock/after-removal".to_vec(),
            value: b"new-quorum".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    cluster.shutdown().await;
}

#[tokio::test]
async fn added_member_bootstraps_from_verified_state_without_reusing_source_votes() {
    use etcqlite::storage::{
        auth::Principal, command::Mutation, membership::MembershipCommand, snapshot,
    };
    let mut cluster = Cluster::new().await;
    let client_address = address();
    let peer_address = address();
    cluster
        .client(0)
        .await
        .put(pb::PutRequest {
            key: b"mock/join".to_vec(),
            value: vec![0, 255],
            ..Default::default()
        })
        .await
        .unwrap();
    cluster.servers[0]
        .runtime
        .mutate(
            Principal::Anonymous,
            Mutation::Membership(MembershipCommand::Add {
                id: 4,
                peer_urls: vec![format!("https://{peer_address}")],
                learner: true,
            }),
        )
        .await
        .unwrap();
    let configuration = cluster.servers[0]
        .runtime
        .read(|_, state| Ok(state.cluster.clone().unwrap()))
        .await
        .unwrap();
    let peer = peer::Options {
        address: peer_address,
        certificate: cluster.pki.join("member-4.crt"),
        key: cluster.pki.join("member-4.key"),
        ca: cluster.pki.join("ca.crt"),
    };
    let artifact = peer::snapshot(configuration, 4, 1, &peer, cluster._directory.path())
        .await
        .unwrap();
    let path = cluster._directory.path().join("joined");
    snapshot::join(
        artifact.path(),
        &path,
        Identity {
            cluster_id: 42,
            member_id: 4,
            name: "mock-joined".into(),
        },
    )
    .unwrap();
    cluster.servers.push(
        server::start(Options {
            data_dir: path.clone(),
            address: client_address,
            request_timeout: Duration::from_secs(5),
            snapshot_tool: None,
            tls: Some(TlsOptions {
                certificate: peer.certificate.clone(),
                key: peer.key.clone(),
                client_ca: Some(peer.ca.clone()),
            }),
            peer: Some(peer),
        })
        .await
        .unwrap(),
    );
    let mut joined = cluster.client(3).await;
    let value = joined
        .range(pb::RangeRequest {
            key: b"mock/join".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(value.kvs[0].value, vec![0, 255]);
    joined
        .put(pb::PutRequest {
            key: b"mock/join-write".to_vec(),
            value: b"safe".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    let mut members = pb::cluster_client::ClusterClient::new(cluster.channel(0).await);
    let list = members
        .member_list(pb::MemberListRequest { linearizable: true })
        .await
        .unwrap()
        .into_inner();
    let member = list.members.iter().find(|member| member.id == 4).unwrap();
    assert_eq!(member.name, "mock-joined");
    assert_eq!(
        member.client_ur_ls,
        vec![format!("https://{client_address}")]
    );
    cluster.servers.pop().unwrap().shutdown().await.unwrap();
    let duplicate = cluster._directory.path().join("duplicate");
    snapshot::join(
        artifact.path(),
        &duplicate,
        Identity {
            cluster_id: 42,
            member_id: 4,
            name: "mock-duplicate".into(),
        },
    )
    .unwrap();
    let duplicate_peer = peer::Options {
        address: peer_address,
        certificate: cluster.pki.join("member-4.crt"),
        key: cluster.pki.join("member-4.key"),
        ca: cluster.pki.join("ca.crt"),
    };
    let error = server::start(Options {
        data_dir: duplicate,
        address: address(),
        request_timeout: Duration::from_millis(500),
        snapshot_tool: None,
        tls: Some(TlsOptions {
            certificate: duplicate_peer.certificate.clone(),
            key: duplicate_peer.key.clone(),
            client_ca: Some(duplicate_peer.ca.clone()),
        }),
        peer: Some(duplicate_peer),
    })
    .await
    .err()
    .unwrap();
    assert!(
        error
            .to_string()
            .contains("incarnation registration failed"),
        "{error}"
    );
    let restarted_peer = peer::Options {
        address: peer_address,
        certificate: cluster.pki.join("member-4.crt"),
        key: cluster.pki.join("member-4.key"),
        ca: cluster.pki.join("ca.crt"),
    };
    cluster.servers.push(
        server::start(Options {
            data_dir: path,
            address: client_address,
            request_timeout: Duration::from_secs(5),
            snapshot_tool: None,
            tls: Some(TlsOptions {
                certificate: restarted_peer.certificate.clone(),
                key: restarted_peer.key.clone(),
                client_ca: Some(restarted_peer.ca.clone()),
            }),
            peer: Some(restarted_peer),
        })
        .await
        .unwrap(),
    );
    joined = cluster.client(3).await;
    joined
        .put(pb::PutRequest {
            key: b"mock/after-duplicate".to_vec(),
            value: b"still-safe".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    members
        .member_promote(pb::MemberPromoteRequest { id: 4 })
        .await
        .unwrap();
    joined
        .range(pb::RangeRequest {
            key: b"mock/join-write".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    cluster.shutdown().await;
}

#[tokio::test]
async fn restored_voters_reject_a_different_checkpoint_under_the_same_cluster_id() {
    use etcqlite::storage::{KvRequest, auth::Principal, snapshot};
    use uuid::Uuid;
    let mut cluster = Cluster::new().await;
    let configuration = cluster.servers[0]
        .runtime
        .read(|_, state| Ok(state.cluster.clone().unwrap()))
        .await
        .unwrap();
    for server in cluster.servers.drain(..) {
        server.shutdown().await.unwrap();
    }
    let mut source = Store::create(
        cluster._directory.path().join("source"),
        Identity {
            cluster_id: 11,
            member_id: 22,
            name: "mock-source".into(),
        },
    )
    .unwrap();
    source
        .apply(
            1,
            Uuid::new_v4(),
            KvRequest::Put(pb::PutRequest {
                key: b"mock/checkpoint".to_vec(),
                value: b"accepted".to_vec(),
                ..Default::default()
            }),
        )
        .unwrap();
    let accepted = source.native_snapshot(&Principal::Internal).unwrap();
    source
        .apply(
            2,
            Uuid::new_v4(),
            KvRequest::Put(pb::PutRequest {
                key: b"mock/checkpoint".to_vec(),
                value: b"wrong-source".to_vec(),
                ..Default::default()
            }),
        )
        .unwrap();
    let different = source.native_snapshot(&Principal::Internal).unwrap();
    for (index, member) in configuration.members.iter().enumerate() {
        let path = cluster
            ._directory
            .path()
            .join(format!("restored-{}", member.id));
        snapshot::restore(
            if index == 2 {
                different.path()
            } else {
                accepted.path()
            },
            &path,
            Identity {
                cluster_id: 42,
                member_id: member.id,
                name: member.name.clone(),
            },
            Some(&configuration),
            0,
            false,
        )
        .unwrap();
        let certificate = cluster.pki.join(format!("member-{}.crt", member.id));
        let key = cluster.pki.join(format!("member-{}.key", member.id));
        let ca = cluster.pki.join("ca.crt");
        cluster.servers.push(
            server::start(Options {
                data_dir: path,
                address: member
                    .client_url
                    .trim_start_matches("https://")
                    .parse()
                    .unwrap(),
                request_timeout: Duration::from_secs(2),
                snapshot_tool: None,
                tls: Some(TlsOptions {
                    certificate: certificate.clone(),
                    key: key.clone(),
                    client_ca: Some(ca.clone()),
                }),
                peer: Some(peer::Options {
                    address: member
                        .peer_url
                        .trim_start_matches("https://")
                        .parse()
                        .unwrap(),
                    certificate,
                    key,
                    ca,
                }),
            })
            .await
            .unwrap(),
        );
    }
    let read = cluster
        .client(0)
        .await
        .range(pb::RangeRequest {
            key: b"mock/checkpoint".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(read.kvs[0].value, b"accepted");
    assert!(
        cluster
            .client(2)
            .await
            .range(pb::RangeRequest {
                key: b"mock/checkpoint".to_vec(),
                ..Default::default()
            })
            .await
            .is_err()
    );
    cluster.shutdown().await;
}

#[tokio::test]
async fn three_tls_voters_replicate_writes_and_fence_reads_after_quorum_loss() {
    let mut cluster = Cluster::new().await;
    let mut first = cluster.client(0).await;
    let response = first
        .put(pb::PutRequest {
            key: b"mock/quorum".to_vec(),
            value: vec![0, 255],
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.header.as_ref().unwrap().revision, 2);
    for index in 0..3 {
        let mut client = cluster.client(index).await;
        let response = client
            .range(pb::RangeRequest {
                key: b"mock/quorum".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.kvs[0].value, vec![0, 255]);
    }

    let last = cluster.servers.pop().unwrap();
    last.shutdown().await.unwrap();
    first
        .put(pb::PutRequest {
            key: b"mock/one-down".to_vec(),
            value: b"safe".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    let second = cluster.servers.pop().unwrap();
    second.shutdown().await.unwrap();
    let failure = first
        .range(pb::RangeRequest {
            key: b"mock/quorum".to_vec(),
            ..Default::default()
        })
        .await;
    assert!(failure.is_err());
    let stale = first
        .range(pb::RangeRequest {
            key: b"mock/quorum".to_vec(),
            serializable: true,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(stale.kvs[0].value, vec![0, 255]);
    cluster.shutdown().await;
}
