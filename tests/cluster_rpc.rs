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
        let clients: Vec<_> = (0..3).map(|_| address()).collect();
        let peers: Vec<_> = (0..3).map(|_| address()).collect();
        let configuration = Configuration {
            cluster_id: 42,
            configuration_id: 1,
            members: (0..3)
                .map(|index| Member {
                    id: index as u64 + 1,
                    pid: index as u64 + 1,
                    name: format!("mock-{}", index + 1),
                    client_url: format!("https://{}", clients[index]),
                    peer_url: format!("https://{}", peers[index]),
                    learner: false,
                })
                .collect(),
        };
        let mut servers = Vec::new();
        for index in 0..3 {
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

    async fn client(&self, index: usize) -> pb::kv_client::KvClient<tonic::transport::Channel> {
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
        pb::kv_client::KvClient::new(endpoint.connect().await.unwrap())
    }

    async fn shutdown(self) {
        for server in self.servers {
            server.shutdown().await.unwrap();
        }
    }
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
