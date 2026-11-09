use etcqlite::{
    cluster::{Command, Configuration, Member},
    replica::{ELECTION_TIMEOUT_TICKS, Replica},
    storage::{Identity, KvRequest, Store, auth::Principal, command::Mutation},
    wire::etcdserverpb as pb,
};
use std::collections::{BTreeMap, BTreeSet};
use tempfile::TempDir;
use uuid::Uuid;

struct Cluster {
    directory: TempDir,
    nodes: BTreeMap<u64, (Replica, Store)>,
    isolated: BTreeSet<u64>,
}

impl Cluster {
    fn new() -> Self {
        let directory = TempDir::new().unwrap();
        let configuration = Configuration {
            cluster_id: 42,
            configuration_id: 1,
            bootstrap_digest: None,
            members: (1..=3)
                .map(|id| Member {
                    id,
                    pid: id,
                    name: format!("mock-{id}"),
                    learner: false,
                    client_url: format!("https://127.0.0.1:{}", 27000 + id),
                    peer_url: format!("https://127.0.0.1:{}", 28000 + id),
                    additional_peer_urls: Vec::new(),
                    incarnation: None,
                })
                .collect(),
        };
        let mut nodes = BTreeMap::new();
        for member in &configuration.members {
            let path = directory.path().join(format!("node-{}", member.id));
            let mut store = Store::create(
                &path,
                Identity {
                    cluster_id: 42,
                    member_id: member.id,
                    name: member.name.clone(),
                },
            )
            .unwrap();
            store.configure_cluster(&configuration).unwrap();
            let replica = Replica::open(&store).unwrap();
            nodes.insert(member.id, (replica, store));
        }
        let mut cluster = Self {
            directory,
            nodes,
            isolated: BTreeSet::new(),
        };
        cluster.run(3 * ELECTION_TIMEOUT_TICKS as usize);
        cluster
    }

    fn run(&mut self, ticks: usize) {
        self.run_skipping_apply(ticks, None);
    }

    fn run_skipping_apply(&mut self, ticks: usize, skipped: Option<u64>) {
        for _ in 0..ticks {
            for (replica, store) in self.nodes.values_mut() {
                replica.tick(store).unwrap();
            }
            for _ in 0..30 {
                let mut messages = Vec::new();
                for (id, (replica, _)) in &mut self.nodes {
                    messages.extend(
                        replica
                            .outgoing()
                            .unwrap()
                            .into_iter()
                            .map(|message| (*id, message)),
                    );
                }
                if messages.is_empty() {
                    break;
                }
                messages.reverse();
                for (sender, message) in messages {
                    if !self.isolated.contains(&sender)
                        && !self.isolated.contains(&message.receiver)
                    {
                        self.nodes
                            .get_mut(&message.receiver)
                            .unwrap()
                            .0
                            .receive(
                                message.configuration.configuration_id,
                                sender,
                                message.message,
                            )
                            .unwrap();
                    }
                }
            }
            for (id, (replica, store)) in &mut self.nodes {
                if Some(*id) != skipped {
                    replica.apply(store).unwrap();
                }
            }
        }
    }

    fn put(&mut self, node: u64, key: &[u8], value: &[u8]) {
        self.nodes
            .get_mut(&node)
            .unwrap()
            .0
            .propose(Command::Mutation {
                id: Uuid::new_v4(),
                principal: Principal::Anonymous,
                request: Mutation::Kv(KvRequest::Put(pb::PutRequest {
                    key: key.to_vec(),
                    value: value.to_vec(),
                    ..Default::default()
                })),
            })
            .unwrap();
    }
}

#[test]
fn native_restore_replays_durable_decisions_not_yet_applied_at_snapshot_time() {
    let mut cluster = Cluster::new();
    cluster.put(1, b"mock/unapplied", b"decided");
    cluster.run_skipping_apply(20, Some(1));
    assert_eq!(cluster.nodes[&1].1.revision(), 1);
    assert_eq!(cluster.nodes[&2].1.revision(), 2);
    let artifact = cluster.nodes[&1]
        .1
        .native_snapshot(&Principal::Internal)
        .unwrap();
    let restored = cluster.directory.path().join("restored");
    let result = etcqlite::storage::snapshot::restore(
        artifact.path(),
        &restored,
        Identity {
            cluster_id: 99,
            member_id: 88,
            name: "mock-restored".into(),
        },
        None,
        0,
        false,
    )
    .unwrap();
    assert_eq!(result.revision, 2);
    let store = Store::open(restored).unwrap();
    assert_eq!(
        store
            .range(&pb::RangeRequest {
                key: b"mock/unapplied".to_vec(),
                ..Default::default()
            })
            .unwrap()
            .kvs[0]
            .value,
        b"decided"
    );
}

#[test]
fn real_sqlite_state_application_follows_only_decided_non_raft_commands() {
    let mut cluster = Cluster::new();
    cluster.put(1, b"mock/shared", &[0, 255]);
    cluster.run(20);
    for (_, store) in cluster.nodes.values() {
        let result = store
            .range(&pb::RangeRequest {
                key: b"mock/shared".to_vec(),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(result.kvs[0].value, vec![0, 255]);
        assert_eq!(store.revision(), 2);
    }
    let leader = cluster
        .nodes
        .iter()
        .find(|(_, (replica, _))| replica.is_leader())
        .map(|(id, _)| *id)
        .unwrap();
    cluster.isolated.insert(leader);
    cluster.put(leader, b"mock/unsafe", b"must-not-apply");
    cluster
        .nodes
        .get_mut(&leader)
        .unwrap()
        .0
        .propose(Command::Barrier { id: Uuid::new_v4() })
        .unwrap();
    cluster.run(3 * ELECTION_TIMEOUT_TICKS as usize);
    assert!(
        cluster.nodes[&leader]
            .1
            .range(&pb::RangeRequest {
                key: b"mock/unsafe".to_vec(),
                ..Default::default()
            })
            .unwrap()
            .kvs
            .is_empty()
    );
    let majority = (1..=3).find(|id| *id != leader).unwrap();
    cluster.put(majority, b"mock/majority", b"committed");
    cluster.run(30);
    assert_eq!(cluster.nodes[&majority].1.revision(), 3);
    cluster.isolated.clear();
    cluster.run(3 * ELECTION_TIMEOUT_TICKS as usize);
    let expected = cluster.nodes[&majority]
        .1
        .range(&pb::RangeRequest {
            key: b"mock/".to_vec(),
            range_end: vec![0],
            ..Default::default()
        })
        .unwrap();
    for (_, store) in cluster.nodes.values() {
        assert_eq!(
            store
                .range(&pb::RangeRequest {
                    key: b"mock/".to_vec(),
                    range_end: vec![0],
                    ..Default::default()
                })
                .unwrap(),
            expected
        );
        let connection = rusqlite::Connection::open_with_flags(
            store.path(),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        let compacted: i64 = connection
            .query_row(
                "SELECT compacted FROM protocol_state WHERE configuration_id=1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            compacted, 0,
            "retained command logs must resynchronize without full-history snapshot blobs"
        );
    }
}

#[test]
fn replica_restart_replays_durable_state_without_repeating_acknowledged_effects() {
    let mut cluster = Cluster::new();
    cluster.put(1, b"mock/restart", b"first");
    cluster.run(20);
    cluster.nodes.remove(&1);
    let path = cluster.directory.path().join("node-1");
    let store = Store::open(path).unwrap();
    let replica = Replica::open(&store).unwrap();
    cluster.nodes.insert(1, (replica, store));
    cluster.run(40);
    assert_eq!(
        cluster.nodes[&1]
            .1
            .range(&pb::RangeRequest {
                key: b"mock/restart".to_vec(),
                ..Default::default()
            })
            .unwrap()
            .kvs[0]
            .version,
        1
    );
}
