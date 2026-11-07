use etcqlite::protocol::SqliteStorage;
use omnipaxos::{
    ClusterConfig, OmniPaxos, OmniPaxosConfig, ServerConfig,
    messages::Message,
    storage::{Storage, StorageOp},
    util::{LogEntry, NodeId},
};
use std::collections::{BTreeMap, BTreeSet};
use support::{Record, Records};
use tempfile::TempDir;

mod support;

type Replica<B = SqliteStorage<Record>> = OmniPaxos<Record, B>;

trait StorageFactory: Storage<Record> {
    fn open(path: &std::path::Path, config: u32) -> Self;
}

impl StorageFactory for SqliteStorage<Record> {
    fn open(path: &std::path::Path, config: u32) -> Self {
        SqliteStorage::open(path, config).unwrap()
    }
}

impl StorageFactory for omnipaxos_storage::memory_storage::MemoryStorage<Record> {
    fn open(_path: &std::path::Path, _config: u32) -> Self {
        Self::default()
    }
}

struct Network<B: StorageFactory = SqliteStorage<Record>> {
    directory: TempDir,
    config: ClusterConfig,
    nodes: BTreeMap<NodeId, Replica<B>>,
    disconnected: BTreeSet<NodeId>,
    seed: u64,
    lossy: bool,
}

impl Network {
    fn new() -> Self {
        Self::with_storage()
    }
}

impl<B: StorageFactory> Network<B> {
    fn with_storage() -> Self {
        let mut network = Self {
            directory: TempDir::new().unwrap(),
            config: ClusterConfig {
                configuration_id: 1,
                nodes: vec![1, 2, 3],
                flexible_quorum: None,
            },
            nodes: BTreeMap::new(),
            disconnected: BTreeSet::new(),
            seed: 42,
            lossy: false,
        };
        for pid in 1..=3 {
            network.restart(pid);
        }
        network.run(20);
        network
    }

    fn restart(&mut self, pid: NodeId) {
        self.nodes.remove(&pid);
        let storage = B::open(
            &self.directory.path().join(format!("node-{pid}.db")),
            self.config.configuration_id,
        );
        let node = OmniPaxosConfig {
            cluster_config: self.config.clone(),
            server_config: ServerConfig {
                pid,
                election_tick_timeout: 5,
                resend_message_tick_timeout: 5,
                ..ServerConfig::default()
            },
        }
        .build(storage)
        .unwrap();
        self.nodes.insert(pid, node);
    }

    fn random(&mut self) -> u64 {
        self.seed ^= self.seed << 13;
        self.seed ^= self.seed >> 7;
        self.seed ^= self.seed << 17;
        self.seed
    }

    fn run(&mut self, ticks: usize) {
        for _ in 0..ticks {
            for node in self.nodes.values_mut() {
                node.tick();
            }
            for _ in 0..30 {
                let mut outgoing = Vec::new();
                for node in self.nodes.values_mut() {
                    node.take_outgoing_messages(&mut outgoing);
                }
                if outgoing.is_empty() {
                    break;
                }
                if self.random().is_multiple_of(2) {
                    outgoing.reverse();
                }
                for message in outgoing {
                    let sender = message.get_sender();
                    let receiver = message.get_receiver();
                    if self.disconnected.contains(&sender)
                        || self.disconnected.contains(&receiver)
                        || (self.lossy && self.random().is_multiple_of(7))
                    {
                        continue;
                    }
                    let duplicate = self.lossy && self.random().is_multiple_of(5);
                    if let Some(node) = self.nodes.get_mut(&receiver) {
                        if duplicate {
                            node.handle_incoming(message.clone());
                        }
                        node.handle_incoming(message);
                    }
                }
            }
        }
    }

    fn leader(&self) -> NodeId {
        self.nodes
            .iter()
            .find(|(pid, node)| node.get_current_leader() == Some((**pid, true)))
            .map(|(pid, _)| *pid)
            .expect("a leader must be in the accepted phase")
    }

    fn propose(&mut self, pid: NodeId, value: u8) {
        self.nodes
            .get_mut(&pid)
            .unwrap()
            .append(Record(vec![value]))
            .unwrap();
    }

    fn records(&self, pid: NodeId) -> Vec<Record> {
        self.nodes[&pid]
            .read_decided_suffix(0)
            .unwrap_or_default()
            .into_iter()
            .flat_map(|entry| match entry {
                LogEntry::Decided(record) => vec![record],
                LogEntry::Snapshotted(snapshot) => snapshot.snapshot.0,
                LogEntry::StopSign(_, true) => Vec::new(),
                other => panic!("unexpected decided log entry: {other:?}"),
            })
            .collect()
    }

    fn assert_equal(&self, expected: &[u8]) {
        let expected: Vec<_> = expected.iter().map(|value| Record(vec![*value])).collect();
        for &pid in self.nodes.keys() {
            assert_eq!(self.records(pid), expected, "replica {pid}");
        }
    }

    fn state(&self) -> String {
        self.nodes
            .iter()
            .map(|(pid, node)| {
                format!(
                    "node {pid}: promise {:?}, leader {:?}, accepted {}, decided {}, records {:?}",
                    node.get_promise(),
                    node.get_current_leader(),
                    node.get_accepted_idx(),
                    node.get_decided_idx(),
                    self.records(*pid),
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn check_decided_prefixes(&self, acknowledged: &mut Vec<Record>) {
        for &pid in self.nodes.keys() {
            let records = self.records(pid);
            let common = records.len().min(acknowledged.len());
            assert_eq!(
                records[..common],
                acknowledged[..common],
                "conflicting decided prefix\n{}",
                self.state()
            );
            if records.len() > acknowledged.len() {
                *acknowledged = records;
            }
        }
    }
}

#[test]
fn old_leader_cannot_decide_mutations_or_read_barriers_after_partition() {
    partition_safety::<SqliteStorage<Record>>();
}

#[test]
fn upstream_memory_backend_partition_safety() {
    partition_safety::<omnipaxos_storage::memory_storage::MemoryStorage<Record>>();
}

fn partition_safety<B: StorageFactory>() {
    let mut network = Network::<B>::with_storage();
    network.propose(1, 10);
    network.run(10);
    let old_leader = network.leader();
    let old_decided = network.nodes[&old_leader].get_decided_idx();
    network.disconnected.insert(old_leader);
    network.propose(old_leader, 20);
    network.propose(old_leader, 30);
    network.run(30);
    assert_eq!(network.nodes[&old_leader].get_decided_idx(), old_decided);
    assert_eq!(network.records(old_leader), vec![Record(vec![10])]);
    let majority_node = (1..=3).find(|pid| *pid != old_leader).unwrap();
    network.propose(majority_node, 40);
    network.run(30);
    assert!(network.records(majority_node).contains(&Record(vec![40])));
    network.disconnected.clear();
    network.run(30);
    let decided = network.records(majority_node);
    for &pid in network.nodes.keys() {
        assert_eq!(network.records(pid), decided, "{}", network.state());
    }
}

#[test]
fn one_replica_restart_preserves_acknowledged_entries() {
    let mut network = Network::new();
    for value in 1..=4 {
        network.propose(1, value);
    }
    network.run(20);
    network.assert_equal(&[1, 2, 3, 4]);
    let leader = network.leader();
    network.restart(leader);
    network.run(30);
    network.propose(2, 5);
    network.run(20);
    network.assert_equal(&[1, 2, 3, 4, 5]);
}

#[test]
fn manual_leadership_attempt_completes_only_after_target_decides_a_barrier() {
    let mut network = Network::new();
    network.propose(1, 1);
    network.run(10);
    let target = (1..=3).find(|pid| *pid != network.leader()).unwrap();
    network.nodes.get_mut(&target).unwrap().try_become_leader();
    network.run(20);
    assert_eq!(network.leader(), target);
    network.propose(target, 2);
    network.run(10);
    assert_eq!(
        network.nodes[&target].get_current_leader(),
        Some((target, true))
    );
    network.assert_equal(&[1, 2]);
}

#[test]
fn stale_replica_recovers_from_a_durable_snapshot_and_suffix() {
    let mut network = Network::new();
    network.propose(1, 1);
    network.run(10);
    let leader = network.leader();
    let lagging = (1..=3).find(|pid| *pid != leader).unwrap();
    network.disconnected.insert(lagging);
    for value in 2..=5 {
        network.propose(leader, value);
    }
    network.run(20);
    network
        .nodes
        .get_mut(&leader)
        .unwrap()
        .snapshot(None, true)
        .unwrap();
    network.propose(leader, 6);
    network.run(10);
    network.restart(lagging);
    network.disconnected.clear();
    network.run(50);
    network.assert_equal(&[1, 2, 3, 4, 5, 6]);
}

#[test]
fn non_voting_catchup_then_committed_reconfiguration_preserves_the_prefix() {
    let mut network = Network::new();
    for value in 1..=3 {
        network.propose(1, value);
    }
    network.run(20);
    let leader = network.leader();
    let prefix = network.records(leader);
    let decided = network.nodes[&leader].get_decided_idx();
    let learner_path = network.directory.path().join("node-4.db");
    let mut learner = SqliteStorage::<Record>::open(&learner_path, 1).unwrap();
    learner
        .write_atomically(vec![
            StorageOp::Trim(decided),
            StorageOp::SetCompactedIdx(decided),
            StorageOp::SetSnapshot(Some(Records(prefix.clone()))),
            StorageOp::SetDecidedIndex(decided),
        ])
        .unwrap();
    assert!(!network.config.nodes.contains(&4));
    assert!(!network.nodes.contains_key(&4));
    let next = ClusterConfig {
        configuration_id: 2,
        nodes: vec![2, 3, 4],
        flexible_quorum: None,
    };
    network
        .nodes
        .get_mut(&leader)
        .unwrap()
        .reconfigure(next.clone(), None)
        .unwrap();
    network.run(20);
    let stopped: Vec<_> = network
        .nodes
        .values()
        .filter_map(Replica::is_reconfigured)
        .collect();
    assert!(stopped.len() >= 2);
    for stopsign in &stopped {
        assert_eq!(stopsign.next_config, next);
    }
    for node in network.nodes.values_mut() {
        assert!(node.append(Record(vec![99])).is_err());
    }
    learner.set_stopsign(Some(stopped[0].clone())).unwrap();
    learner.set_decided_idx(decided + 1).unwrap();
    drop(learner);
    let recovered_learner = SqliteStorage::<Record>::open(&learner_path, 1).unwrap();
    assert_eq!(recovered_learner.get_snapshot().unwrap().unwrap().0, prefix);
    assert_eq!(
        recovered_learner.get_stopsign().unwrap().unwrap(),
        stopped[0]
    );
    drop(recovered_learner);
    network.nodes.clear();
    network.config = next;
    for pid in [2, 3, 4] {
        network.restart(pid);
    }
    network.run(30);
    network.propose(4, 4);
    network.run(20);
    for pid in [2, 3, 4] {
        let mut application_history = prefix.clone();
        application_history.extend(network.records(pid));
        assert_eq!(
            application_history,
            (1..=4).map(|n| Record(vec![n])).collect::<Vec<_>>()
        );
    }
}

#[test]
fn stopped_configuration_can_resynchronize_without_treating_stopsign_as_a_data_entry() {
    let mut network = Network::new();
    network.propose(1, 1);
    network.run(20);
    let leader = network.leader();
    network
        .nodes
        .get_mut(&leader)
        .unwrap()
        .reconfigure(
            ClusterConfig {
                configuration_id: 2,
                nodes: vec![1, 2, 3],
                flexible_quorum: None,
            },
            None,
        )
        .unwrap();
    network.run(20);
    assert!(network.nodes[&leader].is_reconfigured().is_some());
    network.restart(leader);
    network.run(50);
    for node in network.nodes.values() {
        assert_eq!(node.get_decided_idx(), 2);
        assert!(node.is_reconfigured().is_some());
    }
    network.assert_equal(&[1]);
}

#[test]
fn duplicate_reordered_and_lost_messages_do_not_diverge_decided_history() {
    fault_history_safety::<SqliteStorage<Record>>();
}

#[test]
fn upstream_memory_backend_fault_history_safety() {
    fault_history_safety::<omnipaxos_storage::memory_storage::MemoryStorage<Record>>();
}

fn fault_history_safety<B: StorageFactory>() {
    for seed in 1..=8 {
        let mut network = Network::<B>::with_storage();
        network.seed = seed;
        network.lossy = true;
        let mut acknowledged = Vec::new();
        for value in 1..=12 {
            let pid = u64::from(value % 3 + 1);
            network.propose(pid, value);
            network.run(3);
            network.check_decided_prefixes(&mut acknowledged);
        }
        network.run(60);
        network.check_decided_prefixes(&mut acknowledged);
        network.lossy = false;
        network.run(30);
        network.check_decided_prefixes(&mut acknowledged);
        // append() admits a proposal; only observing its decided entry acknowledges it.
        for value in 1..=12 {
            if !acknowledged.contains(&Record(vec![value])) {
                network.propose(network.leader(), value);
            }
        }
        network.run(30);
        network.check_decided_prefixes(&mut acknowledged);
        let expected = network.records(network.leader());
        assert!(!expected.is_empty());
        for pid in [1, 2, 3] {
            assert_eq!(
                network.records(pid),
                expected,
                "seed {seed}, replica {pid}\n{}",
                network.state()
            );
        }
        for value in 1..=12 {
            assert!(
                expected.contains(&Record(vec![value])),
                "lost proposal in seed {seed}"
            );
        }
    }
}

#[test]
fn delta_snapshot_does_not_include_the_old_leaders_undecided_suffix() {
    use omnipaxos::{
        ballot_leader_election::Ballot,
        messages::sequence_paxos::{AcceptSync, PaxosMessage, PaxosMsg, Prepare},
        storage::SnapshotType,
        util::LogSync,
    };

    let directory = TempDir::new().unwrap();
    let mut storage = SqliteStorage::<Record>::open(directory.path().join("node.db"), 1).unwrap();
    let old_ballot = Ballot::with(1, 1, 0, 3);
    let new_ballot = Ballot::with(1, 2, 0, 2);
    storage
        .write_atomically(vec![
            StorageOp::SetPromise(old_ballot),
            StorageOp::SetAcceptedRound(old_ballot),
            StorageOp::AppendEntries(vec![Record(vec![1]), Record(vec![99])]),
            StorageOp::SetDecidedIndex(1),
        ])
        .unwrap();
    let mut node = OmniPaxosConfig {
        cluster_config: ClusterConfig {
            configuration_id: 1,
            nodes: vec![1, 2, 3],
            flexible_quorum: None,
        },
        server_config: ServerConfig {
            pid: 3,
            ..ServerConfig::default()
        },
    }
    .build(storage)
    .unwrap();
    node.handle_incoming(Message::SequencePaxos(PaxosMessage {
        from: 2,
        to: 3,
        msg: PaxosMsg::Prepare(Prepare {
            n: new_ballot,
            n_accepted: new_ballot,
            accepted_idx: 2,
            decided_idx: 2,
        }),
    }));
    node.handle_incoming(Message::SequencePaxos(PaxosMessage {
        from: 2,
        to: 3,
        msg: PaxosMsg::AcceptSync(AcceptSync {
            n: new_ballot,
            seq_num: Default::default(),
            decided_idx: 2,
            log_sync: LogSync {
                decided_snapshot: Some(SnapshotType::Delta(Records(vec![Record(vec![2])]))),
                suffix: Vec::new(),
                sync_idx: 2,
                stopsign: None,
            },
        }),
    }));
    let entries = node.read_decided_suffix(0).unwrap();
    match &entries[0] {
        LogEntry::Snapshotted(snapshot) => {
            assert_eq!(snapshot.trimmed_idx, 2);
            assert_eq!(snapshot.snapshot.0, vec![Record(vec![1]), Record(vec![2])]);
        }
        other => panic!("expected a decided snapshot, got {other:?}"),
    }
    assert_eq!(node.get_decided_idx(), 2);
}

#[test]
fn forwarded_compaction_storage_errors_poison_the_replica_even_when_library_ignores_them() {
    let mut network = Network::new();
    network.propose(1, 1);
    network.run(10);
    let follower = (1..=3).find(|pid| *pid != network.leader()).unwrap();
    let path = network.directory.path().join(format!("node-{follower}.db"));
    network.nodes.remove(&follower);
    let storage = SqliteStorage::<Record>::open(&path, 1).unwrap();
    let healthy = storage.health_flag();
    let mut node = OmniPaxosConfig {
        cluster_config: network.config.clone(),
        server_config: ServerConfig {
            pid: follower,
            ..ServerConfig::default()
        },
    }
    .build(storage)
    .unwrap();
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER fail_compaction BEFORE DELETE ON protocol_entries
             BEGIN SELECT RAISE(ABORT, 'injected compaction failure'); END;",
        )
        .unwrap();
    let message = Message::SequencePaxos(omnipaxos::messages::sequence_paxos::PaxosMessage {
        from: network.leader(),
        to: follower,
        msg: omnipaxos::messages::sequence_paxos::PaxosMsg::Compaction(
            omnipaxos::messages::sequence_paxos::Compaction::Trim(1),
        ),
    });
    node.handle_incoming(message);
    assert!(!healthy.load(std::sync::atomic::Ordering::Acquire));
    drop(node);
    assert!(!network.nodes.contains_key(&follower));
}
