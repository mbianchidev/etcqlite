use etcqlite::protocol::SqliteStorage;
use omnipaxos::{
    ClusterConfig,
    ballot_leader_election::Ballot,
    storage::{StopSign, Storage, StorageOp},
};
use std::process::Command;
use support::{Record, Records};
use tempfile::TempDir;

mod support;

#[test]
fn durable_atomic_protocol_batch_survives_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("node.db");
    let ballot = Ballot::with(1, 7, 0, 2);
    let records = vec![Record(vec![0, 255, 1]), Record(vec![])];
    {
        let mut storage = SqliteStorage::<Record>::open(&path, 1).unwrap();
        storage
            .write_atomically(vec![
                StorageOp::SetPromise(ballot),
                StorageOp::SetAcceptedRound(ballot),
                StorageOp::AppendEntries(records.clone()),
                StorageOp::SetDecidedIndex(2),
            ])
            .unwrap();
    }
    let storage = SqliteStorage::<Record>::open(&path, 1).unwrap();
    assert_eq!(storage.get_promise().unwrap(), Some(ballot));
    assert_eq!(storage.get_accepted_round().unwrap(), Some(ballot));
    assert_eq!(storage.get_decided_idx().unwrap(), 2);
    assert_eq!(storage.get_entries(0, 2).unwrap(), records);
    assert!(storage.is_healthy());
}

#[test]
fn failed_batch_rolls_back_every_operation_and_poison_is_visible() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("node.db");
    let ballot = Ballot::with(1, 2, 0, 1);
    let mut storage = SqliteStorage::<Record>::open(&path, 1).unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER fail_entry BEFORE INSERT ON protocol_entries
             BEGIN SELECT RAISE(ABORT, 'injected disk write failure'); END;",
        )
        .unwrap();
    let error = storage
        .write_atomically(vec![
            StorageOp::SetPromise(ballot),
            StorageOp::AppendEntry(Record(vec![9])),
        ])
        .unwrap_err();
    assert!(error.to_string().contains("injected disk write failure"));
    assert!(!storage.is_healthy());
    drop(storage);
    connection
        .execute_batch("DROP TRIGGER fail_entry;")
        .unwrap();
    let reopened = SqliteStorage::<Record>::open(&path, 1).unwrap();
    assert_eq!(reopened.get_promise().unwrap(), None);
    assert_eq!(reopened.get_log_len().unwrap(), 0);
}

#[test]
fn decided_prefix_cannot_be_overwritten() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("node.db");
    let mut storage = SqliteStorage::<Record>::open(&path, 1).unwrap();
    storage.append_entry(Record(vec![1])).unwrap();
    storage.set_decided_idx(1).unwrap();
    assert!(storage.append_on_prefix(0, vec![Record(vec![2])]).is_err());
    drop(storage);
    let reopened = SqliteStorage::<Record>::open(&path, 1).unwrap();
    assert_eq!(reopened.get_entries(0, 1).unwrap(), vec![Record(vec![1])]);
}

#[test]
fn snapshot_trim_suffix_and_stopsign_recover_at_absolute_indexes() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("node.db");
    let snapshot = Records(vec![Record(vec![1]), Record(vec![2])]);
    let stopsign = StopSign::with(
        ClusterConfig {
            configuration_id: 2,
            nodes: vec![2, 3, 4],
            flexible_quorum: None,
        },
        Some(vec![0, 255]),
    );
    {
        let mut storage = SqliteStorage::<Record>::open(&path, 1).unwrap();
        storage.append_entries(snapshot.0.clone()).unwrap();
        storage.set_decided_idx(2).unwrap();
        storage
            .write_atomically(vec![
                StorageOp::Trim(2),
                StorageOp::SetCompactedIdx(2),
                StorageOp::SetSnapshot(Some(snapshot.clone())),
            ])
            .unwrap();
        storage.append_entry(Record(vec![3])).unwrap();
        storage.set_stopsign(Some(stopsign.clone())).unwrap();
        storage.set_decided_idx(4).unwrap();
    }
    let storage = SqliteStorage::<Record>::open(&path, 1).unwrap();
    assert_eq!(storage.get_compacted_idx().unwrap(), 2);
    assert_eq!(storage.get_log_len().unwrap(), 1);
    assert_eq!(storage.get_suffix(2).unwrap(), vec![Record(vec![3])]);
    assert_eq!(storage.get_snapshot().unwrap(), Some(snapshot));
    assert_eq!(storage.get_stopsign().unwrap(), Some(stopsign));
    assert_eq!(storage.get_decided_idx().unwrap(), 4);
    assert!(storage.get_entries(1, 3).unwrap().is_empty());
}

#[test]
fn index_and_ballot_boundaries_are_checked_without_partial_mutation() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("node.db");
    let mut storage = SqliteStorage::<Record>::open(&path, 1).unwrap();
    storage.set_promise(Ballot::with(1, 3, 0, 1)).unwrap();
    assert!(
        storage
            .write_atomically(vec![
                StorageOp::AppendEntry(Record(vec![1])),
                StorageOp::SetPromise(Ballot::with(1, 2, 0, 1)),
            ])
            .is_err()
    );
    drop(storage);
    let storage = SqliteStorage::<Record>::open(&path, 1).unwrap();
    assert_eq!(storage.get_log_len().unwrap(), 0);
    assert_eq!(storage.get_promise().unwrap().unwrap().n, 3);
}

#[test]
fn corrupted_entry_is_rejected_instead_of_returning_data() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("node.db");
    let mut storage = SqliteStorage::<Record>::open(&path, 1).unwrap();
    storage.append_entry(Record(vec![1])).unwrap();
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute("UPDATE protocol_entries SET payload = x'00';", [])
        .unwrap();
    assert!(storage.get_entries(0, 1).is_err());
    assert!(!storage.is_healthy());
}

#[test]
fn process_exit_before_destructors_keeps_acknowledged_protocol_state() {
    const CHILD_PATH: &str = "ETCQLITE_CRASH_PROBE";
    if let Some(path) = std::env::var_os(CHILD_PATH) {
        let mut storage = SqliteStorage::<Record>::open(path, 1).unwrap();
        storage
            .write_atomically(vec![
                StorageOp::SetPromise(Ballot::with(1, 5, 0, 3)),
                StorageOp::AppendEntry(Record(vec![0, 255])),
                StorageOp::SetDecidedIndex(1),
            ])
            .unwrap();
        std::process::exit(73);
    }
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("node.db");
    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "process_exit_before_destructors_keeps_acknowledged_protocol_state",
        ])
        .env(CHILD_PATH, &path)
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(73));
    let storage = SqliteStorage::<Record>::open(&path, 1).unwrap();
    assert_eq!(storage.get_decided_idx().unwrap(), 1);
    assert_eq!(storage.get_promise().unwrap().unwrap().n, 5);
    assert_eq!(
        storage.get_entries(0, 1).unwrap(),
        vec![Record(vec![0, 255])]
    );
}
