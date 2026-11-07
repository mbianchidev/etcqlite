use omnipaxos::{
    ballot_leader_election::Ballot,
    storage::{Entry, StopSign, Storage, StorageOp, StorageResult},
};
use rusqlite::{Connection, OpenFlags, Transaction, params};
use serde::{Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use std::{
    marker::PhantomData,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("SQLite protocol storage: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("protocol encoding: {0}")]
    Encoding(#[from] serde_json::Error),
    #[error("protocol storage invariant: {0}")]
    Invariant(String),
    #[error("protocol storage is poisoned; stop this replica and recover it")]
    Poisoned,
}

type Result<T> = std::result::Result<T, StorageError>;

pub(crate) fn verify_database(connection: &Connection) -> Result<()> {
    let exists: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'protocol_state')", [], |row| row.get(0))?;
    if !exists {
        return Ok(());
    }
    let mut statement =
        connection.prepare("SELECT configuration_id, checksum FROM protocol_state")?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let configuration_id: u32 = row.get(0)?;
        let checksum: Vec<u8> = row.get(1)?;
        let state = read_state(connection, configuration_id)?;
        ensure(
            state_digest(&state)? == checksum,
            "protocol metadata checksum mismatch",
        )?;
        validate_state(connection, configuration_id, &state)?;
    }
    Ok(())
}

#[derive(Serialize)]
struct State {
    promise: Option<Vec<u8>>,
    accepted_round: Option<Vec<u8>>,
    decided: i64,
    compacted: i64,
    end: i64,
    stopsign: Option<Vec<u8>>,
    snapshot: Option<Vec<u8>>,
}

pub struct SqliteStorage<T: Entry> {
    connection: Connection,
    configuration_id: u32,
    healthy: Arc<AtomicBool>,
    entry: PhantomData<T>,
}

impl<T> SqliteStorage<T>
where
    T: Entry + Serialize + DeserializeOwned,
    T::Snapshot: Serialize + DeserializeOwned,
{
    pub fn open(path: impl AsRef<Path>, configuration_id: u32) -> Result<Self> {
        ensure(configuration_id != 0, "configuration ID cannot be zero")?;
        let connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        crate::sqlite::configure(&connection)?;
        let version: i64 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
        ensure(version <= 2, "unsupported protocol storage schema version")?;
        connection.execute_batch(
            "BEGIN IMMEDIATE;
             CREATE TABLE IF NOT EXISTS protocol_state (
                 configuration_id INTEGER PRIMARY KEY CHECK(configuration_id > 0),
                 promise BLOB,
                 accepted_round BLOB,
                 decided INTEGER NOT NULL DEFAULT 0 CHECK(decided >= 0),
                 compacted INTEGER NOT NULL DEFAULT 0 CHECK(compacted >= 0),
                 end_idx INTEGER NOT NULL DEFAULT 0 CHECK(end_idx >= 0),
                 stopsign BLOB,
                 snapshot BLOB,
                 checksum BLOB NOT NULL
             ) STRICT;
             CREATE TABLE IF NOT EXISTS protocol_entries (
                 configuration_id INTEGER NOT NULL REFERENCES protocol_state(configuration_id),
                 log_idx INTEGER NOT NULL CHECK(log_idx >= 0),
                 payload BLOB NOT NULL,
                 checksum BLOB NOT NULL,
                 PRIMARY KEY (configuration_id, log_idx)
             ) STRICT;
             COMMIT;",
        )?;
        if version == 0 {
            connection.pragma_update(None, "user_version", 1)?;
        }
        let initial = State {
            promise: None,
            accepted_round: None,
            decided: 0,
            compacted: 0,
            end: 0,
            stopsign: None,
            snapshot: None,
        };
        connection.execute(
            "INSERT OR IGNORE INTO protocol_state(configuration_id, checksum) VALUES(?1, ?2)",
            params![configuration_id, state_digest(&initial)?],
        )?;
        let integrity: String = connection.query_row("PRAGMA quick_check", [], |row| row.get(0))?;
        ensure(
            integrity == "ok",
            &format!("SQLite integrity check: {integrity}"),
        )?;
        let storage = Self {
            connection,
            configuration_id,
            healthy: Arc::new(AtomicBool::new(true)),
            entry: PhantomData,
        };
        let state = read_state(&storage.connection, configuration_id)?;
        let stored_digest: Vec<u8> = storage.connection.query_row(
            "SELECT checksum FROM protocol_state WHERE configuration_id = ?1",
            [configuration_id],
            |row| row.get(0),
        )?;
        ensure(
            state_digest(&state)? == stored_digest,
            "protocol metadata checksum mismatch",
        )?;
        validate_state(&storage.connection, configuration_id, &state)?;
        Ok(storage)
    }

    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }

    pub fn health_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.healthy)
    }

    fn guard<R>(&self, result: Result<R>) -> StorageResult<R> {
        match result {
            Ok(value) => Ok(value),
            Err(error) => {
                self.healthy.store(false, Ordering::Release);
                Err(Box::new(error))
            }
        }
    }

    fn ready(&self) -> Result<()> {
        if self.is_healthy() {
            Ok(())
        } else {
            Err(StorageError::Poisoned)
        }
    }

    fn write(&mut self, operations: Vec<StorageOp<T>>) -> Result<()> {
        self.ready()?;
        let config = self.configuration_id;
        let transaction = self
            .connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let mut state = read_state(&transaction, config)?;
        let immutable_prefix = state.decided.min(state.end);
        let immutable_stopsign = if state.decided > state.end {
            state.stopsign.clone()
        } else {
            None
        };
        for operation in operations {
            match operation {
                StorageOp::AppendEntry(entry) => {
                    ensure(
                        immutable_stopsign.is_none(),
                        "cannot append data after a decided StopSign",
                    )?;
                    append(&transaction, config, &mut state.end, vec![entry])?;
                }
                StorageOp::AppendEntries(entries) => {
                    ensure(
                        immutable_stopsign.is_none() || entries.is_empty(),
                        "cannot append data after a decided StopSign",
                    )?;
                    append(&transaction, config, &mut state.end, entries)?;
                }
                StorageOp::AppendOnPrefix(from, entries) => {
                    ensure(
                        immutable_stopsign.is_none() || entries.is_empty(),
                        "cannot append a suffix after a decided StopSign",
                    )?;
                    let from = index(from)?;
                    ensure(
                        from >= immutable_prefix,
                        "cannot overwrite a decided prefix",
                    )?;
                    ensure(
                        from >= state.compacted && from <= state.end,
                        &format!(
                            "append prefix {from} is outside configuration {config} log [{}, {}], decided {}",
                            state.compacted, state.end, state.decided
                        ),
                    )?;
                    transaction.execute(
                        "DELETE FROM protocol_entries WHERE configuration_id = ?1 AND log_idx >= ?2",
                        params![config, from],
                    )?;
                    state.end = from;
                    append(&transaction, config, &mut state.end, entries)?;
                }
                StorageOp::SetPromise(ballot) => {
                    validate_ballot(config, &state.promise, ballot)?;
                    state.promise = Some(serde_json::to_vec(&ballot)?);
                }
                StorageOp::SetAcceptedRound(ballot) => {
                    validate_ballot(config, &state.accepted_round, ballot)?;
                    state.accepted_round = Some(serde_json::to_vec(&ballot)?);
                }
                StorageOp::SetDecidedIndex(decided) => {
                    let decided = index(decided)?;
                    ensure(decided >= state.decided, "decided position cannot decrease")?;
                    state.decided = decided;
                }
                StorageOp::SetCompactedIdx(compacted) => {
                    let compacted = index(compacted)?;
                    ensure(
                        compacted >= state.compacted,
                        "compacted position cannot decrease",
                    )?;
                    state.compacted = compacted;
                }
                StorageOp::Trim(to) => {
                    let to = index(to)?;
                    ensure(
                        immutable_stopsign.is_none() || to <= state.end,
                        "cannot trim beyond the data prefix of a decided StopSign",
                    )?;
                    ensure(to >= state.compacted, "trim position cannot decrease")?;
                    transaction.execute(
                        "DELETE FROM protocol_entries WHERE configuration_id = ?1 AND log_idx < ?2",
                        params![config, to],
                    )?;
                    state.end = state.end.max(to);
                }
                StorageOp::SetStopsign(stopsign) => {
                    if let Some(expected) = &immutable_stopsign {
                        ensure(
                            stopsign
                                .as_ref()
                                .map(serde_json::to_vec)
                                .transpose()?
                                .as_ref()
                                == Some(expected),
                            "a decided StopSign cannot be replaced or removed",
                        )?;
                    }
                    if let Some(stopsign) = &stopsign {
                        ensure(
                            stopsign.next_config.configuration_id > config,
                            "next configuration must increase",
                        )?;
                        stopsign
                            .next_config
                            .validate()
                            .map_err(|error| StorageError::Invariant(error.to_string()))?;
                    }
                    state.stopsign = stopsign
                        .map(|value| serde_json::to_vec(&value))
                        .transpose()?;
                }
                StorageOp::SetSnapshot(snapshot) => {
                    state.snapshot = snapshot
                        .map(|value| serde_json::to_vec(&value))
                        .transpose()?;
                }
            }
        }
        validate_state(&transaction, config, &state)?;
        transaction.execute(
            "UPDATE protocol_state
             SET promise = ?2, accepted_round = ?3, decided = ?4, compacted = ?5,
                 end_idx = ?6, stopsign = ?7, snapshot = ?8, checksum = ?9
             WHERE configuration_id = ?1",
            params![
                config,
                state.promise,
                state.accepted_round,
                state.decided,
                state.compacted,
                state.end,
                state.stopsign,
                state.snapshot,
                state_digest(&state)?,
            ],
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn read_field<R: DeserializeOwned>(&self, field: &str) -> Result<Option<R>> {
        self.ready()?;
        let sql = format!("SELECT {field} FROM protocol_state WHERE configuration_id = ?1");
        let bytes: Option<Vec<u8>> =
            self.connection
                .query_row(&sql, [self.configuration_id], |row| row.get(0))?;
        bytes
            .map(|value| serde_json::from_slice(&value))
            .transpose()
            .map_err(Into::into)
    }

    fn read_index(&self, field: &str) -> Result<usize> {
        self.ready()?;
        let sql = format!("SELECT {field} FROM protocol_state WHERE configuration_id = ?1");
        let value: i64 = self
            .connection
            .query_row(&sql, [self.configuration_id], |row| row.get(0))?;
        usize::try_from(value)
            .map_err(|_| StorageError::Invariant(format!("invalid {field} position")))
    }

    fn entries(&self, from: usize, to: usize) -> Result<Vec<T>> {
        self.ready()?;
        let state = read_state(&self.connection, self.configuration_id)?;
        let (from, to) = (index(from)?, index(to)?);
        if from < state.compacted || from >= to || to > state.end {
            return Ok(Vec::new());
        }
        let mut statement = self.connection.prepare(
            "SELECT log_idx, payload, checksum FROM protocol_entries
             WHERE configuration_id = ?1 AND log_idx >= ?2 AND log_idx < ?3
             ORDER BY log_idx",
        )?;
        let mut rows = statement.query(params![self.configuration_id, from, to])?;
        let mut entries = Vec::new();
        let mut expected_index = from;
        while let Some(row) = rows.next()? {
            let log_index: i64 = row.get(0)?;
            let bytes: Vec<u8> = row.get(1)?;
            let checksum: Vec<u8> = row.get(2)?;
            ensure(log_index == expected_index, "protocol log contains a gap")?;
            ensure(
                entry_digest(self.configuration_id, log_index, &bytes) == checksum,
                "protocol entry checksum mismatch",
            )?;
            entries.push(serde_json::from_slice(&bytes)?);
            expected_index += 1;
        }
        ensure(expected_index == to, "protocol log interval is incomplete")?;
        Ok(entries)
    }
}

impl<T> Storage<T> for SqliteStorage<T>
where
    T: Entry + Serialize + DeserializeOwned,
    T::Snapshot: Serialize + DeserializeOwned,
{
    fn write_atomically(&mut self, operations: Vec<StorageOp<T>>) -> StorageResult<()> {
        let result = self.write(operations);
        self.guard(result)
    }

    fn append_entry(&mut self, entry: T) -> StorageResult<()> {
        self.write_atomically(vec![StorageOp::AppendEntry(entry)])
    }

    fn append_entries(&mut self, entries: Vec<T>) -> StorageResult<()> {
        self.write_atomically(vec![StorageOp::AppendEntries(entries)])
    }

    fn append_on_prefix(&mut self, from: usize, entries: Vec<T>) -> StorageResult<()> {
        self.write_atomically(vec![StorageOp::AppendOnPrefix(from, entries)])
    }

    fn set_promise(&mut self, promise: Ballot) -> StorageResult<()> {
        self.write_atomically(vec![StorageOp::SetPromise(promise)])
    }

    fn get_promise(&self) -> StorageResult<Option<Ballot>> {
        self.guard(self.read_field("promise"))
    }

    fn set_accepted_round(&mut self, ballot: Ballot) -> StorageResult<()> {
        self.write_atomically(vec![StorageOp::SetAcceptedRound(ballot)])
    }

    fn get_accepted_round(&self) -> StorageResult<Option<Ballot>> {
        self.guard(self.read_field("accepted_round"))
    }

    fn set_decided_idx(&mut self, index: usize) -> StorageResult<()> {
        self.write_atomically(vec![StorageOp::SetDecidedIndex(index)])
    }

    fn get_decided_idx(&self) -> StorageResult<usize> {
        self.guard(self.read_index("decided"))
    }

    fn get_entries(&self, from: usize, to: usize) -> StorageResult<Vec<T>> {
        self.guard(self.entries(from, to))
    }

    fn get_suffix(&self, from: usize) -> StorageResult<Vec<T>> {
        let result = self
            .read_index("end_idx")
            .and_then(|to| self.entries(from, to));
        self.guard(result)
    }

    fn get_log_len(&self) -> StorageResult<usize> {
        let result = self.read_index("end_idx").and_then(|end| {
            let compacted = self.read_index("compacted")?;
            end.checked_sub(compacted)
                .ok_or_else(|| StorageError::Invariant("invalid retained log length".into()))
        });
        self.guard(result)
    }

    fn set_stopsign(&mut self, stopsign: Option<StopSign>) -> StorageResult<()> {
        self.write_atomically(vec![StorageOp::SetStopsign(stopsign)])
    }

    fn get_stopsign(&self) -> StorageResult<Option<StopSign>> {
        self.guard(self.read_field("stopsign"))
    }

    fn trim(&mut self, to: usize) -> StorageResult<()> {
        self.write_atomically(vec![StorageOp::Trim(to)])
    }

    fn set_compacted_idx(&mut self, index: usize) -> StorageResult<()> {
        self.write_atomically(vec![StorageOp::SetCompactedIdx(index)])
    }

    fn get_compacted_idx(&self) -> StorageResult<usize> {
        self.guard(self.read_index("compacted"))
    }

    fn set_snapshot(&mut self, snapshot: Option<T::Snapshot>) -> StorageResult<()> {
        self.write_atomically(vec![StorageOp::SetSnapshot(snapshot)])
    }

    fn get_snapshot(&self) -> StorageResult<Option<T::Snapshot>> {
        self.guard(self.read_field("snapshot"))
    }
}

fn ensure(condition: bool, message: &str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(StorageError::Invariant(message.to_owned()))
    }
}

fn index(value: usize) -> Result<i64> {
    i64::try_from(value).map_err(|_| {
        StorageError::Invariant("protocol position exceeds signed 64-bit range".into())
    })
}

fn read_state(connection: &Connection, configuration_id: u32) -> Result<State> {
    Ok(connection.query_row(
        "SELECT promise, accepted_round, decided, compacted, end_idx, stopsign, snapshot
         FROM protocol_state WHERE configuration_id = ?1",
        [configuration_id],
        |row| {
            Ok(State {
                promise: row.get(0)?,
                accepted_round: row.get(1)?,
                decided: row.get(2)?,
                compacted: row.get(3)?,
                end: row.get(4)?,
                stopsign: row.get(5)?,
                snapshot: row.get(6)?,
            })
        },
    )?)
}

fn validate_ballot(config: u32, previous: &Option<Vec<u8>>, ballot: Ballot) -> Result<()> {
    ensure(
        ballot == Ballot::default() || ballot.config_id == config,
        "ballot belongs to another configuration",
    )?;
    if let Some(previous) = previous {
        let previous: Ballot = serde_json::from_slice(previous)?;
        ensure(ballot >= previous, "protocol ballot cannot decrease")?;
    }
    Ok(())
}

fn validate_state(connection: &Connection, config: u32, state: &State) -> Result<()> {
    ensure(
        state.compacted <= state.decided && state.compacted <= state.end,
        "compaction exceeds decided or accepted log",
    )?;
    let accepted = state
        .end
        .checked_add(i64::from(state.stopsign.is_some()))
        .ok_or_else(|| StorageError::Invariant("accepted position exhausted".into()))?;
    ensure(state.decided <= accepted, "decision exceeds accepted log")?;
    let (count, first, last): (i64, Option<i64>, Option<i64>) = connection.query_row(
        "SELECT COUNT(*), MIN(log_idx), MAX(log_idx) FROM protocol_entries
         WHERE configuration_id = ?1",
        [config],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    ensure(
        count == state.end - state.compacted,
        "retained log length mismatch",
    )?;
    if count != 0 {
        ensure(
            first == Some(state.compacted) && last == Some(state.end - 1),
            "retained log boundary mismatch",
        )?;
    }
    Ok(())
}

fn append<T: Serialize>(
    transaction: &Transaction<'_>,
    config: u32,
    end: &mut i64,
    entries: Vec<T>,
) -> Result<()> {
    for entry in entries {
        let next = end
            .checked_add(1)
            .ok_or_else(|| StorageError::Invariant("protocol log index exhausted".into()))?;
        let payload = serde_json::to_vec(&entry)?;
        transaction.execute(
            "INSERT INTO protocol_entries(configuration_id, log_idx, payload, checksum)
             VALUES(?1, ?2, ?3, ?4)",
            params![config, *end, payload, entry_digest(config, *end, &payload)],
        )?;
        *end = next;
    }
    Ok(())
}

fn state_digest(state: &State) -> Result<Vec<u8>> {
    Ok(Sha256::digest(serde_json::to_vec(state)?).to_vec())
}

fn entry_digest(config: u32, index: i64, payload: &[u8]) -> Vec<u8> {
    let mut digest = Sha256::new();
    digest.update(config.to_be_bytes());
    digest.update(index.to_be_bytes());
    digest.update(payload);
    digest.finalize().to_vec()
}
