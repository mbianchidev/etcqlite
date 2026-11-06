use omnipaxos::storage::{Entry, Snapshot};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Record(pub Vec<u8>);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Records(pub Vec<Record>);

impl Entry for Record {
    type Snapshot = Records;
}

impl Snapshot<Record> for Records {
    fn create(entries: &[Record]) -> Self {
        Self(entries.to_vec())
    }

    fn merge(&mut self, delta: Self) {
        self.0.extend(delta.0);
    }

    fn use_snapshots() -> bool {
        true
    }
}
