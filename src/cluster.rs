use crate::{
    error::{Result, RpcError},
    storage::{auth::Principal, command::Mutation},
};
use omnipaxos::storage::{Entry, Snapshot};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use tonic::Code;
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Configuration {
    pub cluster_id: u64,
    pub configuration_id: u32,
    pub members: Vec<Member>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Member {
    pub id: u64,
    pub pid: u64,
    pub name: String,
    pub client_url: String,
    pub peer_url: String,
    #[serde(default)]
    pub learner: bool,
}

impl Configuration {
    pub fn validate(&self) -> Result<()> {
        if self.cluster_id == 0 || self.configuration_id == 0 {
            return Err(RpcError::invalid(
                "cluster/configuration IDs must be nonzero",
            ));
        }
        let voters = self.members.iter().filter(|member| !member.learner).count();
        if voters < 2 || self.members.len() > 128 {
            return Err(RpcError::new(
                Code::FailedPrecondition,
                "replicated configurations require 2..128 voters/members",
            ));
        }
        let mut ids = BTreeSet::new();
        let mut pids = BTreeSet::new();
        let mut urls = BTreeSet::new();
        for member in &self.members {
            if member.id == 0
                || member.pid == 0
                || member.pid > 128
                || member.name.is_empty()
                || !ids.insert(member.id)
                || !pids.insert(member.pid)
                || !urls.insert(&member.peer_url)
            {
                return Err(RpcError::invalid(
                    "cluster members require unique nonzero IDs, bounded process IDs, names and peer URLs",
                ));
            }
            for url in [&member.client_url, &member.peer_url] {
                if !url.starts_with("https://") {
                    return Err(RpcError::new(
                        Code::FailedPrecondition,
                        "cluster client and peer endpoints must use authenticated HTTPS",
                    ));
                }
                let uri: axum::http::Uri = url
                    .parse()
                    .map_err(|_| RpcError::invalid("cluster URL is invalid"))?;
                if uri.authority().is_none() || uri.path() != "/" {
                    return Err(RpcError::invalid(
                        "cluster URLs require a host/port and no path",
                    ));
                }
            }
        }
        Ok(())
    }

    pub fn member(&self, id: u64) -> Result<&Member> {
        self.members
            .iter()
            .find(|member| member.id == id)
            .ok_or_else(|| RpcError::new(Code::NotFound, "etcdserver: member not found"))
    }

    pub fn member_by_pid(&self, pid: u64) -> Result<&Member> {
        self.members
            .iter()
            .find(|member| member.pid == pid)
            .ok_or_else(|| RpcError::corrupt("protocol message refers to an unknown process ID"))
    }

    pub fn voters(&self) -> Vec<u64> {
        self.members
            .iter()
            .filter(|member| !member.learner)
            .map(|member| member.pid)
            .collect()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Command {
    Mutation {
        id: Uuid,
        principal: Principal,
        request: Mutation,
    },
    Barrier {
        id: Uuid,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommandSnapshot {
    pub commands: Vec<Command>,
}

impl Entry for Command {
    type Snapshot = CommandSnapshot;
}

impl Snapshot<Command> for CommandSnapshot {
    fn create(commands: &[Command]) -> Self {
        Self {
            commands: commands.to_vec(),
        }
    }

    fn merge(&mut self, delta: Self) {
        self.commands.extend(delta.commands);
    }

    fn use_snapshots() -> bool {
        true
    }
}

impl Command {
    pub fn id(&self) -> Uuid {
        match self {
            Self::Mutation { id, .. } | Self::Barrier { id } => *id,
        }
    }
}
