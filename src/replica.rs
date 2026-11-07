use crate::{
    cluster::{Command, Configuration},
    error::{Result, RpcError},
    protocol::SqliteStorage,
    storage::{Store, command::Reply},
};
use omnipaxos::{OmniPaxos, OmniPaxosConfig, ServerConfig, messages::Message, util::LogEntry};
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use uuid::Uuid;

pub struct Replica {
    pub configuration: Configuration,
    pub member_id: u64,
    pub pid: u64,
    core: OmniPaxos<Command, SqliteStorage<Command>>,
    storage_health: Arc<AtomicBool>,
}

impl Replica {
    pub fn open(path: &Path, configuration: Configuration, member_id: u64) -> Result<Self> {
        configuration.validate()?;
        let member = configuration.member(member_id)?;
        if member.learner {
            return Err(RpcError::new(
                tonic::Code::FailedPrecondition,
                "learner bootstrap requires snapshot catch-up before participating",
            ));
        }
        let pid = member.pid;
        let storage = SqliteStorage::open(path, configuration.configuration_id)
            .map_err(|error| RpcError::corrupt(format!("replicated SQLite storage: {error}")))?;
        let storage_health = storage.health_flag();
        let core = OmniPaxosConfig {
            cluster_config: omnipaxos::ClusterConfig {
                configuration_id: configuration.configuration_id,
                nodes: configuration.voters(),
                flexible_quorum: None,
            },
            server_config: ServerConfig {
                pid,
                election_tick_timeout: 10,
                resend_message_tick_timeout: 10,
                batch_size: 1,
                ..Default::default()
            },
        }
        .build(storage)
        .map_err(|error| {
            RpcError::corrupt(format!("replicated protocol configuration: {error}"))
        })?;
        Ok(Self {
            configuration,
            member_id,
            pid,
            core,
            storage_health,
        })
    }

    fn healthy(&self) -> Result<()> {
        if self.storage_health.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(RpcError::corrupt(
                "protocol persistence failed; no messages or authority may leave this replica",
            ))
        }
    }

    pub fn propose(&mut self, command: Command) -> Result<()> {
        self.healthy()?;
        self.core.append(command).map_err(|_| {
            RpcError::new(
                tonic::Code::Unavailable,
                "configuration transition is fencing new proposals",
            )
        })?;
        self.healthy()
    }

    pub fn receive(&mut self, message: Message<Command>) -> Result<()> {
        self.healthy()?;
        if message.get_receiver() != self.pid {
            return Err(RpcError::new(
                tonic::Code::PermissionDenied,
                "peer packet has a different destination",
            ));
        }
        let sender = self.configuration.member_by_pid(message.get_sender())?;
        if sender.learner {
            return Err(RpcError::new(
                tonic::Code::PermissionDenied,
                "learners cannot send voting protocol messages",
            ));
        }
        self.core.handle_incoming(message);
        self.healthy()
    }

    pub fn tick(&mut self) -> Result<()> {
        self.healthy()?;
        self.core.tick();
        self.healthy()
    }

    pub fn reconnected(&mut self, pid: u64) -> Result<()> {
        self.configuration.member_by_pid(pid)?;
        self.core.reconnected(pid);
        self.healthy()
    }

    pub fn outgoing(&mut self) -> Result<Vec<Message<Command>>> {
        self.healthy()?;
        let mut messages = Vec::new();
        self.core.take_outgoing_messages(&mut messages);
        self.healthy()?;
        Ok(messages)
    }

    pub fn term(&self) -> u64 {
        let ballot = self.core.get_promise();
        (u64::from(self.configuration.configuration_id) << 32) | u64::from(ballot.n)
    }

    pub fn leader(&self) -> Option<u64> {
        self.core.get_current_leader().and_then(|(pid, _)| {
            self.configuration
                .members
                .iter()
                .find(|member| member.pid == pid)
                .map(|member| member.id)
        })
    }

    pub fn is_leader(&self) -> bool {
        self.core.get_current_leader() == Some((self.pid, true))
    }

    pub fn decided_index(&self) -> usize {
        self.core.get_decided_idx()
    }

    pub fn apply(&mut self, store: &mut Store) -> Result<Vec<(Uuid, Result<Option<Reply>>)>> {
        self.healthy()?;
        let position = usize::try_from(store.applied_index())
            .map_err(|_| RpcError::corrupt("applied index exceeds platform bounds"))?;
        if position > self.core.get_decided_idx() {
            return Err(RpcError::corrupt(
                "application state is ahead of the durable decided log",
            ));
        }
        let mut replies = Vec::new();
        let Some(entries) = self.core.read_decided_suffix(position) else {
            return Ok(replies);
        };
        for entry in entries {
            match entry {
                LogEntry::Decided(command) => apply_command(store, command, &mut replies)?,
                LogEntry::Snapshotted(snapshot) => {
                    if snapshot.snapshot.commands.len() != snapshot.trimmed_idx {
                        return Err(RpcError::corrupt(
                            "protocol snapshot does not bind the exact decided command prefix",
                        ));
                    }
                    let applied = usize::try_from(store.applied_index()).map_err(|_| {
                        RpcError::corrupt("applied position exceeds platform bounds")
                    })?;
                    for command in snapshot.snapshot.commands.into_iter().skip(applied) {
                        apply_command(store, command, &mut replies)?;
                    }
                }
                LogEntry::StopSign(_, true) => {
                    return Err(RpcError::new(
                        tonic::Code::Unavailable,
                        "committed reconfiguration requires its activation state machine",
                    ));
                }
                _ => {
                    return Err(RpcError::corrupt(
                        "non-decided or unbound log state reached deterministic application",
                    ));
                }
            }
        }
        Ok(replies)
    }
}

fn apply_command(
    store: &mut Store,
    command: Command,
    replies: &mut Vec<(Uuid, Result<Option<Reply>>)>,
) -> Result<()> {
    let id = command.id();
    let result = match command {
        Command::Mutation {
            id,
            principal,
            request,
        } => store
            .command(store.applied_index() + 1, id, principal, request)
            .map(Some),
        Command::Barrier { .. } => store.advance_barrier().map(|_| None),
    };
    if !store.is_healthy() {
        return Err(result
            .err()
            .unwrap_or_else(|| RpcError::corrupt("application failed after a decided command")));
    }
    replies.push((id, result));
    Ok(())
}
