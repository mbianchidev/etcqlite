use crate::{
    cluster::{Catchup, Command, Configuration, Outgoing, PeerMessage, Transition},
    error::{Result, RpcError},
    protocol::SqliteStorage,
    storage::{Store, command::Reply},
};
use omnipaxos::{
    OmniPaxos, OmniPaxosConfig, ServerConfig,
    storage::{StopSign, Storage, StorageOp},
    util::LogEntry,
};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tonic::Code;
use uuid::Uuid;

type AppliedReplies = Vec<(Uuid, Result<Option<Reply>>)>;

struct Engine {
    configuration: Configuration,
    base: u64,
    pid: u64,
    core: OmniPaxos<Command, SqliteStorage<Command>>,
    health: Arc<AtomicBool>,
}

impl Engine {
    fn open(path: &Path, configuration: Configuration, member_id: u64, base: u64) -> Result<Self> {
        let pid = configuration.member(member_id)?.pid;
        let storage = SqliteStorage::open(path, configuration.configuration_id)
            .map_err(|error| RpcError::corrupt(format!("replicated SQLite storage: {error}")))?;
        let health = storage.health_flag();
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
            base,
            pid,
            core,
            health,
        })
    }

    fn healthy(&self) -> Result<()> {
        if self.health.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(RpcError::corrupt(
                "protocol persistence failed; no messages or authority may leave this replica",
            ))
        }
    }

    fn term(&self) -> u64 {
        (u64::from(self.configuration.configuration_id) << 32)
            | u64::from(self.core.get_promise().n)
    }

    fn leader(&self) -> Option<u64> {
        self.core.get_current_leader().and_then(|(pid, _)| {
            self.configuration
                .member_by_pid(pid)
                .ok()
                .map(|member| member.id)
        })
    }

    fn is_leader(&self) -> bool {
        self.core.get_current_leader() == Some((self.pid, true))
    }
}

pub struct Replica {
    pub configuration: Configuration,
    pub member_id: u64,
    pub pid: u64,
    path: PathBuf,
    base: u64,
    current: Option<Engine>,
    passive: Option<SqliteStorage<Command>>,
    archives: BTreeMap<u32, Engine>,
    submitted: Vec<Outgoing>,
    activation: Option<Uuid>,
    observed_term: u64,
    observed_leader: Option<u64>,
}

impl Replica {
    pub fn open(store: &Store) -> Result<Self> {
        let configuration = store
            .cluster_configuration()?
            .ok_or_else(|| RpcError::corrupt("replicated startup has no configuration"))?;
        configuration.validate()?;
        let member_id = store.identity().member_id;
        let member = configuration.member(member_id)?;
        let incarnation = store.incarnation()?;
        if member.incarnation.is_some() && incarnation != member.incarnation {
            return Err(RpcError::new(
                Code::FailedPrecondition,
                "member incarnation does not match its committed identity; use a new member ID for replacement",
            ));
        }
        let pid = member.pid;
        let base = store.configuration_base();
        let history = store.configuration_history()?;
        let activation = history
            .iter()
            .find(|(entry, _, _)| entry.configuration_id == configuration.configuration_id)
            .and_then(|entry| entry.2);
        let current = if member.learner || !store.identity_registered()? {
            None
        } else {
            Some(Engine::open(
                store.path(),
                configuration.clone(),
                member_id,
                base,
            )?)
        };
        let passive = if current.is_none() {
            Some(
                SqliteStorage::open(store.path(), configuration.configuration_id).map_err(
                    |error| RpcError::corrupt(format!("passive protocol storage: {error}")),
                )?,
            )
        } else {
            None
        };
        let mut archives = BTreeMap::new();
        for (previous, base, _) in history {
            if previous.configuration_id < configuration.configuration_id
                && previous
                    .members
                    .iter()
                    .any(|member| member.id == member_id && !member.learner)
            {
                let engine = Engine::open(store.path(), previous, member_id, base)?;
                if engine.core.is_reconfigured().is_none() {
                    return Err(RpcError::corrupt(
                        "active configuration has an unstopped voting predecessor",
                    ));
                }
                archives.insert(engine.configuration.configuration_id, engine);
            }
        }
        Ok(Self {
            observed_term: u64::from(configuration.configuration_id) << 32,
            observed_leader: None,
            configuration,
            member_id,
            pid,
            path: store.path().to_owned(),
            base,
            current,
            passive,
            archives,
            submitted: Vec::new(),
            activation,
        })
    }

    fn healthy(&self) -> Result<()> {
        if let Some(current) = &self.current {
            current.healthy()?;
        }
        if self
            .passive
            .as_ref()
            .is_some_and(|storage| !storage.is_healthy())
        {
            return Err(RpcError::corrupt("passive protocol persistence failed"));
        }
        for archive in self.archives.values() {
            archive.healthy()?;
        }
        Ok(())
    }

    pub fn removed(&self) -> bool {
        !self
            .configuration
            .members
            .iter()
            .any(|member| member.id == self.member_id)
    }
    pub fn learner(&self) -> bool {
        self.configuration
            .members
            .iter()
            .any(|member| member.id == self.member_id && member.learner)
    }
    pub fn non_voting(&self) -> bool {
        self.current.is_none() && !self.removed()
    }

    pub fn propose(&mut self, command: Command) -> Result<()> {
        self.healthy()?;
        if self.removed() {
            return Err(RpcError::new(
                Code::Unavailable,
                "etcdserver: member permanently removed",
            ));
        }
        if let Some(current) = &mut self.current {
            current.core.append(command).map_err(|_| {
                RpcError::new(
                    Code::Unavailable,
                    "configuration transition is fencing new proposals",
                )
            })?;
        } else {
            let receiver = self
                .observed_leader
                .filter(|id| {
                    self.configuration
                        .members
                        .iter()
                        .any(|member| member.id == *id && !member.learner)
                })
                .or_else(|| {
                    self.configuration
                        .members
                        .iter()
                        .find(|member| !member.learner)
                        .map(|member| member.id)
                })
                .ok_or_else(|| RpcError::new(Code::Unavailable, "etcdserver: no leader"))?;
            self.submitted.push(Outgoing {
                configuration: self.configuration.clone(),
                receiver,
                message: PeerMessage::Proposal(command),
            });
        }
        self.healthy()
    }

    pub fn receive(
        &mut self,
        configuration_id: u32,
        sender_id: u64,
        message: PeerMessage,
    ) -> Result<()> {
        self.healthy()?;
        match message {
            PeerMessage::Proposal(command) => {
                if configuration_id != self.configuration.configuration_id {
                    return Err(RpcError::new(
                        Code::Unavailable,
                        "proposal belongs to a stale configuration",
                    ));
                }
                self.configuration.member(sender_id)?;
                self.propose(command)
            }
            PeerMessage::Protocol(message) => {
                let engine = if configuration_id == self.configuration.configuration_id {
                    self.current.as_mut()
                } else {
                    self.archives.get_mut(&configuration_id)
                }
                .ok_or_else(|| {
                    RpcError::new(
                        Code::PermissionDenied,
                        "non-voting or stale protocol destination",
                    )
                })?;
                let sender = engine.configuration.member(sender_id)?;
                if sender.learner
                    || message.get_sender() != sender.pid
                    || message.get_receiver() != engine.pid
                {
                    return Err(RpcError::new(
                        Code::PermissionDenied,
                        "protocol identities do not match voting membership",
                    ));
                }
                engine.core.handle_incoming(message);
                self.healthy()
            }
        }
    }

    pub fn tick(&mut self, store: &Store) -> Result<()> {
        self.healthy()?;
        for archive in self.archives.values_mut() {
            archive.core.tick();
        }
        if let Some(current) = &mut self.current {
            current.core.tick();
            if current.is_leader() {
                if let Some(transition) = store.transition()? {
                    if current.core.is_reconfigured().is_none() {
                        let next = omnipaxos::ClusterConfig {
                            configuration_id: transition.configuration.configuration_id,
                            nodes: transition.configuration.voters(),
                            flexible_quorum: None,
                        };
                        if current
                            .core
                            .reconfigure(next, Some(serde_json::to_vec(&transition)?))
                            .is_err()
                        {
                            tracing::debug!("membership StopSign is already in flight");
                        }
                    }
                } else if let Some(id) = self.activation {
                    current.core.append(Command::Barrier { id }).map_err(|_| {
                        RpcError::new(Code::Unavailable, "activation barrier is fenced")
                    })?;
                }
            }
        }
        self.healthy()
    }

    pub fn reconnected(&mut self, configuration_id: u32, pid: u64) -> Result<()> {
        let engine = if configuration_id == self.configuration.configuration_id {
            self.current.as_mut()
        } else {
            self.archives.get_mut(&configuration_id)
        };
        if let Some(engine) = engine {
            engine.configuration.member_by_pid(pid)?;
            engine.core.reconnected(pid);
        }
        self.healthy()
    }

    pub fn outgoing(&mut self) -> Result<Vec<Outgoing>> {
        self.healthy()?;
        let mut outgoing = std::mem::take(&mut self.submitted);
        for engine in self.current.iter_mut().chain(self.archives.values_mut()) {
            let mut messages = Vec::new();
            engine.core.take_outgoing_messages(&mut messages);
            for message in messages {
                let receiver = engine
                    .configuration
                    .member_by_pid(message.get_receiver())?
                    .id;
                outgoing.push(Outgoing {
                    configuration: engine.configuration.clone(),
                    receiver,
                    message: PeerMessage::Protocol(message),
                });
            }
        }
        self.healthy()?;
        Ok(outgoing)
    }

    pub fn term(&self) -> u64 {
        self.current
            .as_ref()
            .map_or(self.observed_term, Engine::term)
    }
    pub fn leader(&self) -> Option<u64> {
        self.current
            .as_ref()
            .map_or(self.observed_leader, Engine::leader)
    }
    pub fn is_leader(&self) -> bool {
        self.current.as_ref().is_some_and(Engine::is_leader)
    }
    pub fn decided_index(&self, store: &Store) -> Result<u64> {
        if let Some(current) = &self.current {
            current
                .base
                .checked_add(
                    u64::try_from(current.core.get_decided_idx())
                        .map_err(|_| RpcError::corrupt("decided position exceeds wire range"))?,
                )
                .ok_or_else(|| RpcError::corrupt("global decided position exhausted"))
        } else {
            Ok(store.applied_index())
        }
    }

    pub fn attempt_leadership(&mut self) -> Result<()> {
        self.healthy()?;
        let current = self
            .current
            .as_mut()
            .ok_or_else(|| RpcError::new(Code::FailedPrecondition, "learners cannot lead"))?;
        if current.core.get_promise().n == u32::MAX {
            return Err(RpcError::new(
                Code::OutOfRange,
                "leadership ballot exhausted",
            ));
        }
        current.core.try_become_leader();
        self.healthy()
    }

    pub fn apply(&mut self, store: &mut Store) -> Result<AppliedReplies> {
        self.healthy()?;
        let mut replies = Vec::new();
        let mut stopped = None;
        if let Some(current) = &self.current {
            let relative = store
                .applied_index()
                .checked_sub(current.base)
                .ok_or_else(|| RpcError::corrupt("application is behind its configuration base"))?;
            let position = usize::try_from(relative)
                .map_err(|_| RpcError::corrupt("applied index exceeds platform bounds"))?;
            if position > current.core.get_decided_idx() {
                return Err(RpcError::corrupt(
                    "application is ahead of the durable decided log",
                ));
            }
            if let Some(entries) = current.core.read_decided_suffix(position) {
                for entry in entries {
                    match entry {
                        LogEntry::Decided(command) => {
                            if Some(command.id()) == self.activation
                                && matches!(command, Command::Barrier { .. })
                            {
                                self.activation = None;
                            }
                            apply_command(store, command, &mut replies)?;
                        }
                        LogEntry::Snapshotted(snapshot) => {
                            if snapshot.snapshot.commands.len() != snapshot.trimmed_idx {
                                return Err(RpcError::corrupt(
                                    "protocol snapshot does not bind its exact decided prefix",
                                ));
                            }
                            let applied = usize::try_from(store.applied_index() - current.base)
                                .map_err(|_| {
                                    RpcError::corrupt("snapshot position exceeds platform bounds")
                                })?;
                            for command in snapshot.snapshot.commands.into_iter().skip(applied) {
                                if Some(command.id()) == self.activation
                                    && matches!(command, Command::Barrier { .. })
                                {
                                    self.activation = None;
                                }
                                apply_command(store, command, &mut replies)?;
                            }
                        }
                        LogEntry::StopSign(sign, true) => {
                            let transition: Transition =
                                serde_json::from_slice(&sign.metadata.ok_or_else(|| {
                                    RpcError::corrupt("StopSign has no application transition")
                                })?)?;
                            if sign.next_config.configuration_id
                                != transition.configuration.configuration_id
                                || sign.next_config.nodes != transition.configuration.voters()
                            {
                                return Err(RpcError::corrupt(
                                    "protocol and application transition memberships differ",
                                ));
                            }
                            stopped = Some(transition);
                        }
                        _ => {
                            return Err(RpcError::corrupt(
                                "non-decided log state reached deterministic application",
                            ));
                        }
                    }
                }
            }
        }
        if self.current.is_none() {
            stopped = self.apply_passive(store, &mut replies)?;
        }
        if let Some(transition) = stopped {
            self.activate(store, transition)?;
        }
        if !self.removed() {
            let latest = store.cluster_configuration()?.ok_or_else(|| {
                RpcError::corrupt("replicated application lost its configuration")
            })?;
            if latest.configuration_id != self.configuration.configuration_id
                || latest.voters() != self.configuration.voters()
            {
                return Err(RpcError::corrupt(
                    "application changed voting membership without StopSign activation",
                ));
            }
            self.configuration = latest;
            if let Some(current) = &mut self.current {
                current.configuration = self.configuration.clone();
            }
            if self.current.is_none() && !self.learner() && store.identity_registered()? {
                self.passive = None;
                self.current = Some(Engine::open(
                    &self.path,
                    self.configuration.clone(),
                    self.member_id,
                    self.base,
                )?);
            }
        }
        self.healthy()?;
        Ok(replies)
    }

    fn apply_passive(
        &self,
        store: &mut Store,
        replies: &mut AppliedReplies,
    ) -> Result<Option<Transition>> {
        let Some(passive) = &self.passive else {
            return Ok(None);
        };
        let compacted = passive.get_compacted_idx().map_err(storage_error)?;
        let end = compacted
            .checked_add(passive.get_log_len().map_err(storage_error)?)
            .ok_or_else(|| RpcError::corrupt("passive log position overflow"))?;
        let decided = passive.get_decided_idx().map_err(storage_error)?;
        let position = usize::try_from(
            store
                .applied_index()
                .checked_sub(self.base)
                .ok_or_else(|| RpcError::corrupt("passive application is behind its base"))?,
        )
        .map_err(|_| RpcError::corrupt("passive application position exceeds platform bounds"))?;
        if position > decided {
            return Err(RpcError::corrupt(
                "passive application is ahead of its certified decided prefix",
            ));
        }
        if position < compacted {
            let snapshot = passive
                .get_snapshot()
                .map_err(storage_error)?
                .ok_or_else(|| RpcError::corrupt("passive snapshot is missing"))?;
            if snapshot.commands.len() != compacted {
                return Err(RpcError::corrupt("passive snapshot prefix is invalid"));
            }
            for command in snapshot.commands.into_iter().skip(position) {
                apply_command(store, command, replies)?;
            }
        }
        let from = position.max(compacted);
        for command in passive
            .get_entries(from, decided.min(end))
            .map_err(storage_error)?
        {
            apply_command(store, command, replies)?;
        }
        if decided > end {
            let sign = passive
                .get_stopsign()
                .map_err(storage_error)?
                .ok_or_else(|| RpcError::corrupt("passive StopSign is missing"))?;
            let transition: Transition =
                serde_json::from_slice(&sign.metadata.ok_or_else(|| {
                    RpcError::corrupt("passive StopSign has no application metadata")
                })?)?;
            if sign.next_config.configuration_id != transition.configuration.configuration_id
                || sign.next_config.nodes != transition.configuration.voters()
            {
                return Err(RpcError::corrupt(
                    "passive protocol and application memberships differ",
                ));
            }
            return Ok(Some(transition));
        }
        Ok(None)
    }

    fn activate(&mut self, store: &mut Store, transition: Transition) -> Result<()> {
        store.activate_configuration(&transition)?;
        if let Some(previous) = self.current.take() {
            self.archives
                .insert(previous.configuration.configuration_id, previous);
        }
        self.passive = None;
        self.configuration = transition.configuration;
        self.base = store.configuration_base();
        self.activation = Some(transition.id);
        self.observed_term = u64::from(self.configuration.configuration_id) << 32;
        self.observed_leader = None;
        if let Ok(member) = self.configuration.member(self.member_id) {
            self.pid = member.pid;
            if !member.learner && store.identity_registered()? {
                self.current = Some(Engine::open(
                    &self.path,
                    self.configuration.clone(),
                    self.member_id,
                    self.base,
                )?);
            } else {
                self.passive = Some(
                    SqliteStorage::open(&self.path, self.configuration.configuration_id).map_err(
                        |error| {
                            RpcError::corrupt(format!("new passive configuration storage: {error}"))
                        },
                    )?,
                );
            }
        }
        Ok(())
    }

    pub fn catchup(&self, configuration_id: u32, from: u64) -> Result<Catchup> {
        self.healthy()?;
        let engine = if configuration_id == self.configuration.configuration_id {
            self.current.as_ref()
        } else {
            self.archives.get(&configuration_id)
        }
        .ok_or_else(|| {
            RpcError::new(
                Code::Unavailable,
                "requested configuration has no voting history here",
            )
        })?;
        let stop = engine.core.is_reconfigured();
        let decided = engine
            .core
            .get_decided_idx()
            .checked_sub(usize::from(stop.is_some()))
            .ok_or_else(|| RpcError::corrupt("invalid stopped decided frontier"))?;
        let position = usize::try_from(from)
            .map_err(|_| RpcError::invalid("catch-up cursor exceeds platform bounds"))?;
        if position > decided {
            return Err(RpcError::new(
                Code::Unavailable,
                "source is behind the requested catch-up cursor",
            ));
        }
        let end = decided.min(position.saturating_add(32));
        let mut commands = Vec::new();
        if position < end {
            for entry in engine
                .core
                .read_entries(position..end)
                .ok_or_else(|| RpcError::corrupt("decided catch-up range is missing"))?
            {
                match entry {
                    LogEntry::Decided(command) => commands.push(command),
                    LogEntry::Snapshotted(snapshot) => {
                        if snapshot.snapshot.commands.len() != snapshot.trimmed_idx {
                            return Err(RpcError::corrupt(
                                "catch-up snapshot has an invalid prefix",
                            ));
                        }
                        commands.extend(
                            snapshot
                                .snapshot
                                .commands
                                .into_iter()
                                .skip(position)
                                .take(end - position),
                        );
                    }
                    _ => {
                        return Err(RpcError::corrupt(
                            "catch-up attempted to expose an undecided entry",
                        ));
                    }
                }
            }
        }
        let transition = if end == decided {
            stop.map(|sign| -> Result<Transition> {
                Ok(serde_json::from_slice(&sign.metadata.ok_or_else(
                    || RpcError::corrupt("catch-up StopSign has no metadata"),
                )?)?)
            })
            .transpose()?
        } else {
            None
        };
        Ok(Catchup {
            configuration_id,
            from,
            term: engine.term(),
            leader_id: engine.leader(),
            commands,
            transition,
        })
    }

    pub fn install_catchup(&mut self, store: &mut Store, frame: Catchup) -> Result<()> {
        if !self.non_voting()
            || frame.configuration_id != self.configuration.configuration_id
            || frame.from != store.applied_index() - self.base
        {
            return Err(RpcError::new(
                Code::Unavailable,
                "stale catch-up frame or non-learner destination",
            ));
        }
        self.observed_term = frame.term;
        self.observed_leader = frame.leader_id;
        let passive = self.passive.as_mut().ok_or_else(|| {
            RpcError::corrupt("non-voting replica has no durable catch-up storage")
        })?;
        let from = usize::try_from(frame.from)
            .map_err(|_| RpcError::corrupt("catch-up position exceeds platform bounds"))?;
        if passive.get_decided_idx().map_err(storage_error)? != from {
            return Err(RpcError::corrupt(
                "catch-up does not extend the durable certified prefix",
            ));
        }
        let end = from
            .checked_add(frame.commands.len())
            .ok_or_else(|| RpcError::corrupt("catch-up position exhausted"))?;
        let mut operations = vec![
            StorageOp::AppendEntries(frame.commands),
            StorageOp::SetDecidedIndex(end),
        ];
        if let Some(transition) = frame.transition {
            operations.push(StorageOp::SetStopsign(Some(StopSign::with(
                omnipaxos::ClusterConfig {
                    configuration_id: transition.configuration.configuration_id,
                    nodes: transition.configuration.voters(),
                    flexible_quorum: None,
                },
                Some(serde_json::to_vec(&transition)?),
            ))));
            operations.push(StorageOp::SetDecidedIndex(end.checked_add(1).ok_or_else(
                || RpcError::corrupt("catch-up StopSign position exhausted"),
            )?));
        }
        passive
            .write_atomically(operations)
            .map_err(storage_error)?;
        Ok(())
    }
}

fn storage_error(error: impl std::fmt::Display) -> RpcError {
    RpcError::corrupt(format!("passive protocol storage: {error}"))
}

fn apply_command(store: &mut Store, command: Command, replies: &mut AppliedReplies) -> Result<()> {
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
