use crate::{
    cluster::{Command as LogCommand, Configuration},
    error::{Result, RpcError},
    replica::Replica,
    storage::{
        Store,
        auth::Principal,
        command::{Mutation, Reply},
        lease::{Lease, LeaseCommand},
    },
    wire::etcdserverpb as pb,
};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, SyncSender},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Notify, oneshot};
use tonic::Code;
use uuid::Uuid;

const QUEUE_CAPACITY: usize = 512;
type ReadJob = Box<dyn FnOnce(&Store, &State) + Send>;
type MaintenanceJob = Box<dyn FnOnce(&mut Store, &State) + Send>;

enum Work {
    Read(ReadJob),
    Maintenance(MaintenanceJob),
    Mutate {
        id: Uuid,
        principal: Principal,
        request: Mutation,
        reply: oneshot::Sender<Result<Reply>>,
    },
    Barrier(oneshot::Sender<Result<()>>),
    Peer(omnipaxos::messages::Message<LogCommand>),
    Reconnected(u64),
    Stop,
}

pub struct State {
    pub epoch: u64,
    pub leader_id: Option<u64>,
    pub cluster: Option<Configuration>,
    deadlines: BTreeMap<i64, (u64, Instant)>,
    started: Instant,
    started_ms: i64,
}

impl State {
    pub fn header(&self, store: &Store) -> pb::ResponseHeader {
        pb::ResponseHeader {
            cluster_id: store.identity().cluster_id,
            member_id: store.identity().member_id,
            revision: store.revision(),
            raft_term: self.epoch,
        }
    }

    pub fn remaining_ttl(&self, lease: &Lease) -> Result<i64> {
        let (_, expiry) = self
            .deadlines
            .get(&lease.id)
            .ok_or_else(|| RpcError::corrupt("committed lease has no authoritative timer"))?;
        i64::try_from(expiry.saturating_duration_since(Instant::now()).as_secs())
            .map_err(|_| RpcError::corrupt("lease time exceeds the signed TTL range"))
    }

    pub fn now_ms(&self) -> Result<i64> {
        self.started_ms
            .checked_add(
                i64::try_from(self.started.elapsed().as_millis())
                    .map_err(|_| RpcError::corrupt("monotonic time exceeds signed range"))?,
            )
            .ok_or_else(|| RpcError::corrupt("monotonic wall-time projection exhausted"))
    }

    fn refresh_leases(&mut self, store: &Store) -> Result<()> {
        let leases = store.leases()?;
        let ids: std::collections::BTreeSet<_> = leases.iter().map(|lease| lease.id).collect();
        self.deadlines.retain(|id, _| ids.contains(id));
        for lease in leases {
            if self
                .deadlines
                .get(&lease.id)
                .is_none_or(|(generation, _)| *generation != lease.generation)
            {
                let duration = u64::try_from(lease.ttl)
                    .map_err(|_| RpcError::corrupt("persisted lease TTL is negative"))?;
                let expiry = Instant::now()
                    .checked_add(Duration::from_secs(duration))
                    .ok_or_else(|| {
                        RpcError::corrupt("lease deadline exceeds monotonic clock bounds")
                    })?;
                self.deadlines.insert(lease.id, (lease.generation, expiry));
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct Runtime {
    sender: SyncSender<Work>,
    active: Arc<AtomicBool>,
    pub changes: Arc<Notify>,
    timeout: Duration,
    outbox: Arc<Mutex<Option<Receiver<omnipaxos::messages::Message<LogCommand>>>>>,
}

pub struct Worker {
    sender: SyncSender<Work>,
    thread: thread::JoinHandle<Result<()>>,
}

impl Runtime {
    pub async fn open(
        path: PathBuf,
        client_url: String,
        timeout: Duration,
    ) -> Result<(Self, Worker)> {
        let (sender, receiver) = mpsc::sync_channel(QUEUE_CAPACITY);
        let (ready, readiness) = oneshot::channel();
        let (outgoing, outbox) = mpsc::sync_channel(4);
        let active = Arc::new(AtomicBool::new(false));
        let changes = Arc::new(Notify::new());
        let thread_active = Arc::clone(&active);
        let thread_changes = Arc::clone(&changes);
        let thread = thread::Builder::new().name("etcqlite-writer".into()).spawn(move || {
            let startup: Result<(Store, State, Option<Replica>)> = (|| {
                let mut store = Store::open(path)?;
                let configuration = store.cluster_configuration()?;
                let mut replica = configuration.clone().map(|configuration| Replica::open(store.path(), configuration, store.identity().member_id)).transpose()?;
                if let Some(replica) = &mut replica { replica.apply(&mut store)?; }
                let epoch = store.begin_session(&client_url)?;
                let mut state = State { epoch, leader_id: None, cluster: configuration, deadlines: BTreeMap::new(), started: Instant::now(), started_ms: wall_time_ms()? };
                state.refresh_leases(&store)?;
                Ok((store, state, replica))
            })();
            let (store, state, replica) = match startup {
                Ok(value) => value,
                Err(error) => {
                    if ready.send(Err(error.clone())).is_err() {
                        tracing::error!(error = %error, "writer startup failed after its waiter disconnected");
                    }
                    return Err(error);
                }
            };
            thread_active.store(true, Ordering::Release);
            if ready.send(Ok(())).is_err() {
                thread_active.store(false, Ordering::Release);
                return Err(RpcError::new(Code::Cancelled, "writer startup was canceled"));
            }
            let result = run_writer(store, state, replica, receiver, &thread_changes, outgoing, timeout);
            thread_active.store(false, Ordering::Release);
            thread_changes.notify_waiters();
            if let Err(error) = &result { tracing::error!(error = %error, "writer stopped; node is not ready"); }
            result
        })?;
        readiness
            .await
            .map_err(|_| RpcError::new(Code::Unavailable, "writer exited during startup"))??;
        Ok((
            Self {
                sender: sender.clone(),
                active,
                changes,
                timeout,
                outbox: Arc::new(Mutex::new(Some(outbox))),
            },
            Worker { sender, thread },
        ))
    }

    pub fn is_ready(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    fn send(&self, work: Work) -> Result<()> {
        if !self.is_ready() {
            return Err(RpcError::new(
                Code::Unavailable,
                "etcdserver: server stopped",
            ));
        }
        self.sender.try_send(work).map_err(|error| match error {
            mpsc::TrySendError::Full(_) => {
                RpcError::new(Code::ResourceExhausted, "etcdserver: too many requests")
            }
            mpsc::TrySendError::Disconnected(_) => {
                RpcError::new(Code::Unavailable, "etcdserver: server stopped")
            }
        })
    }

    async fn receive<T>(
        &self,
        receiver: oneshot::Receiver<Result<T>>,
        mutation: bool,
    ) -> Result<T> {
        tokio::time::timeout(self.timeout, receiver)
            .await
            .map_err(|_| {
                RpcError::new(
                    Code::DeadlineExceeded,
                    if mutation {
                        "request deadline exceeded; the mutation may have committed"
                    } else {
                        "request deadline exceeded"
                    },
                )
            })?
            .map_err(|_| {
                RpcError::new(
                    Code::Unavailable,
                    "writer stopped before returning an outcome",
                )
            })?
    }

    pub async fn barrier(&self) -> Result<()> {
        let (sender, receiver) = oneshot::channel();
        self.send(Work::Barrier(sender))?;
        self.receive(receiver, false).await
    }

    pub async fn read<R: Send + 'static>(
        &self,
        function: impl FnOnce(&Store, &State) -> Result<R> + Send + 'static,
    ) -> Result<R> {
        let (sender, receiver) = oneshot::channel();
        self.send(Work::Read(Box::new(move |store, state| {
            if sender.send(function(store, state)).is_err() {
                tracing::debug!("read response abandoned after client cancellation");
            }
        })))?;
        self.receive(receiver, false).await
    }

    pub async fn maintenance<R: Send + 'static>(
        &self,
        function: impl FnOnce(&mut Store, &State) -> Result<R> + Send + 'static,
    ) -> Result<R> {
        let (sender, receiver) = oneshot::channel();
        self.send(Work::Maintenance(Box::new(move |store, state| {
            if sender.send(function(store, state)).is_err() {
                tracing::debug!(
                    "maintenance response abandoned; operation outcome may be uncertain"
                );
            }
        })))?;
        self.receive(receiver, true).await
    }

    pub async fn mutate(&self, principal: Principal, request: Mutation) -> Result<Reply> {
        let (sender, receiver) = oneshot::channel();
        self.send(Work::Mutate {
            id: Uuid::new_v4(),
            principal,
            request,
            reply: sender,
        })?;
        self.receive(receiver, true).await
    }

    pub async fn header(&self) -> Result<pb::ResponseHeader> {
        self.read(|store, state| Ok(state.header(store))).await
    }

    pub fn take_outbox(&self) -> Result<Receiver<omnipaxos::messages::Message<LogCommand>>> {
        self.outbox
            .lock()
            .map_err(|_| RpcError::corrupt("peer outbox state is poisoned"))?
            .take()
            .ok_or_else(|| RpcError::corrupt("peer outbox already belongs to a transport"))
    }

    pub fn deliver_peer(&self, message: omnipaxos::messages::Message<LogCommand>) -> Result<()> {
        self.send(Work::Peer(message))
    }

    pub fn peer_reconnected(&self, pid: u64) -> Result<()> {
        self.send(Work::Reconnected(pid))
    }
}

impl Worker {
    pub async fn shutdown(self) -> Result<()> {
        tokio::task::spawn_blocking(move || {
            match self.sender.send(Work::Stop) {
                Ok(()) => {}
                Err(_) if self.thread.is_finished() => {}
                Err(_) => return Err(RpcError::new(Code::Unavailable, "cannot stop the writer")),
            }
            self.thread
                .join()
                .map_err(|_| RpcError::corrupt("writer panicked; replica must not continue"))?
        })
        .await
        .map_err(|error| RpcError::corrupt(format!("writer shutdown task: {error}")))?
    }
}

struct Pending {
    command: LogCommand,
    response: ResponseWaiter,
    deadline: Instant,
    retried: Instant,
}

enum ResponseWaiter {
    Mutation(oneshot::Sender<Result<Reply>>),
    Barrier(oneshot::Sender<Result<()>>),
}

fn run_writer(
    mut store: Store,
    mut state: State,
    mut replica: Option<Replica>,
    receiver: Receiver<Work>,
    changes: &Notify,
    outgoing: SyncSender<omnipaxos::messages::Message<LogCommand>>,
    timeout: Duration,
) -> Result<()> {
    let mut pending = BTreeMap::<Uuid, Pending>::new();
    let mut expiry_requests = BTreeMap::<i64, (u64, Uuid, Instant)>::new();
    let mut was_leader = false;
    loop {
        match receiver.recv_timeout(Duration::from_millis(20)) {
            Ok(Work::Read(job)) => job(&store, &state),
            Ok(Work::Maintenance(job)) => job(&mut store, &state),
            Ok(Work::Barrier(reply)) => {
                if let Some(replica) = &mut replica {
                    let id = Uuid::new_v4();
                    let command = LogCommand::Barrier { id };
                    replica.propose(command.clone())?;
                    pending.insert(
                        id,
                        Pending {
                            command,
                            response: ResponseWaiter::Barrier(reply),
                            deadline: Instant::now() + timeout,
                            retried: Instant::now(),
                        },
                    );
                } else {
                    let result = store.advance_barrier();
                    if reply.send(result).is_err() {
                        tracing::debug!("barrier waiter canceled");
                    }
                }
            }
            Ok(Work::Mutate {
                id,
                principal,
                request,
                reply,
            }) => {
                if let Some(replica) = &mut replica {
                    let command = LogCommand::Mutation {
                        id,
                        principal,
                        request,
                    };
                    replica.propose(command.clone())?;
                    pending.insert(
                        id,
                        Pending {
                            command,
                            response: ResponseWaiter::Mutation(reply),
                            deadline: Instant::now() + timeout,
                            retried: Instant::now(),
                        },
                    );
                } else {
                    let before = store.revision();
                    let kind = request.kind();
                    let started = Instant::now();
                    let mut result =
                        store.command(store.applied_index() + 1, id, principal, request);
                    if let Ok(reply) = &mut result
                        && let Some(header) = reply.header_mut()
                    {
                        let physical = state.header(&store);
                        header.cluster_id = physical.cluster_id;
                        header.member_id = physical.member_id;
                        header.raft_term = physical.raft_term;
                    }
                    if let Err(error) = &result {
                        tracing::warn!(request_id = %id, operation = kind, error = %error, "operation failed");
                    }
                    tracing::debug!(request_id = %id, operation = kind, duration_us = started.elapsed().as_micros(), "operation completed");
                    if !store.is_healthy() {
                        let error = result
                            .as_ref()
                            .err()
                            .cloned()
                            .unwrap_or_else(|| RpcError::corrupt("application storage failed"));
                        if reply.send(result).is_err() {
                            tracing::debug!(request_id = %id, "failed mutation waiter canceled");
                        }
                        return Err(error);
                    }
                    state.refresh_leases(&store)?;
                    if before != store.revision() {
                        changes.notify_waiters();
                    }
                    if reply.send(result).is_err() {
                        tracing::debug!(request_id = %id, "mutation response abandoned; outcome is uncertain");
                    }
                }
            }
            Ok(Work::Stop) | Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
            Ok(Work::Peer(message)) => {
                let replica = replica
                    .as_mut()
                    .ok_or_else(|| RpcError::corrupt("peer packet sent to local mode"))?;
                replica.receive(message)?;
            }
            Ok(Work::Reconnected(pid)) => {
                if let Some(replica) = &mut replica {
                    replica.reconnected(pid)?;
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
        if !store.is_healthy() {
            return Err(RpcError::corrupt(
                "application storage failed; refusing further authority",
            ));
        }
        if let Some(replica) = &mut replica {
            replica.tick()?;
            state.epoch = replica.term();
            state.leader_id = replica.leader();
            let leader = replica.is_leader();
            if leader && !was_leader {
                state.deadlines.clear();
                expiry_requests.clear();
                state.refresh_leases(&store)?;
            }
            was_leader = leader;
            let before = store.revision();
            for (id, mut result) in replica.apply(&mut store)? {
                if let Some(waiter) = pending.remove(&id) {
                    if let Ok(Some(reply)) = &mut result
                        && let Some(header) = reply.header_mut()
                    {
                        let physical = state.header(&store);
                        header.cluster_id = physical.cluster_id;
                        header.member_id = physical.member_id;
                        header.raft_term = physical.raft_term;
                    }
                    match waiter.response {
                        ResponseWaiter::Mutation(sender) => {
                            let response = result.and_then(|reply| {
                                reply.ok_or_else(|| {
                                    RpcError::corrupt("mutation decided as a barrier")
                                })
                            });
                            if sender.send(response).is_err() {
                                tracing::debug!(request_id = %id, "replicated mutation response abandoned");
                            }
                        }
                        ResponseWaiter::Barrier(sender) => {
                            if sender.send(result.map(|_| ())).is_err() {
                                tracing::debug!(request_id = %id, "replicated barrier waiter canceled");
                            }
                        }
                    }
                }
            }
            state.refresh_leases(&store)?;
            if before != store.revision() {
                changes.notify_waiters();
            }
            let expired: Vec<_> = pending
                .iter()
                .filter(|(_, pending)| Instant::now() >= pending.deadline)
                .map(|(id, _)| *id)
                .collect();
            for id in expired {
                if let Some(expired) = pending.remove(&id) {
                    match expired.response {
                        ResponseWaiter::Barrier(sender) => {
                            if sender
                                .send(Err(RpcError::new(
                                    Code::Unavailable,
                                    "no quorum decision for the requested read barrier",
                                )))
                                .is_err()
                            {
                                tracing::debug!(request_id = %id, "expired read barrier waiter canceled");
                            }
                        }
                        ResponseWaiter::Mutation(sender) => {
                            if sender
                                .send(Err(RpcError::new(
                                    Code::DeadlineExceeded,
                                    "quorum decision timed out; mutation outcome is uncertain",
                                )))
                                .is_err()
                            {
                                tracing::debug!(request_id = %id, "expired mutation waiter canceled");
                            }
                        }
                    }
                }
            }
            for pending in pending.values_mut() {
                if pending.retried.elapsed() >= Duration::from_millis(250) {
                    replica.propose(pending.command.clone())?;
                    pending.retried = Instant::now();
                }
            }
            for message in replica.outgoing()? {
                match outgoing.try_send(message) {
                    Ok(()) => {}
                    Err(mpsc::TrySendError::Full(_)) => tracing::warn!(
                        "peer outbox overloaded; protocol retransmission is required"
                    ),
                    Err(mpsc::TrySendError::Disconnected(_)) => {
                        return Err(RpcError::new(Code::Unavailable, "peer transport stopped"));
                    }
                }
            }
            if !leader {
                continue;
            }
        } else {
            state.leader_id = Some(store.identity().member_id);
        }
        let expired: Vec<_> = state
            .deadlines
            .iter()
            .filter(|(_, (_, deadline))| *deadline <= Instant::now())
            .map(|(id, (generation, _))| (*id, *generation))
            .collect();
        for (id, generation) in expired {
            if let Some(replica) = &mut replica {
                let retry = expiry_requests
                    .get(&id)
                    .is_none_or(|(current, _, retried)| {
                        *current != generation || retried.elapsed() >= Duration::from_millis(250)
                    });
                if retry {
                    let request_id = expiry_requests
                        .get(&id)
                        .filter(|(current, _, _)| *current == generation)
                        .map_or_else(Uuid::new_v4, |(_, request, _)| *request);
                    replica.propose(LogCommand::Mutation {
                        id: request_id,
                        principal: Principal::Internal,
                        request: Mutation::Lease(LeaseCommand::Expire { id, generation }),
                    })?;
                    expiry_requests.insert(id, (generation, request_id, Instant::now()));
                }
                continue;
            }
            let before = store.revision();
            store.command(
                store.applied_index() + 1,
                Uuid::new_v4(),
                Principal::Internal,
                Mutation::Lease(LeaseCommand::Expire { id, generation }),
            )?;
            state.refresh_leases(&store)?;
            if before != store.revision() {
                changes.notify_waiters();
            }
        }
        expiry_requests.retain(|id, (generation, _, _)| {
            state
                .deadlines
                .get(id)
                .is_some_and(|(current, _)| current == generation)
        });
    }
}

pub fn wall_time_ms() -> Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| RpcError::corrupt(format!("system time before Unix epoch: {error}")))?;
    i64::try_from(duration.as_millis())
        .map_err(|_| RpcError::corrupt("system time exceeds signed range"))
}
