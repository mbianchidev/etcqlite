use crate::{
    cluster::{Catchup, Configuration, Member, Outgoing, PeerMessage},
    error::{Result, RpcError},
    runtime::Runtime,
    storage::{
        auth::Principal,
        snapshot::{NativeSnapshot, inspect},
    },
    wire::peer as pb,
};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{Semaphore, oneshot},
};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{
    Code, Request, Response, Status,
    transport::{
        Certificate, ClientTlsConfig, Endpoint, Identity, Server, ServerTlsConfig,
        server::{TcpConnectInfo, TlsConnectInfo},
    },
};
use uuid::Uuid;

const CHUNK_BYTES: usize = 1 << 20;
const MAX_PACKET_BYTES: usize = 256 << 20;
const MAX_EXPANDED_BYTES: u64 = 8 << 30;
const INLINE_BYTES: usize = 64 << 10;
const MAX_CATCHUP_BYTES: usize = 16 << 20;
const MAX_NATIVE_BYTES: u64 = 16 << 30;
const OUTBOUND_BUDGET_UNITS: usize = MAX_PACKET_BYTES / 1024;

#[derive(Clone, Debug)]
pub struct Options {
    pub address: SocketAddr,
    pub certificate: PathBuf,
    pub key: PathBuf,
    pub ca: PathBuf,
}

pub struct RunningPeer {
    stop: oneshot::Sender<()>,
    server: tokio::task::JoinHandle<Result<()>>,
    bridge: tokio::task::JoinHandle<()>,
    manager: tokio::task::JoinHandle<Result<()>>,
    poller: tokio::task::JoinHandle<()>,
}

#[derive(Clone)]
struct Service {
    cluster_id: u64,
    member_id: u64,
    runtime: Runtime,
    snapshot_slots: Arc<Semaphore>,
    packet_slots: Arc<Semaphore>,
    bulk_slots: Arc<Semaphore>,
}

struct Outbound {
    payload: Arc<EncodedMessage>,
    _budget: tokio::sync::OwnedSemaphorePermit,
}

enum EncodedData {
    Inline(Vec<u8>),
    File(tempfile::NamedTempFile),
}

struct EncodedMessage {
    data: EncodedData,
    bytes: u64,
    checksum: Vec<u8>,
}

struct Spool {
    data: EncodedData,
    bytes: u64,
    hash: Sha256,
    exceeded: bool,
}

impl std::io::Write for Spool {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        if self.bytes + buffer.len() as u64 > MAX_PACKET_BYTES as u64 {
            self.exceeded = true;
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "compressed peer packet exceeds 256 MiB",
            ));
        }
        if let EncodedData::Inline(data) = &self.data
            && data.len() + buffer.len() > INLINE_BYTES
        {
            let mut file = tempfile::NamedTempFile::new()?;
            file.write_all(data)?;
            self.data = EncodedData::File(file);
        }
        match &mut self.data {
            EncodedData::Inline(data) => data.extend_from_slice(buffer),
            EncodedData::File(file) => file.write_all(buffer)?,
        }
        self.bytes += buffer.len() as u64;
        self.hash.update(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match &mut self.data {
            EncodedData::Inline(_) => Ok(()),
            EncodedData::File(file) => file.flush(),
        }
    }
}

#[derive(Clone)]
struct Credentials {
    identity: Identity,
    ca: Vec<u8>,
    incarnation: String,
}

fn spool_message(message: PeerMessage) -> Result<EncodedMessage> {
    let spool = Spool {
        data: EncodedData::Inline(Vec::new()),
        bytes: 0,
        hash: Sha256::new(),
        exceeded: false,
    };
    let mut encoder = flate2::write::GzEncoder::new(spool, flate2::Compression::fast());
    if let Err(error) = serde_json::to_writer(&mut encoder, &message) {
        if encoder.get_ref().exceeded {
            return Err(RpcError::new(
                Code::ResourceExhausted,
                "compressed peer packet exceeds 256 MiB",
            ));
        }
        return Err(error.into());
    }
    if let Err(error) = encoder.try_finish() {
        if encoder.get_ref().exceeded {
            return Err(RpcError::new(
                Code::ResourceExhausted,
                "compressed peer packet exceeds 256 MiB",
            ));
        }
        return Err(error.into());
    }
    let spool = encoder.finish()?;
    Ok(EncodedMessage {
        data: spool.data,
        bytes: spool.bytes,
        checksum: spool.hash.finalize().to_vec(),
    })
}

fn decode_message(data: &[u8], encoding: &str) -> Result<PeerMessage> {
    use std::io::Read;
    match encoding {
        "gzip-json-v1" => {
            let mut decoder = flate2::bufread::GzDecoder::new(std::io::Cursor::new(data));
            let mut bounded = (&mut decoder).take(MAX_EXPANDED_BYTES + 1);
            let message = serde_json::from_reader(&mut bounded)
                .map_err(|error| RpcError::corrupt(format!("compressed peer payload: {error}")))?;
            if bounded.limit() == 0 {
                return Err(RpcError::new(
                    Code::ResourceExhausted,
                    "expanded peer packet exceeds 8 GiB",
                ));
            }
            if decoder.get_ref().position() != data.len() as u64 {
                return Err(RpcError::invalid(
                    "compressed peer packet contains trailing data",
                ));
            }
            Ok(message)
        }
        "" if data.len() <= MAX_CATCHUP_BYTES => serde_json::from_slice(data)
            .map_err(|error| RpcError::corrupt(format!("legacy peer payload: {error}"))),
        _ => Err(RpcError::invalid(
            "unsupported peer encoding or legacy transfer limit",
        )),
    }
}

pub async fn start(
    configuration: Configuration,
    member_id: u64,
    runtime: Runtime,
    options: Options,
) -> Result<RunningPeer> {
    configuration.validate()?;
    let own = configuration.member(member_id)?;
    let incarnation = runtime
        .read(|store, _| {
            Ok(store
                .incarnation()?
                .map(|id| id.to_string())
                .unwrap_or_default())
        })
        .await?;
    let address = own
        .peer_url
        .parse::<axum::http::Uri>()
        .map_err(|_| RpcError::invalid("persisted peer URL is invalid"))?;
    if address.port_u16() != Some(options.address.port()) {
        return Err(RpcError::new(
            Code::FailedPrecondition,
            "peer listener does not match the persisted endpoint",
        ));
    }
    crate::server::private_key_permissions(&options.key)?;
    let identity = Identity::from_pem(
        tokio::fs::read(&options.certificate).await?,
        tokio::fs::read(&options.key).await?,
    );
    let ca = tokio::fs::read(&options.ca).await?;
    let listener = tokio::net::TcpListener::bind(options.address).await?;
    let service = Service {
        cluster_id: configuration.cluster_id,
        member_id,
        runtime: runtime.clone(),
        snapshot_slots: Arc::new(Semaphore::new(2)),
        packet_slots: Arc::new(Semaphore::new(16)),
        bulk_slots: Arc::new(Semaphore::new(1)),
    };
    let (stop, stopped) = oneshot::channel();
    let server = Server::builder()
        .tls_config(
            ServerTlsConfig::new()
                .identity(identity.clone())
                .client_ca_root(Certificate::from_pem(ca.clone())),
        )
        .map_err(|error| RpcError::new(Code::FailedPrecondition, format!("peer TLS: {error}")))?
        .concurrency_limit_per_connection(16)
        .add_service(
            pb::peer_server::PeerServer::new(service)
                .max_decoding_message_size(CHUNK_BYTES + 4096)
                .max_encoding_message_size(MAX_CATCHUP_BYTES + 4096),
        );
    let server = tokio::spawn(async move {
        server
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = stopped.await;
            })
            .await
            .map_err(|error| RpcError::new(Code::Unavailable, format!("peer listener: {error}")))
    });
    let outbox = runtime.take_outbox()?;
    let (sender, mut receiver) = tokio::sync::mpsc::channel::<Outgoing>(4);
    let bridge = tokio::task::spawn_blocking(move || {
        while let Ok(packet) = outbox.recv() {
            if sender.blocking_send(packet).is_err() {
                break;
            }
        }
    });
    let relay_runtime = runtime.clone();
    let credentials = Credentials {
        identity,
        ca,
        incarnation,
    };
    let manager = tokio::spawn(async move {
        let budget = Arc::new(Semaphore::new(OUTBOUND_BUDGET_UNITS));
        let mut relays =
            BTreeMap::<(u32, u64, Vec<String>), tokio::sync::mpsc::Sender<Outbound>>::new();
        let mut tasks = Vec::new();
        while let Some(packet) = receiver.recv().await {
            let target = packet.configuration.member(packet.receiver)?.clone();
            let urls = target.peer_urls().map(str::to_owned).collect::<Vec<_>>();
            let key = (packet.configuration.configuration_id, target.id, urls);
            let sender = relays.entry(key).or_insert_with(|| {
                let (sender, receiver) = tokio::sync::mpsc::channel(32);
                tasks.push(tokio::spawn(relay(
                    packet.configuration.clone(),
                    member_id,
                    target,
                    relay_runtime.clone(),
                    credentials.clone(),
                    receiver,
                )));
                sender
            });
            let spool = match tokio::task::spawn_blocking(move || spool_message(packet.message))
                .await
                .map_err(|error| RpcError::corrupt(format!("peer spool task: {error}")))?
            {
                Ok(spool) => spool,
                Err(error) if error.status().code() == Code::ResourceExhausted => {
                    tracing::error!(peer_id = packet.receiver, error = %error, "peer packet cannot be transferred");
                    continue;
                }
                Err(error) => return Err(error),
            };
            let units = u32::try_from(spool.bytes.div_ceil(1024).max(1))
                .map_err(|_| RpcError::corrupt("peer budget exceeds supported range"))?;
            let permit = match budget.clone().try_acquire_many_owned(units) {
                Ok(permit) => permit,
                Err(_) => {
                    tracing::warn!("peer byte budget exhausted; retransmission is required");
                    continue;
                }
            };
            if sender
                .try_send(Outbound {
                    payload: Arc::new(spool),
                    _budget: permit,
                })
                .is_err()
            {
                tracing::warn!(
                    peer_id = packet.receiver,
                    "peer relay overloaded or stopped; retransmission is required"
                );
            }
        }
        drop(relays);
        for task in tasks {
            task.await
                .map_err(|error| RpcError::corrupt(format!("peer relay task: {error}")))??;
        }
        Ok(())
    });
    let poll_runtime = runtime.clone();
    let poller = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        while poll_runtime.is_ready() {
            interval.tick().await;
            let request = poll_runtime
                .replica_read(|store, state, replica| {
                    let configuration = state
                        .cluster
                        .clone()
                        .ok_or_else(|| RpcError::corrupt("peer runtime lost its cluster"))?;
                    if !replica.non_voting() {
                        return Ok(None);
                    }
                    Ok(Some((
                        configuration,
                        store.identity().member_id,
                        store.applied_index() - store.configuration_base(),
                        store
                            .incarnation()?
                            .map(|id| id.to_string())
                            .unwrap_or_default(),
                    )))
                })
                .await;
            let (configuration, member, from, incarnation) = match request {
                Ok(Some(request)) => request,
                Ok(None) => continue,
                Err(error) => {
                    tracing::debug!(error = %error, "learner poll stopped or unavailable");
                    continue;
                }
            };
            for target in configuration
                .members
                .iter()
                .filter(|target| !target.learner)
            {
                let frame = async {
                    let mut client =
                        connect(&target.peer_url, &options, Duration::from_secs(3)).await?;
                    let mut peer = query(&configuration, member, target.id)?;
                    peer.incarnation = incarnation.clone();
                    let response = client
                        .fetch_decided(pb::CatchupRequest {
                            peer: Some(peer),
                            configuration_id: configuration.configuration_id,
                            from,
                        })
                        .await
                        .map_err(rpc_error)?
                        .into_inner();
                    let frame: Catchup = serde_json::from_slice(&response.frame).map_err(|_| {
                        RpcError::new(Code::DataLoss, "learner catch-up frame is invalid")
                    })?;
                    poll_runtime.install_catchup(frame).await
                }
                .await;
                match frame {
                    Ok(()) => break,
                    Err(error) => {
                        tracing::debug!(peer_id = target.id, error = %error, "learner source unavailable; trying another voter")
                    }
                }
            }
        }
    });
    Ok(RunningPeer {
        stop,
        server,
        bridge,
        manager,
        poller,
    })
}

async fn relay(
    configuration: Configuration,
    member_id: u64,
    target: Member,
    runtime: Runtime,
    credentials: Credentials,
    mut receiver: tokio::sync::mpsc::Receiver<Outbound>,
) -> Result<()> {
    let mut clients = Vec::new();
    for url in target.peer_urls() {
        let endpoint = Endpoint::from_shared(url.to_owned())
            .map_err(|_| RpcError::invalid("peer endpoint is invalid"))?
            .tls_config(
                ClientTlsConfig::new()
                    .identity(credentials.identity.clone())
                    .ca_certificate(Certificate::from_pem(credentials.ca.clone())),
            )
            .map_err(|error| {
                RpcError::new(
                    Code::FailedPrecondition,
                    format!("peer client TLS: {error}"),
                )
            })?
            .connect_timeout(Duration::from_secs(1))
            .timeout(Duration::from_secs(60));
        clients.push(pb::peer_client::PeerClient::new(endpoint.connect_lazy()));
    }
    let mut disconnected = false;
    while let Some(outbound) = receiver.recv().await {
        let mut delivered = false;
        for client in &mut clients {
            let id = Uuid::new_v4();
            let checksum = outbound.payload.checksum.clone();
            let payload = Arc::clone(&outbound.payload);
            let size = payload.bytes;
            let cluster_id = configuration.cluster_id;
            let configuration_id = configuration.configuration_id;
            let bootstrap_digest = configuration.bootstrap_digest.clone().unwrap_or_default();
            let incarnation = credentials.incarnation.clone();
            let target_id = target.id;
            let mut input = match &payload.data {
                EncodedData::Inline(_) => None,
                EncodedData::File(file) => Some(tokio::fs::File::open(file.path()).await?),
            };
            let (chunks, stream) = tokio::sync::mpsc::channel(2);
            let sending = tokio::spawn(async move {
                let mut offset = 0;
                while offset < size {
                    let mut data =
                        vec![
                            0;
                            usize::try_from((size - offset).min(CHUNK_BYTES as u64))
                                .map_err(|_| RpcError::corrupt("peer chunk size overflow"))?
                        ];
                    match (&payload.data, &mut input) {
                        (EncodedData::Inline(bytes), _) => {
                            let start = offset as usize;
                            let count = data.len();
                            data.copy_from_slice(&bytes[start..start + count]);
                        }
                        (EncodedData::File(_), Some(input)) => {
                            input.read_exact(&mut data).await?;
                        }
                        _ => return Err(RpcError::corrupt("peer spool reader is unavailable")),
                    }
                    let count = data.len() as u64;
                    let chunk = pb::PacketChunk {
                        cluster_id,
                        configuration_id,
                        sender_id: member_id,
                        receiver_id: target_id,
                        message_id: id.as_bytes().to_vec(),
                        total_bytes: size,
                        offset,
                        checksum: checksum.clone(),
                        data,
                        bootstrap_digest: bootstrap_digest.clone(),
                        incarnation: incarnation.clone(),
                        encoding: "gzip-json-v1".into(),
                    };
                    if chunks.send(chunk).await.is_err() {
                        return Ok(());
                    }
                    offset += count;
                }
                Ok::<(), RpcError>(())
            });
            let response = client.exchange(ReceiverStream::new(stream)).await;
            sending
                .await
                .map_err(|error| RpcError::corrupt(format!("peer stream task: {error}")))??;
            match response {
                Ok(receipt) if receipt.get_ref().message_id == id.as_bytes() => {
                    delivered = true;
                    break;
                }
                Ok(_) => return Err(RpcError::corrupt("peer receipt identity mismatch")),
                Err(error) => {
                    tracing::debug!(peer_id = target.id, code = ?error.code(), "peer endpoint delivery failed")
                }
            }
        }
        if delivered
            && disconnected
            && let Err(error) = runtime.peer_reconnected(configuration.configuration_id, target.pid)
        {
            match error.status().code() {
                Code::Unavailable => {
                    tracing::debug!(error = %error, "peer reconnection completed after writer admission stopped");
                    return Ok(());
                }
                Code::ResourceExhausted => {
                    tracing::warn!(error = %error, "peer reconnection notification is backpressured; retrying on the next delivery");
                    continue;
                }
                _ => return Err(error),
            }
        }
        disconnected = !delivered;
        if !delivered {
            tracing::warn!(
                peer_id = target.id,
                "peer delivery failed; durable retransmission is required"
            );
        }
    }
    Ok(())
}

impl RunningPeer {
    pub async fn shutdown(self) -> Result<()> {
        if self.stop.send(()).is_err() && !self.server.is_finished() {
            return Err(RpcError::new(
                Code::Unavailable,
                "peer shutdown channel failed",
            ));
        }
        self.server
            .await
            .map_err(|error| RpcError::corrupt(format!("peer server task: {error}")))??;
        self.bridge
            .await
            .map_err(|error| RpcError::corrupt(format!("peer bridge task: {error}")))?;
        self.manager
            .await
            .map_err(|error| RpcError::corrupt(format!("peer manager task: {error}")))??;
        self.poller
            .await
            .map_err(|error| RpcError::corrupt(format!("learner poll task: {error}")))?;
        Ok(())
    }
}

impl Service {
    async fn authorize(
        &self,
        request: &pb::PeerQuery,
        certificate: &[u8],
        configuration_id: Option<u32>,
    ) -> std::result::Result<Configuration, Status> {
        if request.cluster_id != self.cluster_id || request.receiver_id != self.member_id {
            return Err(Status::permission_denied(
                "peer request has a different authority or destination",
            ));
        }
        validate_certificate(certificate, self.cluster_id, request.requester_id)?;
        let requester = request.requester_id;
        let bootstrap_digest = request.bootstrap_digest.clone();
        let incarnation = request.incarnation.clone();
        self.runtime
            .read(move |store, _| {
                let configuration = match configuration_id {
                    Some(id) => store.configuration_at(id)?,
                    None => store
                        .cluster_configuration()?
                        .ok_or_else(|| RpcError::corrupt("peer runtime has no cluster"))?,
                };
                let member = configuration.member(requester)?;
                if member
                    .incarnation
                    .is_some_and(|expected| expected.to_string() != incarnation)
                {
                    return Err(RpcError::new(
                        Code::PermissionDenied,
                        "peer member incarnation is not the committed owner",
                    ));
                }
                if configuration.bootstrap_digest.as_deref().unwrap_or("") != bootstrap_digest {
                    return Err(RpcError::new(
                        Code::PermissionDenied,
                        "peer bootstrap checkpoint differs from this cluster's verified genesis",
                    ));
                }
                Ok(configuration)
            })
            .await
            .map_err(|error| error.status())
    }
}

#[tonic::async_trait]
impl pb::peer_server::Peer for Service {
    async fn exchange(
        &self,
        request: Request<tonic::Streaming<pb::PacketChunk>>,
    ) -> std::result::Result<Response<pb::PacketReceipt>, Status> {
        let permit = self
            .packet_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("peer transfers are at capacity"))?;
        let certificate = peer_certificate(&request)?;
        let mut stream = request.into_inner();
        let first = stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("peer transfer is empty"))?;
        let configuration = self
            .authorize(
                &pb::PeerQuery {
                    cluster_id: first.cluster_id,
                    requester_id: first.sender_id,
                    receiver_id: first.receiver_id,
                    bootstrap_digest: first.bootstrap_digest.clone(),
                    incarnation: first.incarnation.clone(),
                },
                &certificate,
                Some(first.configuration_id),
            )
            .await?;
        let total = usize::try_from(first.total_bytes)
            .map_err(|_| Status::resource_exhausted("peer transfer exceeds platform bounds"))?;
        if total == 0
            || total > MAX_PACKET_BYTES
            || first.message_id.len() != 16
            || first.checksum.len() != 32
            || first.offset != 0
        {
            return Err(Status::invalid_argument(
                "peer transfer metadata is invalid",
            ));
        }
        if first.encoding != "gzip-json-v1"
            && !(first.encoding.is_empty() && total <= MAX_CATCHUP_BYTES)
        {
            return Err(Status::invalid_argument(
                "unsupported peer encoding or legacy transfer limit",
            ));
        }
        let mut admission = vec![permit];
        if total > INLINE_BYTES {
            admission.push(
                self.bulk_slots.clone().try_acquire_owned().map_err(|_| {
                    Status::resource_exhausted("bulk peer transfers are at capacity")
                })?,
            );
        }
        let mut data = Vec::new();
        append_chunk(&mut data, &first, total)?;
        while let Some(chunk) = stream.message().await? {
            validate_chunk(&first, &chunk)?;
            append_chunk(&mut data, &chunk, total)?;
        }
        if data.len() != total || Sha256::digest(&data).as_slice() != first.checksum {
            return Err(Status::data_loss("peer transfer is truncated or corrupt"));
        }
        let encoding = first.encoding.clone();
        let message = tokio::task::spawn_blocking(move || decode_message(&data, &encoding))
            .await
            .map_err(|error| Status::internal(format!("peer decoding task: {error}")))?
            .map_err(|error| error.status())?;
        let sender = configuration
            .member(first.sender_id)
            .map_err(|error| error.status())?;
        let registered = sender.incarnation.is_some()
            || (first.incarnation.is_empty()
                && (configuration.configuration_id == 1 || !sender.name.is_empty()));
        if !registered {
            let valid_claim = matches!(&message, PeerMessage::Proposal(crate::cluster::Command::Mutation {
                principal: Principal::Internal, request: crate::storage::command::Mutation::Membership(
                    crate::storage::membership::MembershipCommand::Publish { id, incarnation: Some(incarnation), .. }), ..
            }) if *id == first.sender_id && incarnation.to_string() == first.incarnation);
            if !valid_claim {
                return Err(Status::permission_denied(
                    "unclaimed member may only register its own durable incarnation",
                ));
            }
        }
        if let PeerMessage::Protocol(message) = &message {
            let sender = configuration
                .member(first.sender_id)
                .map_err(|error| error.status())?;
            let receiver = configuration
                .member(self.member_id)
                .map_err(|error| error.status())?;
            if !registered
                || sender.learner
                || receiver.learner
                || message.get_sender() != sender.pid
                || message.get_receiver() != receiver.pid
            {
                return Err(Status::permission_denied(
                    "protocol identity is not authenticated voting membership",
                ));
            }
        }
        self.runtime
            .deliver_admitted_peer(first.configuration_id, first.sender_id, message, admission)
            .map_err(|error| error.status())?;
        Ok(Response::new(pb::PacketReceipt {
            message_id: first.message_id,
        }))
    }

    async fn fetch_decided(
        &self,
        request: Request<pb::CatchupRequest>,
    ) -> std::result::Result<Response<pb::CatchupResponse>, Status> {
        let certificate = peer_certificate(&request)?;
        let request = request.into_inner();
        let peer = request
            .peer
            .ok_or_else(|| Status::invalid_argument("catch-up has no peer identity"))?;
        self.authorize(&peer, &certificate, Some(request.configuration_id))
            .await?;
        let mut frame = self
            .runtime
            .replica_read(move |_, _, replica| {
                replica.catchup(request.configuration_id, request.from)
            })
            .await
            .map_err(|error| error.status())?;
        let mut bytes =
            serde_json::to_vec(&frame).map_err(|_| Status::internal("catch-up encoding failed"))?;
        while bytes.len() > MAX_CATCHUP_BYTES && !frame.commands.is_empty() {
            frame.commands.pop();
            frame.transition = None;
            bytes = serde_json::to_vec(&frame)
                .map_err(|_| Status::internal("catch-up encoding failed"))?;
        }
        if bytes.len() > MAX_CATCHUP_BYTES
            || (frame.commands.is_empty()
                && frame.transition.is_none()
                && bytes.len() > CHUNK_BYTES)
        {
            return Err(Status::resource_exhausted(
                "catch-up entry exceeds the bounded transfer limit",
            ));
        }
        Ok(Response::new(pb::CatchupResponse { frame: bytes }))
    }

    async fn progress(
        &self,
        request: Request<pb::PeerQuery>,
    ) -> std::result::Result<Response<pb::PeerProgress>, Status> {
        let certificate = peer_certificate(&request)?;
        self.authorize(request.get_ref(), &certificate, None)
            .await?;
        let response = self
            .runtime
            .replica_read(|store, _, replica| {
                Ok(pb::PeerProgress {
                    configuration_id: replica.configuration.configuration_id,
                    applied_index: store.applied_index(),
                    learner: replica.learner(),
                })
            })
            .await
            .map_err(|error| error.status())?;
        Ok(Response::new(response))
    }

    type SnapshotStream = crate::server::ResponseStream<pb::PacketChunk>;
    async fn snapshot(
        &self,
        request: Request<pb::PeerQuery>,
    ) -> std::result::Result<Response<Self::SnapshotStream>, Status> {
        let permit = self
            .snapshot_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("peer snapshots are at capacity"))?;
        let certificate = peer_certificate(&request)?;
        self.authorize(request.get_ref(), &certificate, None)
            .await?;
        let receiver_id = request.get_ref().requester_id;
        self.runtime
            .barrier()
            .await
            .map_err(|error| error.status())?;
        let (snapshot, _) = self
            .runtime
            .native_snapshot(Principal::Internal)
            .await
            .map_err(|error| error.status())?;
        let configuration = snapshot.configuration.clone().ok_or_else(|| {
            Status::data_loss("native snapshot lost its captured cluster identity")
        })?;
        let (sender, receiver) = tokio::sync::mpsc::channel(4);
        let member_id = self.member_id;
        tokio::spawn(async move {
            let _permit = permit;
            let result: std::result::Result<(), Status> = async {
                let mut file = tokio::fs::File::open(snapshot.path())
                    .await
                    .map_err(|error| Status::internal(format!("snapshot open: {error}")))?;
                let total = snapshot.info.bytes;
                let id = Uuid::new_v4();
                let checksum = file_digest(snapshot.path())
                    .await
                    .map_err(|error| error.status())?;
                let mut offset = 0;
                while offset < total {
                    let mut data =
                        vec![
                            0;
                            usize::try_from((total - offset).min(CHUNK_BYTES as u64))
                                .map_err(|_| Status::internal("snapshot chunk overflow"))?
                        ];
                    file.read_exact(&mut data)
                        .await
                        .map_err(|error| Status::internal(format!("snapshot read: {error}")))?;
                    let count = data.len() as u64;
                    if sender
                        .send(Ok(pb::PacketChunk {
                            cluster_id: configuration.cluster_id,
                            configuration_id: configuration.configuration_id,
                            sender_id: member_id,
                            receiver_id,
                            message_id: id.as_bytes().to_vec(),
                            total_bytes: total,
                            offset,
                            checksum: checksum.clone(),
                            data,
                            bootstrap_digest: configuration
                                .bootstrap_digest
                                .clone()
                                .unwrap_or_default(),
                            incarnation: configuration
                                .member(member_id)
                                .map_err(|error| error.status())?
                                .incarnation
                                .map(|id| id.to_string())
                                .unwrap_or_default(),
                            encoding: "native-snapshot-v1".into(),
                        }))
                        .await
                        .is_err()
                    {
                        return Ok(());
                    }
                    offset += count;
                }
                Ok(())
            }
            .await;
            if let Err(error) = result
                && sender.send(Err(error)).await.is_err()
            {
                tracing::debug!("native snapshot error waiter canceled");
            }
        });
        Ok(Response::new(
            Box::pin(ReceiverStream::new(receiver)) as Self::SnapshotStream
        ))
    }

    async fn transfer_leader(
        &self,
        request: Request<pb::LeadershipRequest>,
    ) -> std::result::Result<Response<pb::LeadershipResponse>, Status> {
        let certificate = peer_certificate(&request)?;
        let request = request.into_inner();
        self.authorize(
            &pb::PeerQuery {
                cluster_id: request.cluster_id,
                requester_id: request.requester_id,
                receiver_id: request.receiver_id,
                bootstrap_digest: request.bootstrap_digest.clone(),
                incarnation: request.incarnation.clone(),
            },
            &certificate,
            None,
        )
        .await?;
        let principal: Principal = serde_json::from_slice(&request.principal)
            .map_err(|_| Status::unauthenticated("forwarded principal is invalid"))?;
        self.runtime
            .read(move |store, _| store.require_admin(&principal))
            .await
            .map_err(|error| error.status())?;
        self.runtime
            .attempt_leadership()
            .await
            .map_err(|error| error.status())?;
        let member = self.member_id;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            self.runtime
                .barrier()
                .await
                .map_err(|error| error.status())?;
            let response = self
                .runtime
                .read(move |store, state| {
                    Ok(
                        (state.leader_id == Some(member)).then_some(pb::LeadershipResponse {
                            leader_id: member,
                            term: state.epoch,
                            applied_index: store.applied_index(),
                        }),
                    )
                })
                .await
                .map_err(|error| error.status())?;
            if let Some(response) = response {
                return Ok(Response::new(response));
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(Status::deadline_exceeded(
                    "target did not acquire quorum leadership",
                ));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn lease_time_to_live(
        &self,
        request: Request<pb::LeaseQuery>,
    ) -> std::result::Result<Response<pb::LeaseState>, Status> {
        let certificate = peer_certificate(&request)?;
        let request = request.into_inner();
        self.authorize(
            &pb::PeerQuery {
                cluster_id: request.cluster_id,
                requester_id: request.requester_id,
                receiver_id: request.receiver_id,
                bootstrap_digest: request.bootstrap_digest.clone(),
                incarnation: request.incarnation.clone(),
            },
            &certificate,
            None,
        )
        .await?;
        let principal: Principal = serde_json::from_slice(&request.principal)
            .map_err(|_| Status::unauthenticated("forwarded principal is invalid"))?;
        self.runtime
            .barrier()
            .await
            .map_err(|error| error.status())?;
        let member_id = self.member_id;
        let response = self
            .runtime
            .read(move |store, state| {
                if state.leader_id != Some(member_id) {
                    return Err(RpcError::new(
                        Code::Unavailable,
                        "etcdserver: leader changed",
                    ));
                }
                if request.keys {
                    store.authorize_lease(&principal, request.lease_id, false)?;
                } else {
                    store.require_authenticated(&principal)?;
                }
                let Some(lease) = store.lease(request.lease_id)? else {
                    return Ok(pb::LeaseState {
                        lease_id: request.lease_id,
                        ttl: -1,
                        ..Default::default()
                    });
                };
                Ok(pb::LeaseState {
                    lease_id: request.lease_id,
                    ttl: state.remaining_ttl(&lease)?,
                    granted_ttl: lease.ttl,
                    keys: if request.keys {
                        store.lease_keys(request.lease_id)?
                    } else {
                        Vec::new()
                    },
                })
            })
            .await
            .map_err(|error| error.status())?;
        Ok(Response::new(response))
    }
}

fn query(
    configuration: &Configuration,
    requester_id: u64,
    receiver_id: u64,
) -> Result<pb::PeerQuery> {
    let member = configuration.member(requester_id)?;
    Ok(pb::PeerQuery {
        cluster_id: configuration.cluster_id,
        requester_id,
        receiver_id,
        bootstrap_digest: configuration.bootstrap_digest.clone().unwrap_or_default(),
        incarnation: member
            .incarnation
            .map(|id| id.to_string())
            .unwrap_or_default(),
    })
}

async fn connect(
    url: &str,
    options: &Options,
    timeout: Duration,
) -> Result<pb::peer_client::PeerClient<tonic::transport::Channel>> {
    crate::server::private_key_permissions(&options.key)?;
    let identity = Identity::from_pem(
        tokio::fs::read(&options.certificate).await?,
        tokio::fs::read(&options.key).await?,
    );
    let ca = Certificate::from_pem(tokio::fs::read(&options.ca).await?);
    let endpoint = Endpoint::from_shared(url.to_owned())
        .map_err(|_| RpcError::invalid("peer endpoint is invalid"))?
        .tls_config(ClientTlsConfig::new().identity(identity).ca_certificate(ca))
        .map_err(|error| {
            RpcError::new(
                Code::FailedPrecondition,
                format!("peer client TLS: {error}"),
            )
        })?
        .connect_timeout(Duration::from_secs(1))
        .timeout(timeout);
    Ok(pb::peer_client::PeerClient::new(
        endpoint.connect().await.map_err(|error| {
            RpcError::new(Code::Unavailable, format!("peer connection: {error}"))
        })?,
    )
    .max_decoding_message_size(MAX_CATCHUP_BYTES + 4096))
}

pub async fn progress(
    configuration: Configuration,
    requester: u64,
    target: u64,
    options: &Options,
) -> Result<pb::PeerProgress> {
    let target_member = configuration.member(target)?;
    let mut client = connect(&target_member.peer_url, options, Duration::from_secs(2)).await?;
    client
        .progress(query(&configuration, requester, target)?)
        .await
        .map(|response| response.into_inner())
        .map_err(rpc_error)
}

pub async fn transfer_leader(
    configuration: Configuration,
    requester: u64,
    target: u64,
    principal: Principal,
    options: Options,
) -> Result<pb::LeadershipResponse> {
    let member = configuration.member(target)?;
    if member.learner {
        return Err(RpcError::new(
            Code::FailedPrecondition,
            "etcdserver: bad leader transferee",
        ));
    }
    let mut client = connect(&member.peer_url, &options, Duration::from_secs(7)).await?;
    let response = client
        .transfer_leader(pb::LeadershipRequest {
            cluster_id: configuration.cluster_id,
            requester_id: requester,
            receiver_id: target,
            principal: serde_json::to_vec(&principal)?,
            bootstrap_digest: configuration.bootstrap_digest.clone().unwrap_or_default(),
            incarnation: configuration
                .member(requester)?
                .incarnation
                .map(|id| id.to_string())
                .unwrap_or_default(),
        })
        .await
        .map_err(rpc_error)?
        .into_inner();
    if response.leader_id != target || response.term == 0 || response.applied_index == 0 {
        return Err(RpcError::corrupt(
            "leadership receipt lacks the requested transition",
        ));
    }
    Ok(response)
}

pub async fn lease_query(
    configuration: Configuration,
    requester: u64,
    receiver: u64,
    principal: Principal,
    id: i64,
    keys: bool,
    options: Options,
) -> Result<pb::LeaseState> {
    let member = configuration.member(receiver)?;
    let mut client = connect(&member.peer_url, &options, Duration::from_secs(5)).await?;
    client
        .lease_time_to_live(pb::LeaseQuery {
            cluster_id: configuration.cluster_id,
            requester_id: requester,
            receiver_id: receiver,
            lease_id: id,
            keys,
            principal: serde_json::to_vec(&principal)?,
            bootstrap_digest: configuration.bootstrap_digest.clone().unwrap_or_default(),
            incarnation: configuration
                .member(requester)?
                .incarnation
                .map(|id| id.to_string())
                .unwrap_or_default(),
        })
        .await
        .map(|response| response.into_inner())
        .map_err(rpc_error)
}

pub async fn snapshot(
    configuration: Configuration,
    requester: u64,
    source: u64,
    options: &Options,
    staging_dir: &std::path::Path,
) -> Result<NativeSnapshot> {
    let member = configuration.member(source)?;
    let mut client = connect(&member.peer_url, options, Duration::from_secs(60)).await?;
    let mut stream = client
        .snapshot(query(&configuration, requester, source)?)
        .await
        .map_err(rpc_error)?
        .into_inner();
    let first = stream
        .message()
        .await
        .map_err(rpc_error)?
        .ok_or_else(|| RpcError::invalid("native peer snapshot is empty"))?;
    if first.cluster_id != configuration.cluster_id
        || first.sender_id != source
        || first.receiver_id != requester
        || first.total_bytes > MAX_NATIVE_BYTES
        || first.message_id.len() != 16
        || first.checksum.len() != 32
        || first.offset != 0
    {
        return Err(RpcError::invalid(
            "native peer snapshot identity or limits are invalid",
        ));
    }
    let file = tempfile::NamedTempFile::new_in(staging_dir)?;
    let mut output = tokio::fs::File::from_std(file.reopen()?);
    let mut hash = Sha256::new();
    let mut offset = 0;
    let mut chunk = first.clone();
    loop {
        validate_chunk(&first, &chunk).map_err(rpc_error)?;
        if chunk.offset != offset
            || chunk.data.is_empty()
            || chunk.data.len() > CHUNK_BYTES
            || offset + chunk.data.len() as u64 > first.total_bytes
        {
            return Err(RpcError::invalid("native snapshot chunk is invalid"));
        }
        hash.update(&chunk.data);
        output.write_all(&chunk.data).await?;
        offset += chunk.data.len() as u64;
        match stream.message().await.map_err(rpc_error)? {
            Some(next) => chunk = next,
            None => break,
        }
    }
    output.sync_all().await?;
    if offset != first.total_bytes || hash.finalize().as_slice() != first.checksum {
        return Err(RpcError::invalid(
            "native snapshot transfer is truncated or corrupt",
        ));
    }
    let info = inspect(file.path())?;
    if info.cluster_id != configuration.cluster_id || info.member_id != source {
        return Err(RpcError::invalid(
            "native snapshot contains a different authority",
        ));
    }
    Ok(NativeSnapshot {
        file,
        info,
        configuration: None,
    })
}

async fn file_digest(path: &std::path::Path) -> Result<Vec<u8>> {
    let mut file = tokio::fs::File::open(path).await?;
    let mut hash = Sha256::new();
    let mut buffer = vec![0; CHUNK_BYTES];
    loop {
        let count = file.read(&mut buffer).await?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }
    Ok(hash.finalize().to_vec())
}

fn append_chunk(
    output: &mut Vec<u8>,
    chunk: &pb::PacketChunk,
    total: usize,
) -> std::result::Result<(), Status> {
    if chunk.offset != output.len() as u64
        || chunk.data.is_empty()
        || chunk.data.len() > CHUNK_BYTES
        || output
            .len()
            .checked_add(chunk.data.len())
            .is_none_or(|end| end > total)
    {
        return Err(Status::invalid_argument(
            "peer chunk offset or size is invalid",
        ));
    }
    output.extend_from_slice(&chunk.data);
    Ok(())
}

fn validate_chunk(
    first: &pb::PacketChunk,
    chunk: &pb::PacketChunk,
) -> std::result::Result<(), Status> {
    if chunk.cluster_id != first.cluster_id
        || chunk.configuration_id != first.configuration_id
        || chunk.sender_id != first.sender_id
        || chunk.receiver_id != first.receiver_id
        || chunk.message_id != first.message_id
        || chunk.total_bytes != first.total_bytes
        || chunk.checksum != first.checksum
        || chunk.bootstrap_digest != first.bootstrap_digest
        || chunk.incarnation != first.incarnation
        || chunk.encoding != first.encoding
    {
        return Err(Status::permission_denied(
            "peer metadata changed mid-stream",
        ));
    }
    Ok(())
}

fn peer_certificate<T>(request: &Request<T>) -> std::result::Result<Vec<u8>, Status> {
    let info = request
        .extensions()
        .get::<TlsConnectInfo<TcpConnectInfo>>()
        .ok_or_else(|| Status::unauthenticated("peer mutual TLS is required"))?;
    let certificates = info
        .peer_certs()
        .ok_or_else(|| Status::unauthenticated("peer certificate is required"))?;
    Ok(certificates
        .first()
        .ok_or_else(|| Status::unauthenticated("peer certificate chain is empty"))?
        .as_ref()
        .to_vec())
}

fn validate_certificate(
    certificate: &[u8],
    cluster_id: u64,
    member_id: u64,
) -> std::result::Result<(), Status> {
    let (_, certificate) = x509_parser::parse_x509_certificate(certificate)
        .map_err(|_| Status::unauthenticated("peer certificate cannot be parsed"))?;
    let names = certificate
        .subject_alternative_name()
        .map_err(|_| Status::unauthenticated("peer certificate SAN is invalid"))?
        .ok_or_else(|| Status::unauthenticated("peer certificate identity is missing"))?;
    let expected = format!("spiffe://etcqlite/{cluster_id:016x}/{member_id:016x}");
    if !names.value.general_names.iter().any(
        |name| matches!(name, x509_parser::extensions::GeneralName::URI(uri) if *uri == expected),
    ) {
        return Err(Status::permission_denied(
            "peer certificate does not bind the expected cluster/member",
        ));
    }
    Ok(())
}

fn rpc_error(error: Status) -> RpcError {
    RpcError::new(error.code(), error.message())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        cluster::Command,
        storage::{KvRequest, command::Mutation},
        wire::etcdserverpb,
    };
    use std::io::Read;

    #[test]
    fn compressed_protocol_spills_without_materializing_the_json_packet() {
        for bytes in [64, 2 << 20] {
            let mut state = 42_u64;
            let value = (0..bytes)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    state as u8
                })
                .collect();
            let message = PeerMessage::Proposal(Command::Mutation {
                id: Uuid::from_u128(42),
                principal: Principal::Anonymous,
                request: Mutation::Kv(KvRequest::Put(etcdserverpb::PutRequest {
                    key: b"mock/transport".to_vec(),
                    value,
                    ..Default::default()
                })),
            });
            let expected = serde_json::to_vec(&message).unwrap();
            let encoded = spool_message(message).unwrap();
            let compressed = match encoded.data {
                EncodedData::Inline(data) => {
                    assert!(bytes < INLINE_BYTES);
                    assert!(data.len() <= INLINE_BYTES);
                    data
                }
                EncodedData::File(file) => {
                    assert!(bytes > INLINE_BYTES);
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::PermissionsExt;
                        assert_eq!(
                            file.as_file().metadata().unwrap().permissions().mode() & 0o777,
                            0o600
                        );
                    }
                    let mut data = Vec::new();
                    file.reopen().unwrap().read_to_end(&mut data).unwrap();
                    data
                }
            };
            assert_eq!(encoded.bytes, compressed.len() as u64);
            assert_eq!(encoded.checksum, Sha256::digest(&compressed).to_vec());
            let decoded = decode_message(&compressed, "gzip-json-v1").unwrap();
            assert_eq!(serde_json::to_vec(&decoded).unwrap(), expected);
            assert!(decode_message(&compressed[..compressed.len() - 1], "gzip-json-v1").is_err());
            assert!(decode_message(&compressed, "unknown").is_err());
            let mut trailing = compressed.clone();
            trailing.push(0);
            assert!(decode_message(&trailing, "gzip-json-v1").is_err());
        }
    }

    #[test]
    fn legacy_protocol_and_compressed_size_limit_are_explicit() {
        use std::io::Write;
        let message = PeerMessage::Proposal(Command::Barrier {
            id: Uuid::from_u128(1),
        });
        let bytes = serde_json::to_vec(&message).unwrap();
        assert_eq!(
            serde_json::to_vec(&decode_message(&bytes, "").unwrap()).unwrap(),
            bytes
        );
        let mut spool = Spool {
            data: EncodedData::Inline(Vec::new()),
            bytes: MAX_PACKET_BYTES as u64,
            hash: Sha256::new(),
            exceeded: false,
        };
        assert!(spool.write_all(&[1]).is_err());
        assert!(spool.exceeded);
        assert!(matches!(spool.data, EncodedData::Inline(data) if data.is_empty()));
    }
}
