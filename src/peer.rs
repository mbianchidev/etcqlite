use crate::{
    cluster::{Command, Configuration},
    error::{Result, RpcError},
    runtime::Runtime,
    storage::auth::Principal,
    wire::peer as pb,
};
use omnipaxos::messages::Message;
use sha2::{Digest, Sha256};
use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::{Semaphore, oneshot};
use tokio_stream::{StreamExt, wrappers::TcpListenerStream};
use tonic::{
    Code, Request, Response, Status,
    transport::{
        Certificate, ClientTlsConfig, Endpoint, Identity, Server, ServerTlsConfig,
        server::{TcpConnectInfo, TlsConnectInfo},
    },
};
use uuid::Uuid;

const CHUNK_BYTES: usize = 1 << 20;
const MAX_PACKET_BYTES: usize = 2 << 30;
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
    relays: Vec<tokio::task::JoinHandle<()>>,
}

#[derive(Clone)]
struct Service {
    configuration: Configuration,
    member_id: u64,
    runtime: Runtime,
}

struct Outbound {
    data: Arc<Vec<u8>>,
    _budget: tokio::sync::OwnedSemaphorePermit,
}

pub async fn start(
    configuration: Configuration,
    member_id: u64,
    runtime: Runtime,
    options: Options,
) -> Result<RunningPeer> {
    configuration.validate()?;
    let own = configuration.member(member_id)?;
    let address = own
        .peer_url
        .parse::<axum::http::Uri>()
        .map_err(|_| RpcError::invalid("invalid persisted peer URL"))?;
    if address.port_u16() != Some(options.address.port()) {
        return Err(RpcError::new(
            Code::FailedPrecondition,
            "peer listener does not match the persisted endpoint",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if std::fs::metadata(&options.key)?.permissions().mode() & 0o077 != 0 {
            return Err(RpcError::new(
                Code::PermissionDenied,
                "peer private key is not private",
            ));
        }
    }
    let certificate = tokio::fs::read(&options.certificate).await?;
    let key = tokio::fs::read(&options.key).await?;
    let ca = tokio::fs::read(&options.ca).await?;
    let identity = Identity::from_pem(certificate.clone(), key.clone());
    let listener = tokio::net::TcpListener::bind(options.address).await?;
    let service = Service {
        configuration: configuration.clone(),
        member_id,
        runtime: runtime.clone(),
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
            pb::peer_server::PeerServer::new(service).max_decoding_message_size(CHUNK_BYTES + 4096),
        );
    let server = tokio::spawn(async move {
        server
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = stopped.await;
            })
            .await
            .map_err(|error| RpcError::new(Code::Unavailable, format!("peer listener: {error}")))
    });
    let mut relays = Vec::new();
    let mut senders = std::collections::BTreeMap::new();
    for member in configuration
        .members
        .iter()
        .filter(|member| member.id != member_id && !member.learner)
    {
        let member = member.clone();
        let runtime = runtime.clone();
        let configuration = configuration.clone();
        let identity = identity.clone();
        let ca = ca.clone();
        let endpoint = Endpoint::from_shared(member.peer_url.clone())
            .map_err(|_| RpcError::invalid("peer endpoint is invalid"))?
            .tls_config(
                ClientTlsConfig::new()
                    .identity(identity)
                    .ca_certificate(Certificate::from_pem(ca)),
            )
            .map_err(|_| RpcError::new(Code::FailedPrecondition, "peer client TLS is invalid"))?
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(5));
        let mut client = pb::peer_client::PeerClient::new(endpoint.connect_lazy());
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<Outbound>(32);
        senders.insert(member.pid, sender);
        relays.push(tokio::spawn(async move {
            while let Some(outbound) = receiver.recv().await {
                let result: std::result::Result<(), Status> = async {
                    let id = Uuid::new_v4();
                    let checksum = Sha256::digest(outbound.data.as_slice()).to_vec();
                    let data = Arc::clone(&outbound.data);
                    let size = data.len();
                    let chunks = tokio_stream::iter((0..size).step_by(CHUNK_BYTES)).map(move |offset| {
                        pb::PacketChunk {
                            cluster_id: configuration.cluster_id, configuration_id: configuration.configuration_id,
                            sender_id: member_id, receiver_id: member.id, message_id: id.as_bytes().to_vec(),
                            total_bytes: size as u64, offset: offset as u64, checksum: checksum.clone(),
                            data: data[offset..(offset + CHUNK_BYTES).min(size)].to_vec(),
                        }
                    });
                    let response = client.exchange(chunks).await?.into_inner();
                    if response.message_id != id.as_bytes() { return Err(Status::data_loss("peer receipt identity mismatch")); }
                    Ok(())
                }.await;
                if let Err(error) = result {
                    tracing::warn!(peer_id = member.id, code = ?error.code(), "peer delivery failed; durable protocol resynchronization required");
                    if let Err(error) = runtime.peer_reconnected(member.pid) {
                        tracing::warn!(peer_id = member.id, error = %error, "cannot request peer resynchronization");
                    }
                }
            }
        }));
    }
    let outbox = runtime.take_outbox()?;
    let budget = Arc::new(Semaphore::new(OUTBOUND_BUDGET_UNITS));
    let bridge = tokio::task::spawn_blocking(move || {
        while let Ok(message) = outbox.recv() {
            let target = message.get_receiver();
            let Some(sender) = senders.get(&target) else {
                tracing::error!(
                    target,
                    "protocol generated a packet for an unconfigured voting peer"
                );
                continue;
            };
            let data = match serde_json::to_vec(&message) {
                Ok(data) => data,
                Err(_) => {
                    tracing::error!("protocol packet serialization failed");
                    continue;
                }
            };
            if data.len() > MAX_PACKET_BYTES {
                tracing::error!(
                    bytes = data.len(),
                    "protocol packet exceeds the configured transfer limit"
                );
                continue;
            }
            let units = data.len().div_ceil(1024).max(1);
            let units = match u32::try_from(units) {
                Ok(units) => units,
                Err(_) => {
                    tracing::error!("peer budget accounting exceeds supported range");
                    continue;
                }
            };
            let permit = match budget.clone().try_acquire_many_owned(units) {
                Ok(permit) => permit,
                Err(_) => {
                    tracing::warn!("peer byte budget exhausted; protocol must resynchronize");
                    continue;
                }
            };
            let outbound = Outbound {
                data: Arc::new(data),
                _budget: permit,
            };
            if sender.try_send(outbound).is_err() {
                tracing::warn!(
                    target,
                    "peer relay overloaded or stopped; protocol must resynchronize"
                );
            }
        }
    });
    Ok(RunningPeer {
        stop,
        server,
        bridge,
        relays,
    })
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
        for relay in self.relays {
            relay
                .await
                .map_err(|error| RpcError::corrupt(format!("peer relay task: {error}")))?;
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl pb::peer_server::Peer for Service {
    async fn exchange(
        &self,
        request: Request<tonic::Streaming<pb::PacketChunk>>,
    ) -> std::result::Result<Response<pb::PacketReceipt>, Status> {
        let certificate = peer_certificate(&request)?;
        let mut stream = request.into_inner();
        let first = stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("peer transfer is empty"))?;
        self.validate_identity(&first, &certificate)?;
        let total = usize::try_from(first.total_bytes).map_err(|_| {
            Status::resource_exhausted("peer transfer size exceeds platform bounds")
        })?;
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
        let mut data = Vec::new();
        append_chunk(&mut data, &first, total)?;
        while let Some(chunk) = stream.message().await? {
            if chunk.cluster_id != first.cluster_id
                || chunk.configuration_id != first.configuration_id
                || chunk.sender_id != first.sender_id
                || chunk.receiver_id != first.receiver_id
                || chunk.message_id != first.message_id
                || chunk.checksum != first.checksum
                || chunk.total_bytes != first.total_bytes
            {
                return Err(Status::permission_denied(
                    "peer transfer metadata changed mid-stream",
                ));
            }
            append_chunk(&mut data, &chunk, total)?;
        }
        if data.len() != total || Sha256::digest(&data).as_slice() != first.checksum {
            return Err(Status::data_loss(
                "peer transfer is incomplete or has an invalid checksum",
            ));
        }
        let message: Message<Command> = serde_json::from_slice(&data)
            .map_err(|_| Status::data_loss("peer protocol payload cannot be decoded"))?;
        let sender = self
            .configuration
            .member(first.sender_id)
            .map_err(|error| error.status())?;
        let receiver = self
            .configuration
            .member(self.member_id)
            .map_err(|error| error.status())?;
        if message.get_sender() != sender.pid || message.get_receiver() != receiver.pid {
            return Err(Status::permission_denied(
                "protocol identity does not match authenticated peer metadata",
            ));
        }
        self.runtime
            .deliver_peer(message)
            .map_err(|error| error.status())?;
        Ok(Response::new(pb::PacketReceipt {
            message_id: first.message_id,
        }))
    }

    async fn lease_time_to_live(
        &self,
        request: Request<pb::LeaseQuery>,
    ) -> std::result::Result<Response<pb::LeaseState>, Status> {
        let certificate = peer_certificate(&request)?;
        let request = request.into_inner();
        if request.cluster_id != self.configuration.cluster_id
            || request.receiver_id != self.member_id
        {
            return Err(Status::permission_denied(
                "peer lease query has the wrong cluster or destination",
            ));
        }
        validate_certificate(
            &certificate,
            self.configuration.cluster_id,
            request.requester_id,
        )?;
        self.configuration
            .member(request.requester_id)
            .map_err(|error| error.status())?;
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

impl Service {
    fn validate_identity(
        &self,
        packet: &pb::PacketChunk,
        certificate: &[u8],
    ) -> std::result::Result<(), Status> {
        if packet.cluster_id != self.configuration.cluster_id
            || packet.configuration_id != self.configuration.configuration_id
            || packet.receiver_id != self.member_id
        {
            return Err(Status::permission_denied(
                "peer transfer has a wrong cluster, configuration or destination",
            ));
        }
        self.configuration
            .member(packet.sender_id)
            .map_err(|error| error.status())?;
        validate_certificate(certificate, packet.cluster_id, packet.sender_id)
    }
}

fn append_chunk(
    output: &mut Vec<u8>,
    chunk: &pb::PacketChunk,
    total: usize,
) -> std::result::Result<(), Status> {
    if chunk.offset != output.len() as u64
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
            "peer certificate does not bind the expected cluster/member identity",
        ));
    }
    Ok(())
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
    let target = configuration.member(receiver)?;
    let identity = Identity::from_pem(
        tokio::fs::read(options.certificate).await?,
        tokio::fs::read(options.key).await?,
    );
    let ca = Certificate::from_pem(tokio::fs::read(options.ca).await?);
    let endpoint = Endpoint::from_shared(target.peer_url.clone())
        .map_err(|_| RpcError::corrupt("leader peer endpoint is invalid"))?
        .tls_config(ClientTlsConfig::new().identity(identity).ca_certificate(ca))
        .map_err(|_| RpcError::corrupt("leader peer TLS is invalid"))?
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(5));
    let channel = endpoint.connect().await.map_err(|error| {
        RpcError::new(
            Code::Unavailable,
            format!("leader peer connection: {error}"),
        )
    })?;
    let mut client = pb::peer_client::PeerClient::new(channel);
    client
        .lease_time_to_live(pb::LeaseQuery {
            cluster_id: configuration.cluster_id,
            requester_id: requester,
            receiver_id: receiver,
            lease_id: id,
            keys,
            principal: serde_json::to_vec(&principal)?,
        })
        .await
        .map(|response| response.into_inner())
        .map_err(|error| RpcError::new(error.code(), error.message()))
}
