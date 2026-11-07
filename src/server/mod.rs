mod auth;
mod concurrency;
mod gateway;
mod kv;
mod lease;
mod watch;

use crate::{
    error::{Result, RpcError},
    runtime::{Runtime, Worker},
    storage::{MAX_REQUEST_BYTES, auth::Principal},
    wire::{self, etcdserverpb as pb, v3electionpb, v3lockpb},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use prost::Message;
use sha2::{Digest, Sha256};
use std::{net::SocketAddr, path::PathBuf, pin::Pin, sync::Arc, time::Duration};
use tokio::sync::{Semaphore, oneshot};
use tokio_stream::{Stream, wrappers::TcpListenerStream};
use tonic::{
    Code, Request, Response, Status,
    service::Routes,
    transport::{
        Certificate, Identity, Server, ServerTlsConfig,
        server::{TcpConnectInfo, TlsConnectInfo},
    },
};
use uuid::Uuid;

pub type ResponseStream<T> =
    Pin<Box<dyn Stream<Item = std::result::Result<T, Status>> + Send + 'static>>;

#[derive(Clone)]
pub struct Api {
    pub(crate) runtime: Runtime,
    password_slots: Arc<Semaphore>,
    watch_slots: Arc<Semaphore>,
}

#[derive(Clone, Debug)]
pub struct TlsOptions {
    pub certificate: PathBuf,
    pub key: PathBuf,
    pub client_ca: Option<PathBuf>,
}

#[derive(Clone, Debug)]
pub struct Options {
    pub data_dir: PathBuf,
    pub address: SocketAddr,
    pub tls: Option<TlsOptions>,
    pub request_timeout: Duration,
}

pub struct RunningServer {
    pub endpoint: String,
    pub runtime: Runtime,
    shutdown: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<Result<()>>,
    worker: Worker,
}

impl RunningServer {
    pub async fn shutdown(self) -> Result<()> {
        if self.shutdown.send(()).is_err() && !self.task.is_finished() {
            return Err(RpcError::new(
                Code::Unavailable,
                "server shutdown channel disconnected",
            ));
        }
        let serving = self
            .task
            .await
            .map_err(|error| RpcError::corrupt(format!("server task: {error}")))?;
        let writer = self.worker.shutdown().await;
        serving.and(writer)
    }
}

pub async fn start(options: Options) -> Result<RunningServer> {
    if !options.address.ip().is_loopback()
        && options
            .tls
            .as_ref()
            .is_none_or(|tls| tls.client_ca.is_none())
    {
        return Err(RpcError::new(
            Code::FailedPrecondition,
            "non-loopback client listeners require TLS and a trusted client CA",
        ));
    }
    let mut builder = Server::builder();
    if let Some(tls) = &options.tls {
        private_key_permissions(&tls.key)?;
        let identity = Identity::from_pem(
            tokio::fs::read(&tls.certificate).await?,
            tokio::fs::read(&tls.key).await?,
        );
        let mut config = ServerTlsConfig::new().identity(identity);
        if let Some(ca) = &tls.client_ca {
            config = config.client_ca_root(Certificate::from_pem(tokio::fs::read(ca).await?));
        }
        builder = builder.tls_config(config).map_err(|error| {
            RpcError::new(
                Code::FailedPrecondition,
                format!("client TLS configuration: {error}"),
            )
        })?;
    }
    let listener = tokio::net::TcpListener::bind(options.address).await?;
    let address = listener.local_addr()?;
    let endpoint = format!(
        "{}://{address}",
        if options.tls.is_some() {
            "https"
        } else {
            "http"
        }
    );
    let (runtime, worker) =
        Runtime::open(options.data_dir, endpoint.clone(), options.request_timeout).await?;
    let api = Api {
        runtime: runtime.clone(),
        password_slots: Arc::new(Semaphore::new(4)),
        watch_slots: Arc::new(Semaphore::new(256)),
    };
    let routes = Routes::new(
        pb::kv_server::KvServer::new(api.clone())
            .max_decoding_message_size(MAX_REQUEST_BYTES)
            .max_encoding_message_size(128 << 20),
    )
    .add_service(
        pb::lease_server::LeaseServer::new(api.clone())
            .max_decoding_message_size(MAX_REQUEST_BYTES),
    )
    .add_service(
        pb::auth_server::AuthServer::new(api.clone()).max_decoding_message_size(MAX_REQUEST_BYTES),
    )
    .add_service(
        pb::watch_server::WatchServer::new(api.clone())
            .max_decoding_message_size(MAX_REQUEST_BYTES)
            .max_encoding_message_size(4 << 20),
    )
    .add_service(
        v3lockpb::lock_server::LockServer::new(api.clone())
            .max_decoding_message_size(MAX_REQUEST_BYTES),
    )
    .add_service(
        v3electionpb::election_server::ElectionServer::new(api.clone())
            .max_decoding_message_size(MAX_REQUEST_BYTES),
    );
    let (shutdown, stopped) = oneshot::channel();
    let router = gateway::router(api, routes.into_axum_router());
    let server = builder
        .accept_http1(true)
        .concurrency_limit_per_connection(128)
        .load_shed(true)
        .add_routes(Routes::from(router));
    let task = tokio::spawn(async move {
        server
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = stopped.await;
            })
            .await
            .map_err(|error| RpcError::new(Code::Unavailable, format!("client listener: {error}")))
    });
    Ok(RunningServer {
        endpoint,
        runtime,
        shutdown,
        task,
        worker,
    })
}

impl Api {
    pub(crate) async fn principal<T>(
        &self,
        request: &Request<T>,
    ) -> std::result::Result<Principal, Status> {
        let token = request
            .metadata()
            .get("token")
            .map(|token| token.to_str())
            .transpose()
            .map_err(|_| Status::unauthenticated("etcdserver: invalid auth token"))?;
        let token_hash: Option<[u8; 32]> = token
            .filter(|token| !token.is_empty())
            .map(|token| Sha256::digest(token.as_bytes()).into());
        let certificate_name = certificate_name(request)?;
        self.runtime
            .read(move |store, state| {
                store.principal(
                    token_hash.as_ref(),
                    certificate_name.as_deref(),
                    state.now_ms()?,
                )
            })
            .await
            .map_err(|error| error.status())
    }

    pub(crate) async fn checked<T: Message>(
        &self,
        request: &Request<T>,
    ) -> std::result::Result<Principal, Status> {
        if request.get_ref().encoded_len() > MAX_REQUEST_BYTES {
            return Err(Status::invalid_argument("etcdserver: request is too large"));
        }
        self.principal(request).await
    }

    pub(crate) fn response<T>(&self, value: T) -> std::result::Result<Response<T>, Status> {
        let mut response = Response::new(value);
        response.metadata_mut().insert(
            "x-etcqlite-build-id",
            wire::BUILD_SHA
                .parse()
                .map_err(|_| Status::internal("invalid stamped build identity"))?,
        );
        Ok(response)
    }

    pub(crate) async fn password_hash(
        &self,
        password: String,
        encoded_hash: String,
        no_password: bool,
    ) -> std::result::Result<Vec<u8>, Status> {
        if no_password {
            return Ok(Vec::new());
        }
        if !encoded_hash.is_empty() {
            let bytes = STANDARD
                .decode(encoded_hash)
                .map_err(|_| Status::invalid_argument("invalid base64 password hash"))?;
            let hash = std::str::from_utf8(&bytes)
                .map_err(|_| Status::invalid_argument("invalid password hash encoding"))?;
            let parts: bcrypt::HashParts = hash
                .parse()
                .map_err(|_| Status::invalid_argument("invalid bcrypt password hash"))?;
            if parts.get_cost() > 16 {
                return Err(Status::resource_exhausted(
                    "bcrypt cost exceeds the bounded verification policy",
                ));
            }
            return Ok(bytes);
        }
        if password.len() > 72 {
            return Err(Status::invalid_argument(
                "password exceeds bcrypt's 72-byte limit",
            ));
        }
        let permit = self
            .password_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Status::unavailable("password worker stopped"))?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            bcrypt::hash(password, 10).map(String::into_bytes)
        })
        .await
        .map_err(|_| Status::internal("password hashing worker failed"))?
        .map_err(|_| Status::internal("password hashing failed"))
    }

    pub(crate) async fn require_admin(
        &self,
        principal: Principal,
    ) -> std::result::Result<(), Status> {
        self.runtime
            .read(move |store, _| store.check_admin(&principal))
            .await
            .map_err(|error| error.status())
    }

    pub(crate) async fn authenticate(
        &self,
        name: String,
        password: String,
    ) -> std::result::Result<pb::AuthenticateResponse, Status> {
        let lookup = name.clone();
        let credentials = self
            .runtime
            .read(move |store, _| store.credentials(&lookup))
            .await
            .map_err(|error| error.status())?;
        if credentials.no_password || password.len() > 72 {
            return Err(Status::invalid_argument(
                "etcdserver: authentication failed, invalid user ID or password",
            ));
        }
        let password_hash = credentials.password_hash.clone();
        let permit = self
            .password_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Status::unavailable("password worker stopped"))?;
        let verified = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let hash = std::str::from_utf8(&password_hash).map_err(|_| ())?;
            bcrypt::verify(password, hash).map_err(|_| ())
        })
        .await
        .map_err(|_| Status::internal("password verification worker failed"))?
        .map_err(|_| {
            Status::invalid_argument(
                "etcdserver: authentication failed, invalid user ID or password",
            )
        })?;
        if !verified {
            return Err(Status::invalid_argument(
                "etcdserver: authentication failed, invalid user ID or password",
            ));
        }
        let token = STANDARD.encode(Uuid::new_v4().as_bytes());
        let token_hash: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        let now = self
            .runtime
            .read(|_, state| state.now_ms())
            .await
            .map_err(|error| error.status())?;
        let expires_at_ms = now
            .checked_add(300_000)
            .ok_or_else(|| Status::internal("token expiry overflow"))?;
        let result = self
            .runtime
            .mutate(
                Principal::Anonymous,
                crate::storage::command::Mutation::Auth(
                    crate::storage::auth::AuthCommand::Authenticate {
                        name,
                        expected_password_hash: credentials.password_hash,
                        expected_auth_revision: credentials.revision,
                        token_hash,
                        expires_at_ms,
                    },
                ),
            )
            .await
            .map_err(|error| error.status())?;
        match result {
            crate::storage::command::Reply::Auth(
                crate::storage::auth::AuthReply::Authenticated {
                    header,
                    token_hash: actual,
                },
            ) if actual == token_hash => Ok(pb::AuthenticateResponse { header, token }),
            _ => Err(Status::internal(
                "authentication returned an invalid receipt",
            )),
        }
    }
}

pub(crate) fn fill_header(header: &mut Option<pb::ResponseHeader>, physical: pb::ResponseHeader) {
    if let Some(header) = header {
        header.cluster_id = physical.cluster_id;
        header.member_id = physical.member_id;
        header.raft_term = physical.raft_term;
    }
}

pub(crate) fn new_lease_id() -> std::result::Result<i64, Status> {
    let bytes: [u8; 8] = Uuid::new_v4().as_bytes()[..8]
        .try_into()
        .map_err(|_| Status::internal("generated lease identifier has invalid width"))?;
    Ok((i64::from_be_bytes(bytes) & i64::MAX).max(1))
}

fn certificate_name<T>(request: &Request<T>) -> std::result::Result<Option<String>, Status> {
    let Some(info) = request.extensions().get::<TlsConnectInfo<TcpConnectInfo>>() else {
        return Ok(None);
    };
    let Some(certificates) = info.peer_certs() else {
        return Ok(None);
    };
    let certificate = certificates
        .first()
        .ok_or_else(|| Status::unauthenticated("client certificate is missing"))?;
    let (_, certificate) = x509_parser::parse_x509_certificate(certificate.as_ref())
        .map_err(|_| Status::unauthenticated("client certificate cannot be parsed"))?;
    certificate
        .subject()
        .iter_common_name()
        .next()
        .map(|name| {
            name.as_str()
                .map(str::to_owned)
                .map_err(|_| Status::unauthenticated("certificate common name is invalid"))
        })
        .transpose()
}

fn private_key_permissions(path: &std::path::Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if std::fs::metadata(path)?.permissions().mode() & 0o077 != 0 {
            return Err(RpcError::new(
                Code::PermissionDenied,
                "TLS private key must not be accessible to group or other users",
            ));
        }
    }
    Ok(())
}
