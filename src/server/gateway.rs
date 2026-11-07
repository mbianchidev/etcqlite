use super::{Api, ResponseStream};
use crate::{
    storage::{
        command::{Mutation, Reply},
        lease::{LeaseCommand, LeaseReply},
    },
    wire::{self, etcdserverpb as pb, v3electionpb as election, v3lockpb as lock},
};
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{Request as HttpRequest, State},
    http::{Extensions, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use prost::Message;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::json;
use tokio_stream::StreamExt;
use tonic::{
    Request, Status,
    transport::server::{TcpConnectInfo, TlsConnectInfo},
};

pub(super) fn router(api: Api, grpc: Router) -> Router {
    let http = Router::new()
        .route("/v3/{*path}", post(dispatch))
        .route("/health", get(health))
        .route("/livez", get(|| async { "ok" }))
        .route("/readyz", get(ready))
        .route("/version", get(version))
        .route("/metrics", get(metrics))
        .route("/etcqlite/v1/{*path}", get(native).post(native))
        .with_state(api);
    grpc.merge(http)
}

#[derive(serde::Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct ProjectionRequest {
    #[serde(default)]
    include_data: bool,
    #[serde(default)]
    ack_sensitive_data: bool,
}

enum NativeArtifact {
    Snapshot(crate::storage::snapshot::NativeSnapshot),
    Projection(crate::storage::inspection::Projection),
}

impl NativeArtifact {
    fn path(&self) -> &std::path::Path {
        match self {
            Self::Snapshot(snapshot) => snapshot.path(),
            Self::Projection(projection) => projection.path(),
        }
    }
}

async fn native(State(api): State<Api>, request: HttpRequest) -> Response {
    let path = request.uri().path().to_owned();
    let method = request.method().clone();
    let (parts, body) = request.into_parts();
    let result: std::result::Result<Response, Status> = async {
        let principal = api.principal(&decode::<pb::SnapshotRequest>(b"{}", &parts.headers, &parts.extensions)?).await?;
        let checked = principal.clone();
        let (descriptor, etag) = api.runtime.read(move |store, state| {
            store.require_admin(&checked)?;
            let etag = format!("\"{:016x}:{:016x}\"", store.identity().cluster_id, store.identity().member_id);
            Ok((json!({
                "format": 1, "kind": "etcqlite", "cluster_id": store.identity().cluster_id.to_string(),
                "member_id": store.identity().member_id.to_string(), "revision": store.revision(),
                "schema_version": store.schema_version()?, "engine_version": wire::ENGINE_VERSION,
                "api_version": wire::ETCD_API_VERSION, "build": wire::BUILD_SHA, "dirty": wire::BUILD_DIRTY,
                "configuration_id": state.cluster.as_ref().map(|configuration| configuration.configuration_id),
                "capabilities": ["native-snapshot-v1", "read-only-projection-v1"],
                "snapshot_path": "/etcqlite/v1/snapshots", "projection_path": "/etcqlite/v1/projections",
                "inspection": "metadata-only-by-default", "recoverable_projection": false
            }), etag))
        }).await.map_err(|error| error.status())?;
        if path == "/etcqlite/v1/discovery" && method == axum::http::Method::GET {
            let mut response = Json(descriptor).into_response();
            response.headers_mut().insert("etag", etag.parse().map_err(|_| Status::internal("invalid resource identity tag"))?);
            return Ok(response);
        }
        if method != axum::http::Method::POST { return Err(Status::not_found("native resource route is not supported")); }
        if path != "/etcqlite/v1/snapshots" && path != "/etcqlite/v1/projections" { return Err(Status::not_found("native resource route is not supported")); }
        match parts.headers.get("if-match").map(|value| value.to_str()).transpose().map_err(|_| Status::invalid_argument("If-Match must be a valid resource identity tag"))? {
            Some(value) if value == etag => {}
            Some(_) => return Ok((StatusCode::PRECONDITION_FAILED, Json(json!({ "code": 9, "message": "resource identity changed; rediscover before retrying" }))).into_response()),
            None => return Ok((StatusCode::PRECONDITION_REQUIRED, Json(json!({ "code": 9, "message": "If-Match from discovery is required" }))).into_response()),
        }
        let bytes = axum::body::to_bytes(body, 64 << 10).await.map_err(|_| Status::resource_exhausted("native request exceeds the bounded JSON limit"))?;
        let options: ProjectionRequest = serde_json::from_slice(&bytes).map_err(|_| Status::invalid_argument("invalid native resource JSON"))?;
        if options.include_data && !options.ack_sensitive_data {
            return Err(Status::permission_denied("raw key/value inspection requires ack_sensitive_data"));
        }
        let permit = api.snapshot_slots.clone().try_acquire_owned().map_err(|_| Status::resource_exhausted("native artifact transfers are at capacity"))?;
        api.runtime.barrier().await.map_err(|error| error.status())?;
        let artifact = match path.as_str() {
            "/etcqlite/v1/snapshots" => {
                if options.include_data { return Err(Status::invalid_argument("include_data applies only to projections; snapshots always contain sensitive full state")); }
                NativeArtifact::Snapshot(api.runtime.native_snapshot(principal).await.map_err(|error| error.status())?.0)
            }
            "/etcqlite/v1/projections" => NativeArtifact::Projection(api.runtime.maintenance(move |store, _| store.inspection_projection(&principal, options.include_data)).await.map_err(|error| error.status())?),
            _ => return Err(Status::not_found("native resource route is not supported")),
        };
        let (sender, receiver) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            let _permit = permit;
            use tokio::io::AsyncReadExt;
            let outcome: std::io::Result<()> = async {
                let mut file = tokio::fs::File::open(artifact.path()).await?;
                loop {
                    let mut buffer = vec![0; 1 << 20];
                    let count = file.read(&mut buffer).await?;
                    if count == 0 { break; }
                    buffer.truncate(count);
                    if sender.send(Ok(Bytes::from(buffer))).await.is_err() { return Ok(()); }
                }
                Ok(())
            }.await;
            if let Err(error) = outcome && sender.send(Err(error)).await.is_err() { tracing::debug!("native artifact read error waiter canceled"); }
        });
        let mut response = Body::from_stream(tokio_stream::wrappers::ReceiverStream::new(receiver)).into_response();
        response.headers_mut().insert("content-type", "application/vnd.sqlite3".parse().map_err(|_| Status::internal("invalid native content type"))?);
        response.headers_mut().insert("etag", etag.parse().map_err(|_| Status::internal("invalid resource identity tag"))?);
        Ok(response)
    }.await;
    match result {
        Ok(response) => response,
        Err(error) => failure(error),
    }
}

async fn dispatch(State(api): State<Api>, request: HttpRequest) -> Response {
    let path = request.uri().path().to_owned();
    let (parts, body) = request.into_parts();
    let body = match axum::body::to_bytes(body, crate::storage::MAX_REQUEST_BYTES * 2).await {
        Ok(body) => body,
        Err(_) => {
            return failure(Status::resource_exhausted(
                "HTTP request exceeds the bounded JSON limit",
            ));
        }
    };
    macro_rules! call {
        ($request:ty, $method:path) => {{
            let request = decode::<$request>(&body, &parts.headers, &parts.extensions)?;
            let response = $method(&api, request).await?;
            output(response.into_inner())?
        }};
    }
    let result: std::result::Result<Response, Status> = async {
        Ok(match path.as_str() {
            "/v3/kv/range" => call!(pb::RangeRequest, pb::kv_server::Kv::range),
            "/v3/kv/put" => call!(pb::PutRequest, pb::kv_server::Kv::put),
            "/v3/kv/deleterange" => call!(pb::DeleteRangeRequest, pb::kv_server::Kv::delete_range),
            "/v3/kv/txn" => call!(pb::TxnRequest, pb::kv_server::Kv::txn),
            "/v3/kv/compaction" => call!(pb::CompactionRequest, pb::kv_server::Kv::compact),
            "/v3/cluster/member/add" => call!(
                pb::MemberAddRequest,
                pb::cluster_server::Cluster::member_add
            ),
            "/v3/cluster/member/remove" => call!(
                pb::MemberRemoveRequest,
                pb::cluster_server::Cluster::member_remove
            ),
            "/v3/cluster/member/update" => call!(
                pb::MemberUpdateRequest,
                pb::cluster_server::Cluster::member_update
            ),
            "/v3/cluster/member/list" => call!(
                pb::MemberListRequest,
                pb::cluster_server::Cluster::member_list
            ),
            "/v3/cluster/member/promote" => call!(
                pb::MemberPromoteRequest,
                pb::cluster_server::Cluster::member_promote
            ),
            "/v3/maintenance/alarm" => {
                call!(pb::AlarmRequest, pb::maintenance_server::Maintenance::alarm)
            }
            "/v3/maintenance/status" => call!(
                pb::StatusRequest,
                pb::maintenance_server::Maintenance::status
            ),
            "/v3/maintenance/defragment" => call!(
                pb::DefragmentRequest,
                pb::maintenance_server::Maintenance::defragment
            ),
            "/v3/maintenance/hash" => {
                call!(pb::HashRequest, pb::maintenance_server::Maintenance::hash)
            }
            "/v3/maintenance/hashkv" => call!(
                pb::HashKvRequest,
                pb::maintenance_server::Maintenance::hash_kv
            ),
            "/v3/maintenance/transfer-leadership" => call!(
                pb::MoveLeaderRequest,
                pb::maintenance_server::Maintenance::move_leader
            ),
            "/v3/maintenance/downgrade" => call!(
                pb::DowngradeRequest,
                pb::maintenance_server::Maintenance::downgrade
            ),
            "/v3/maintenance/snapshot" => {
                let request =
                    decode::<pb::SnapshotRequest>(&body, &parts.headers, &parts.extensions)?;
                stream(
                    pb::maintenance_server::Maintenance::snapshot(&api, request)
                        .await?
                        .into_inner(),
                )
            }
            "/v3/lease/grant" => call!(pb::LeaseGrantRequest, pb::lease_server::Lease::lease_grant),
            "/v3/lease/revoke" => call!(
                pb::LeaseRevokeRequest,
                pb::lease_server::Lease::lease_revoke
            ),
            "/v3/lease/timetolive" => call!(
                pb::LeaseTimeToLiveRequest,
                pb::lease_server::Lease::lease_time_to_live
            ),
            "/v3/lease/leases" => call!(
                pb::LeaseLeasesRequest,
                pb::lease_server::Lease::lease_leases
            ),
            "/v3/auth/enable" => call!(pb::AuthEnableRequest, pb::auth_server::Auth::auth_enable),
            "/v3/auth/disable" => {
                call!(pb::AuthDisableRequest, pb::auth_server::Auth::auth_disable)
            }
            "/v3/auth/status" => call!(pb::AuthStatusRequest, pb::auth_server::Auth::auth_status),
            "/v3/auth/authenticate" => {
                call!(pb::AuthenticateRequest, pb::auth_server::Auth::authenticate)
            }
            "/v3/auth/user/add" => call!(pb::AuthUserAddRequest, pb::auth_server::Auth::user_add),
            "/v3/auth/user/get" => call!(pb::AuthUserGetRequest, pb::auth_server::Auth::user_get),
            "/v3/auth/user/list" => {
                call!(pb::AuthUserListRequest, pb::auth_server::Auth::user_list)
            }
            "/v3/auth/user/delete" => call!(
                pb::AuthUserDeleteRequest,
                pb::auth_server::Auth::user_delete
            ),
            "/v3/auth/user/changepw" => call!(
                pb::AuthUserChangePasswordRequest,
                pb::auth_server::Auth::user_change_password
            ),
            "/v3/auth/user/grant" => call!(
                pb::AuthUserGrantRoleRequest,
                pb::auth_server::Auth::user_grant_role
            ),
            "/v3/auth/user/revoke" => call!(
                pb::AuthUserRevokeRoleRequest,
                pb::auth_server::Auth::user_revoke_role
            ),
            "/v3/auth/role/add" => call!(pb::AuthRoleAddRequest, pb::auth_server::Auth::role_add),
            "/v3/auth/role/get" => call!(pb::AuthRoleGetRequest, pb::auth_server::Auth::role_get),
            "/v3/auth/role/list" => {
                call!(pb::AuthRoleListRequest, pb::auth_server::Auth::role_list)
            }
            "/v3/auth/role/delete" => call!(
                pb::AuthRoleDeleteRequest,
                pb::auth_server::Auth::role_delete
            ),
            "/v3/auth/role/grant" => call!(
                pb::AuthRoleGrantPermissionRequest,
                pb::auth_server::Auth::role_grant_permission
            ),
            "/v3/auth/role/revoke" => call!(
                pb::AuthRoleRevokePermissionRequest,
                pb::auth_server::Auth::role_revoke_permission
            ),
            "/v3/lock/lock" => call!(lock::LockRequest, lock::lock_server::Lock::lock),
            "/v3/lock/unlock" => call!(lock::UnlockRequest, lock::lock_server::Lock::unlock),
            "/v3/election/campaign" => call!(
                election::CampaignRequest,
                election::election_server::Election::campaign
            ),
            "/v3/election/proclaim" => call!(
                election::ProclaimRequest,
                election::election_server::Election::proclaim
            ),
            "/v3/election/leader" => call!(
                election::LeaderRequest,
                election::election_server::Election::leader
            ),
            "/v3/election/resign" => call!(
                election::ResignRequest,
                election::election_server::Election::resign
            ),
            "/v3/election/observe" => {
                let request =
                    decode::<election::LeaderRequest>(&body, &parts.headers, &parts.extensions)?;
                stream(
                    election::election_server::Election::observe(&api, request)
                        .await?
                        .into_inner(),
                )
            }
            "/v3/watch" => {
                let request = decode::<pb::WatchRequest>(&body, &parts.headers, &parts.extensions)?;
                let principal = api.principal(&request).await?;
                let incoming =
                    tokio_stream::iter([Ok(request.into_inner())]).chain(tokio_stream::pending());
                stream(api.watch_stream(principal, incoming).await?)
            }
            "/v3/lease/keepalive" => {
                let request =
                    decode::<pb::LeaseKeepAliveRequest>(&body, &parts.headers, &parts.extensions)?;
                let principal = api.checked(&request).await?;
                let before = api.runtime.header().await.map_err(|error| error.status())?;
                let now_ms = api
                    .runtime
                    .read(|_, state| state.now_ms())
                    .await
                    .map_err(|error| error.status())?;
                let response = api
                    .runtime
                    .mutate(
                        principal,
                        Mutation::Lease(LeaseCommand::Renew {
                            id: request.into_inner().id,
                            now_ms,
                        }),
                    )
                    .await
                    .map_err(|error| error.status())?;
                let Reply::Lease(LeaseReply::Renew(mut response)) = response else {
                    return Err(Status::internal("keepalive returned an invalid result"));
                };
                response.header = Some(before);
                stream(Box::pin(tokio_stream::iter([Ok(response)])))
            }
            _ => return Err(Status::not_found("gateway route is not supported")),
        })
    }
    .await;
    match result {
        Ok(response) => response,
        Err(error) => failure(error),
    }
}

fn decode<T: DeserializeOwned>(
    bytes: &[u8],
    headers: &HeaderMap,
    extensions: &Extensions,
) -> std::result::Result<Request<T>, Status> {
    let value = serde_json::from_slice(bytes).map_err(|error| {
        Status::invalid_argument(format!(
            "invalid protobuf JSON at line {}, column {}",
            error.line(),
            error.column()
        ))
    })?;
    let mut request = Request::new(value);
    if let Some(token) = headers.get("authorization") {
        let token = token
            .to_str()
            .map_err(|_| Status::unauthenticated("etcdserver: invalid auth token"))?;
        request.metadata_mut().insert(
            "token",
            token
                .parse()
                .map_err(|_| Status::unauthenticated("etcdserver: invalid auth token"))?,
        );
    }
    if let Some(tls) = extensions.get::<TlsConnectInfo<TcpConnectInfo>>() {
        request.extensions_mut().insert(tls.clone());
    }
    Ok(request)
}

fn output<T: Message + Serialize>(value: T) -> std::result::Result<Response, Status> {
    let value = wire::protobuf_json(&value).map_err(|error| error.status())?;
    Ok(Json(value).into_response())
}

fn stream<T: Message + Serialize + 'static>(responses: ResponseStream<T>) -> Response {
    let body = Body::from_stream(responses.map(|response| {
        let value = match response {
            Ok(value) => match wire::protobuf_json(&value) {
                Ok(value) => json!({ "result": value }),
                Err(error) => json!({ "error": { "code": error.status().code() as i32, "message": error.status().message() } }),
            },
            Err(error) => json!({ "error": { "code": error.code() as i32, "message": error.message() } }),
        };
        serde_json::to_vec(&value).map(|mut bytes| { bytes.push(b'\n'); Bytes::from(bytes) }).map_err(std::io::Error::other)
    }));
    ([("content-type", "application/json")], body).into_response()
}

fn failure(error: Status) -> Response {
    let status = match error.code() {
        tonic::Code::InvalidArgument | tonic::Code::OutOfRange => StatusCode::BAD_REQUEST,
        tonic::Code::Unauthenticated => StatusCode::UNAUTHORIZED,
        tonic::Code::PermissionDenied => StatusCode::FORBIDDEN,
        tonic::Code::NotFound => StatusCode::NOT_FOUND,
        tonic::Code::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
        tonic::Code::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        tonic::Code::DeadlineExceeded => StatusCode::GATEWAY_TIMEOUT,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, Json(json!({ "error": error.message(), "code": error.code() as i32, "message": error.message() }))).into_response()
}

async fn readiness(api: &Api) -> crate::error::Result<()> {
    api.runtime.barrier().await?;
    api.runtime.read(|store, _| store.read_health()).await
}

async fn health(State(api): State<Api>) -> Response {
    match readiness(&api).await {
        Ok(()) => Json(json!({ "health": "true" })).into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "health": "false", "reason": error.status().message() })),
        )
            .into_response(),
    }
}

async fn ready(State(api): State<Api>) -> Response {
    match readiness(&api).await {
        Ok(()) => "ok".into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            error.status().message().to_owned(),
        )
            .into_response(),
    }
}

async fn version(State(api): State<Api>) -> Response {
    match api.runtime.read(|store, _| Ok((store.protocol_version().to_owned(), store.schema_version()?, store.identity_registered()?))).await {
        Ok((profile, schema, registered)) => Json(json!({
            "etcdserver": wire::ETCD_API_VERSION, "etcdcluster": profile,
            "etcqlite": { "version": wire::ENGINE_VERSION, "commit": wire::BUILD_SHA, "dirty": wire::BUILD_DIRTY,
                "schema_version": schema, "identity_registered": registered, "quota_kind": "logical-mvcc-estimate" }
        })).into_response(),
        Err(error) => failure(error.status()),
    }
}

async fn metrics(State(api): State<Api>) -> Response {
    let ready = match readiness(&api).await {
        Ok(()) => 1,
        Err(error) => {
            tracing::debug!(error = %error, "readiness gauge is false");
            0
        }
    };
    match api
        .runtime
        .read(|store, _| {
            Ok((
                store.revision(),
                store.auth_revision(),
                store.applied_index(),
                store.sizes()?,
            ))
        })
        .await
    {
        Ok((revision, auth_revision, applied, (size, used))) => {
            let body = format!(
                "etcqlite_ready {ready}\netcqlite_writer_running 1\netcqlite_mvcc_revision {revision}\netcqlite_auth_revision {auth_revision}\netcqlite_applied_index {applied}\netcqlite_db_size_bytes {size}\netcqlite_db_in_use_bytes {used}\n"
            );
            ([("content-type", "text/plain; version=0.0.4")], body).into_response()
        }
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            error.status().message().to_owned(),
        )
            .into_response(),
    }
}
