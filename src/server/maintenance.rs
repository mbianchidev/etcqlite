use super::{Api, ResponseStream, fill_header};
use crate::{
    error::RpcError,
    storage::{
        command::{Mutation, Reply},
        maintenance::{MaintenanceCommand, MaintenanceReply},
    },
    wire::{self, etcdserverpb as pb},
};
use tokio::io::AsyncReadExt;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Request, Response, Status};

#[tonic::async_trait]
impl pb::maintenance_server::Maintenance for Api {
    async fn status(
        &self,
        request: Request<pb::StatusRequest>,
    ) -> Result<Response<pb::StatusResponse>, Status> {
        let principal = self.checked(&request).await?;
        let response = self
            .runtime
            .read(move |store, state| {
                store.require_admin(&principal)?;
                let (allocated, used) = store.sizes()?;
                Ok(pb::StatusResponse {
                    header: Some(state.header(store)),
                    version: wire::ETCD_API_VERSION.into(),
                    db_size: allocated,
                    db_size_in_use: used,
                    db_size_quota: i64::try_from(store.quota_bytes())
                        .map_err(|_| RpcError::corrupt("quota exceeds wire range"))?,
                    leader: state.leader_id.unwrap_or(0),
                    raft_term: state.epoch,
                    raft_index: state.decided_index,
                    raft_applied_index: store.applied_index(),
                    is_learner: state
                        .cluster
                        .as_ref()
                        .map(|configuration| {
                            configuration
                                .member(store.identity().member_id)
                                .map(|member| member.learner)
                        })
                        .transpose()?
                        .unwrap_or(false),
                    storage_version: store.protocol_version().into(),
                    downgrade_info: store.downgrade_target().map(|target| pb::DowngradeInfo {
                        enabled: true,
                        target_version: target.to_owned(),
                    }),
                    errors: {
                        let mut errors = store
                            .alarms(&principal, &pb::AlarmRequest::default())?
                            .alarms
                            .into_iter()
                            .map(|alarm| {
                                format!(
                                    "etcdserver: {} alarm on member {:x}",
                                    if alarm.alarm == 1 {
                                        "NOSPACE"
                                    } else {
                                        "CORRUPT"
                                    },
                                    alarm.member_id
                                )
                            })
                            .collect::<Vec<_>>();
                        if state.leader_id.is_none() {
                            errors.push("etcdserver: no leader".into());
                        }
                        errors
                    },
                })
            })
            .await
            .map_err(|error| error.status())?;
        self.response(response)
    }

    async fn defragment(
        &self,
        request: Request<pb::DefragmentRequest>,
    ) -> Result<Response<pb::DefragmentResponse>, Status> {
        let principal = self.checked(&request).await?;
        let response = self
            .runtime
            .maintenance(move |store, state| {
                let mut response = store.defragment(&principal)?;
                fill_header(&mut response.header, state.header(store));
                Ok(response)
            })
            .await
            .map_err(|error| error.status())?;
        self.response(response)
    }

    async fn move_leader(
        &self,
        request: Request<pb::MoveLeaderRequest>,
    ) -> Result<Response<pb::MoveLeaderResponse>, Status> {
        let principal = self.checked(&request).await?;
        let target = request.into_inner().target_id;
        self.runtime
            .barrier()
            .await
            .map_err(|error| error.status())?;
        let checked = principal.clone();
        let (configuration, requester, header) = self
            .runtime
            .read(move |store, state| {
                store.require_admin(&checked)?;
                if state.leader_id != Some(store.identity().member_id) {
                    return Err(RpcError::new(
                        Code::FailedPrecondition,
                        "etcdserver: not leader",
                    ));
                }
                Ok((
                    state.cluster.clone(),
                    store.identity().member_id,
                    state.header(store),
                ))
            })
            .await
            .map_err(|error| error.status())?;
        if target == requester {
            return self.response(pb::MoveLeaderResponse {
                header: Some(header),
            });
        }
        let configuration = configuration
            .ok_or_else(|| Status::failed_precondition("local mode has no peer leadership"))?;
        let options = self
            .peer_tls
            .clone()
            .ok_or_else(|| Status::failed_precondition("peer TLS is not configured"))?;
        let receipt =
            crate::peer::transfer_leader(configuration, requester, target, principal, options)
                .await
                .map_err(|error| error.status())?;
        self.response(pb::MoveLeaderResponse {
            header: Some(pb::ResponseHeader {
                raft_term: receipt.term,
                ..header
            }),
        })
    }

    async fn alarm(
        &self,
        request: Request<pb::AlarmRequest>,
    ) -> Result<Response<pb::AlarmResponse>, Status> {
        let principal = self.checked(&request).await?;
        let request = request.into_inner();
        if request.action == 0 {
            let response = self
                .runtime
                .read(move |store, state| {
                    let mut response = store.alarms(&principal, &request)?;
                    fill_header(&mut response.header, state.header(store));
                    Ok(response)
                })
                .await
                .map_err(|error| error.status())?;
            return self.response(response);
        }
        match self
            .runtime
            .mutate(
                principal,
                Mutation::Maintenance(MaintenanceCommand::Alarm(request)),
            )
            .await
            .map_err(|error| error.status())?
        {
            Reply::Maintenance(MaintenanceReply::Alarm(response)) => self.response(response),
            _ => Err(Status::internal(
                "alarm command returned a different result",
            )),
        }
    }
    async fn hash(
        &self,
        request: Request<pb::HashRequest>,
    ) -> Result<Response<pb::HashResponse>, Status> {
        let principal = self.checked(&request).await?;
        let response = self
            .runtime
            .maintenance_read(principal, move |store, header| {
                Ok(pb::HashResponse {
                    hash: store.backend_hash(
                        &crate::storage::auth::Principal::Internal,
                        header.raft_term,
                    )?,
                    header: Some(header),
                })
            })
            .await
            .map_err(|error| error.status())?;
        self.response(response)
    }
    async fn hash_kv(
        &self,
        request: Request<pb::HashKvRequest>,
    ) -> Result<Response<pb::HashKvResponse>, Status> {
        let principal = self.checked(&request).await?;
        let revision = request.into_inner().revision;
        let response = self
            .runtime
            .maintenance_read(principal, move |store, header| {
                let mut response =
                    store.hash_kv(&crate::storage::auth::Principal::Internal, revision)?;
                fill_header(&mut response.header, header);
                Ok(response)
            })
            .await
            .map_err(|error| error.status())?;
        self.response(response)
    }
    type SnapshotStream = ResponseStream<pb::SnapshotResponse>;
    async fn snapshot(
        &self,
        request: Request<pb::SnapshotRequest>,
    ) -> Result<Response<Self::SnapshotStream>, Status> {
        let principal = self.checked(&request).await?;
        let permit = self
            .snapshot_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("snapshot transfers are at capacity"))?;
        self.runtime
            .barrier()
            .await
            .map_err(|error| error.status())?;
        let (snapshot, header) = self
            .runtime
            .native_snapshot(principal)
            .await
            .map_err(|error| error.status())?;
        let directory = tempfile::tempdir()
            .map_err(|error| Status::internal(format!("snapshot staging: {error}")))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))
                .map_err(|error| {
                    Status::internal(format!("snapshot staging permissions: {error}"))
                })?;
        }
        let image = directory.path().join("snapshot.db");
        let mut command = tokio::process::Command::new(&self.snapshot_tool);
        command
            .arg("export-native")
            .arg(snapshot.path())
            .arg(&image)
            .kill_on_drop(true);
        let output = tokio::time::timeout(std::time::Duration::from_secs(600), command.output()).await
            .map_err(|_| Status::deadline_exceeded("upstream snapshot conversion exceeded its deadline"))?
            .map_err(|error| Status::failed_precondition(format!("snapshot converter is unavailable: {error}; install the companion tool or pass --snapshot-tool")))?;
        if !output.status.success() {
            let message = String::from_utf8_lossy(&output.stderr);
            tracing::error!(status = %output.status, "upstream snapshot conversion failed");
            return Err(Status::internal(format!(
                "upstream snapshot conversion failed: {}",
                message.chars().take(2_000).collect::<String>()
            )));
        }
        let total = tokio::fs::metadata(&image)
            .await
            .map_err(|error| Status::internal(format!("converted snapshot metadata: {error}")))?
            .len();
        if total < 544 || total % 512 != 32 {
            return Err(Status::data_loss(
                "converter produced an invalid upstream snapshot shape",
            ));
        }
        let (sender, receiver) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            let _permit = permit;
            let _directory = directory;
            let result: Result<(), Status> = async {
                let mut file = tokio::fs::File::open(image).await.map_err(|error| {
                    Status::internal(format!("converted snapshot open: {error}"))
                })?;
                let mut offset = 0;
                while offset < total {
                    let count = usize::try_from((total - offset).min(1 << 20))
                        .map_err(|_| Status::internal("snapshot chunk length overflow"))?;
                    let mut blob = vec![0; count];
                    file.read_exact(&mut blob).await.map_err(|error| {
                        Status::internal(format!("converted snapshot read: {error}"))
                    })?;
                    let response = pb::SnapshotResponse {
                        header: if offset == 0 { Some(header) } else { None },
                        remaining_bytes: total - offset - count as u64,
                        blob,
                        version: if offset == 0 {
                            wire::ETCD_API_VERSION.into()
                        } else {
                            String::new()
                        },
                    };
                    if sender.send(Ok(response)).await.is_err() {
                        return Ok(());
                    }
                    offset += count as u64;
                }
                Ok(())
            }
            .await;
            if let Err(error) = result
                && sender.send(Err(error)).await.is_err()
            {
                tracing::debug!("snapshot error waiter canceled");
            }
        });
        self.response(Box::pin(ReceiverStream::new(receiver)) as Self::SnapshotStream)
    }
    async fn downgrade(
        &self,
        request: Request<pb::DowngradeRequest>,
    ) -> Result<Response<pb::DowngradeResponse>, Status> {
        let principal = self.checked(&request).await?;
        let request = request.into_inner();
        if request.action == 0 {
            self.runtime
                .barrier()
                .await
                .map_err(|error| error.status())?;
            let response = self
                .runtime
                .read(move |store, state| {
                    let mut response = store.downgrade_validate(&principal, &request.version)?;
                    fill_header(&mut response.header, state.header(store));
                    Ok(response)
                })
                .await
                .map_err(|error| error.status())?;
            return self.response(response);
        }
        match self
            .runtime
            .mutate(
                principal,
                Mutation::Maintenance(MaintenanceCommand::Downgrade(request)),
            )
            .await
            .map_err(|error| error.status())?
        {
            Reply::Maintenance(MaintenanceReply::Downgrade(response)) => self.response(response),
            _ => Err(Status::internal(
                "downgrade command returned a different result",
            )),
        }
    }
}
