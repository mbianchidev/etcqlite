use super::{Api, ResponseStream, fill_header};
use crate::{
    storage::{
        auth::Principal,
        command::{Mutation, Reply},
        lease::{LeaseCommand, LeaseReply},
    },
    wire::etcdserverpb as pb,
};
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tonic::{Request, Response, Status};

impl Api {
    async fn lease_mutation(
        &self,
        principal: Principal,
        request: LeaseCommand,
    ) -> std::result::Result<LeaseReply, Status> {
        match self
            .runtime
            .mutate(principal, Mutation::Lease(request))
            .await
            .map_err(|error| error.status())?
        {
            Reply::Lease(reply) => Ok(reply),
            _ => Err(Status::internal(
                "lease command returned the wrong reply kind",
            )),
        }
    }
}

#[tonic::async_trait]
impl pb::lease_server::Lease for Api {
    async fn lease_grant(
        &self,
        request: Request<pb::LeaseGrantRequest>,
    ) -> std::result::Result<Response<pb::LeaseGrantResponse>, Status> {
        let principal = self.checked(&request).await?;
        let request = request.into_inner();
        let id = if request.id == 0 {
            super::new_lease_id()?
        } else {
            request.id
        };
        let now_ms = self
            .runtime
            .read(|_, state| state.now_ms())
            .await
            .map_err(|error| error.status())?;
        match self
            .lease_mutation(
                principal,
                LeaseCommand::Grant {
                    id,
                    ttl: request.ttl,
                    now_ms,
                },
            )
            .await?
        {
            LeaseReply::Grant(reply) => self.response(reply),
            _ => Err(Status::internal("invalid lease grant reply")),
        }
    }

    async fn lease_revoke(
        &self,
        request: Request<pb::LeaseRevokeRequest>,
    ) -> std::result::Result<Response<pb::LeaseRevokeResponse>, Status> {
        let principal = self.checked(&request).await?;
        match self
            .lease_mutation(
                principal,
                LeaseCommand::Revoke {
                    id: request.into_inner().id,
                },
            )
            .await?
        {
            LeaseReply::Revoke(reply) => self.response(reply),
            _ => Err(Status::internal("invalid lease revoke reply")),
        }
    }

    type LeaseKeepAliveStream = ResponseStream<pb::LeaseKeepAliveResponse>;
    async fn lease_keep_alive(
        &self,
        request: Request<tonic::Streaming<pb::LeaseKeepAliveRequest>>,
    ) -> std::result::Result<Response<Self::LeaseKeepAliveStream>, Status> {
        let principal = self.principal(&request).await?;
        let mut incoming = request.into_inner();
        let (sender, receiver) = tokio::sync::mpsc::channel(16);
        let api = self.clone();
        tokio::spawn(async move {
            let result: std::result::Result<(), Status> = async {
                while let Some(request) = incoming.next().await {
                    let request = request?;
                    let before = api.runtime.header().await.map_err(|error| error.status())?;
                    let now_ms = api
                        .runtime
                        .read(|_, state| state.now_ms())
                        .await
                        .map_err(|error| error.status())?;
                    let mut response = match api
                        .lease_mutation(
                            principal.clone(),
                            LeaseCommand::Renew {
                                id: request.id,
                                now_ms,
                            },
                        )
                        .await?
                    {
                        LeaseReply::Renew(reply) => reply,
                        _ => return Err(Status::internal("invalid lease renewal reply")),
                    };
                    response.header = Some(before);
                    if sender.send(Ok(response)).await.is_err() {
                        return Ok(());
                    }
                }
                Ok(())
            }
            .await;
            if let Err(error) = result
                && sender.send(Err(error)).await.is_err()
            {
                tracing::debug!("keepalive stream error response abandoned after cancellation");
            }
        });
        self.response(Box::pin(ReceiverStream::new(receiver)) as Self::LeaseKeepAliveStream)
    }

    async fn lease_time_to_live(
        &self,
        request: Request<pb::LeaseTimeToLiveRequest>,
    ) -> std::result::Result<Response<pb::LeaseTimeToLiveResponse>, Status> {
        let principal = self.checked(&request).await?;
        let request = request.into_inner();
        self.runtime
            .barrier()
            .await
            .map_err(|error| error.status())?;
        let response = self
            .runtime
            .read(move |store, state| {
                if request.keys {
                    store.authorize_lease(&principal, request.id, false)?;
                } else {
                    store.require_authenticated(&principal)?;
                }
                let mut response = if let Some(lease) = store.lease(request.id)? {
                    pb::LeaseTimeToLiveResponse {
                        header: Some(state.header(store)),
                        id: request.id,
                        ttl: state.remaining_ttl(&lease)?,
                        granted_ttl: lease.ttl,
                        keys: if request.keys {
                            store.lease_keys(request.id)?
                        } else {
                            Vec::new()
                        },
                    }
                } else {
                    pb::LeaseTimeToLiveResponse {
                        header: Some(state.header(store)),
                        id: request.id,
                        ttl: -1,
                        ..Default::default()
                    }
                };
                fill_header(&mut response.header, state.header(store));
                Ok(response)
            })
            .await
            .map_err(|error| error.status())?;
        self.response(response)
    }

    async fn lease_leases(
        &self,
        request: Request<pb::LeaseLeasesRequest>,
    ) -> std::result::Result<Response<pb::LeaseLeasesResponse>, Status> {
        let principal = self.checked(&request).await?;
        let response = self
            .runtime
            .read(move |store, state| {
                let mut response = store.lease_list(&principal)?;
                fill_header(&mut response.header, state.header(store));
                Ok(response)
            })
            .await
            .map_err(|error| error.status())?;
        self.response(response)
    }
}
