use super::{Api, fill_header};
use crate::{
    error::RpcError,
    storage::{
        auth::Principal,
        command::{Mutation, Reply},
        membership::{ClusterReply, MembershipCommand, MembershipReply},
    },
    wire::etcdserverpb as pb,
};
use tonic::{Code, Request, Response, Status};
use uuid::Uuid;

impl Api {
    async fn membership(
        &self,
        principal: Principal,
        command: MembershipCommand,
    ) -> Result<MembershipReply, Status> {
        match self
            .runtime
            .mutate(principal, Mutation::Membership(command))
            .await
            .map_err(|error| error.status())?
        {
            Reply::Membership(response) => Ok(response),
            _ => Err(Status::internal(
                "membership command returned a different result",
            )),
        }
    }
}

#[tonic::async_trait]
impl pb::cluster_server::Cluster for Api {
    async fn member_list(
        &self,
        request: Request<pb::MemberListRequest>,
    ) -> Result<Response<pb::MemberListResponse>, Status> {
        let principal = self.checked(&request).await?;
        if request.get_ref().linearizable {
            self.runtime
                .barrier()
                .await
                .map_err(|error| error.status())?;
        }
        let response = self
            .runtime
            .read(move |store, state| {
                let mut response = store.members(&principal)?;
                fill_header(&mut response.header, state.header(store));
                Ok(response)
            })
            .await
            .map_err(|error| error.status())?;
        self.response(response)
    }

    async fn member_add(
        &self,
        request: Request<pb::MemberAddRequest>,
    ) -> Result<Response<pb::MemberAddResponse>, Status> {
        let principal = self.checked(&request).await?;
        let request = request.into_inner();
        let bytes: [u8; 8] = Uuid::new_v4().as_bytes()[..8]
            .try_into()
            .map_err(|_| Status::internal("member ID allocation failed"))?;
        let command = MembershipCommand::Add {
            id: u64::from_be_bytes(bytes).max(1),
            peer_urls: request.peer_ur_ls,
            learner: request.is_learner,
        };
        match self.membership(principal, command).await?.response {
            ClusterReply::Add(response) => self.response(response),
            _ => Err(Status::internal("invalid member-add result")),
        }
    }

    async fn member_remove(
        &self,
        request: Request<pb::MemberRemoveRequest>,
    ) -> Result<Response<pb::MemberRemoveResponse>, Status> {
        let principal = self.checked(&request).await?;
        match self
            .membership(
                principal,
                MembershipCommand::Remove {
                    id: request.into_inner().id,
                },
            )
            .await?
            .response
        {
            ClusterReply::Remove(response) => self.response(response),
            _ => Err(Status::internal("invalid member-remove result")),
        }
    }

    async fn member_update(
        &self,
        request: Request<pb::MemberUpdateRequest>,
    ) -> Result<Response<pb::MemberUpdateResponse>, Status> {
        let principal = self.checked(&request).await?;
        let request = request.into_inner();
        match self
            .membership(
                principal,
                MembershipCommand::Update {
                    id: request.id,
                    peer_urls: request.peer_ur_ls,
                },
            )
            .await?
            .response
        {
            ClusterReply::Update(response) => self.response(response),
            _ => Err(Status::internal("invalid member-update result")),
        }
    }

    async fn member_promote(
        &self,
        request: Request<pb::MemberPromoteRequest>,
    ) -> Result<Response<pb::MemberPromoteResponse>, Status> {
        let principal = self.checked(&request).await?;
        let id = request.into_inner().id;
        self.runtime
            .barrier()
            .await
            .map_err(|error| error.status())?;
        let checked = principal.clone();
        let (configuration, requester, applied) = self
            .runtime
            .read(move |store, state| {
                store.require_admin(&checked)?;
                let configuration = state.cluster.clone().ok_or_else(|| {
                    RpcError::new(Code::FailedPrecondition, "local mode has no learners")
                })?;
                if !configuration.member(id)?.learner {
                    return Err(RpcError::new(
                        Code::FailedPrecondition,
                        "etcdserver: can only promote a learner member",
                    ));
                }
                Ok((
                    configuration,
                    store.identity().member_id,
                    store.applied_index(),
                ))
            })
            .await
            .map_err(|error| error.status())?;
        let options = self
            .peer_tls
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("learner verification requires peer TLS"))?;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        let progress = loop {
            match crate::peer::progress(configuration.clone(), requester, id, options).await {
                Ok(progress)
                    if progress.learner
                        && progress.configuration_id == configuration.configuration_id
                        && progress.applied_index >= applied =>
                {
                    break progress;
                }
                Ok(_) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await
                }
                Ok(_) => {
                    return Err(Status::failed_precondition(
                        "etcdserver: can only promote a learner member which is in sync with leader",
                    ));
                }
                Err(error) => {
                    tracing::warn!(member_id = id, error = %error, "learner promotion cannot verify catch-up");
                    if matches!(
                        error.status().code(),
                        Code::Unavailable | Code::DeadlineExceeded
                    ) {
                        return Err(Status::failed_precondition(
                            "etcdserver: can only promote a learner member which is in sync with leader",
                        ));
                    }
                    return Err(error.status());
                }
            }
        };
        match self
            .membership(
                principal,
                MembershipCommand::Promote {
                    id,
                    configuration_id: progress.configuration_id,
                    applied_index: progress.applied_index,
                },
            )
            .await?
            .response
        {
            ClusterReply::Promote(response) => self.response(response),
            _ => Err(Status::internal("invalid member-promote result")),
        }
    }
}
