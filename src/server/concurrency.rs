use super::{Api, ResponseStream};
use crate::{
    storage::{KvReply, KvRequest, auth::Principal, command::Mutation, lease::LeaseCommand},
    wire::{etcdserverpb as pb, mvccpb::KeyValue, v3electionpb as election, v3lockpb as lock},
};
use std::time::Duration;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

struct Candidate {
    key: Vec<u8>,
    revision: i64,
    lease: i64,
    header: Option<pb::ResponseHeader>,
}

struct PendingCandidate {
    api: Api,
    principal: Principal,
    key: Vec<u8>,
    revision: i64,
    armed: bool,
}

impl Drop for PendingCandidate {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let api = self.api.clone();
        let principal = self.principal.clone();
        let key = self.key.clone();
        let revision = self.revision;
        tokio::spawn(async move {
            let request = pb::TxnRequest {
                compare: vec![compare_create(&key, revision)],
                success: vec![pb::RequestOp {
                    request: Some(pb::request_op::Request::RequestDeleteRange(
                        pb::DeleteRangeRequest {
                            key,
                            ..Default::default()
                        },
                    )),
                }],
                ..Default::default()
            };
            if let Err(error) = api.kv_mutation(principal, KvRequest::Txn(request)).await {
                tracing::warn!(code = ?error.code(), "canceled concurrency candidate cleanup failed; lease expiry remains the fallback");
            }
        });
    }
}

impl Api {
    async fn candidate(
        &self,
        principal: Principal,
        name: &[u8],
        lease: i64,
        value: Vec<u8>,
        with_owner: bool,
    ) -> std::result::Result<(Candidate, bool), Status> {
        let lease = if lease == 0 {
            let id = super::new_lease_id()?;
            let now_ms = self
                .runtime
                .read(|_, state| state.now_ms())
                .await
                .map_err(|error| error.status())?;
            self.runtime
                .mutate(
                    principal.clone(),
                    Mutation::Lease(LeaseCommand::Grant {
                        id,
                        ttl: 60,
                        now_ms,
                    }),
                )
                .await
                .map_err(|error| error.status())?;
            id
        } else {
            lease
        };
        let prefix = prefix(name);
        let mut key = prefix.clone();
        let suffix = if lease < 0 {
            format!("-{:x}", lease.unsigned_abs())
        } else {
            format!("{lease:x}")
        };
        key.extend_from_slice(suffix.as_bytes());
        let mut success = vec![pb::RequestOp {
            request: Some(pb::request_op::Request::RequestPut(pb::PutRequest {
                key: key.clone(),
                value,
                lease,
                ..Default::default()
            })),
        }];
        let mut failure = vec![range_op(pb::RangeRequest {
            key: key.clone(),
            ..Default::default()
        })];
        if with_owner {
            let owner = range_op(first(&prefix, 0));
            success.push(owner.clone());
            failure.push(owner);
        }
        let response = match self
            .kv_mutation(
                principal,
                KvRequest::Txn(pb::TxnRequest {
                    compare: vec![compare_create(&key, 0)],
                    success,
                    failure,
                }),
            )
            .await?
        {
            KvReply::Txn(response) => response,
            _ => {
                return Err(Status::internal(
                    "candidate transaction returned the wrong reply",
                ));
            }
        };
        let revision = if response.succeeded {
            response
                .header
                .as_ref()
                .ok_or_else(|| Status::internal("candidate header missing"))?
                .revision
        } else {
            match response
                .responses
                .first()
                .and_then(|response| response.response.as_ref())
            {
                Some(pb::response_op::Response::ResponseRange(response)) => {
                    response
                        .kvs
                        .first()
                        .ok_or_else(|| Status::internal("reused candidate key missing"))?
                        .create_revision
                }
                _ => {
                    return Err(Status::internal(
                        "candidate reuse returned an invalid response",
                    ));
                }
            }
        };
        let immediate = if with_owner {
            match response
                .responses
                .get(1)
                .and_then(|response| response.response.as_ref())
            {
                Some(pb::response_op::Response::ResponseRange(owner)) => owner
                    .kvs
                    .first()
                    .is_none_or(|kv| kv.create_revision == revision),
                _ => return Err(Status::internal("candidate owner response missing")),
            }
        } else {
            false
        };
        Ok((
            Candidate {
                key,
                revision,
                lease,
                header: response.header,
            },
            immediate,
        ))
    }

    async fn wait_candidate(
        &self,
        principal: Principal,
        name: &[u8],
        candidate: &Candidate,
    ) -> std::result::Result<pb::ResponseHeader, Status> {
        let prefix = prefix(name);
        loop {
            let notification = self.runtime.changes.notified();
            tokio::pin!(notification);
            notification.as_mut().enable();
            self.runtime
                .barrier()
                .await
                .map_err(|error| error.status())?;
            let current = self
                .range_snapshot(
                    principal.clone(),
                    pb::RangeRequest {
                        key: candidate.key.clone(),
                        ..Default::default()
                    },
                )
                .await?;
            let Some(key) = current.kvs.first() else {
                return Err(Status::unknown("mutex: session is expired"));
            };
            if key.create_revision != candidate.revision || key.lease != candidate.lease {
                return Err(Status::unknown("mutex: session is expired"));
            }
            let owner = self
                .range_snapshot(principal.clone(), first(&prefix, 0))
                .await?;
            if owner
                .kvs
                .first()
                .is_some_and(|key| key.create_revision == candidate.revision)
            {
                return current
                    .header
                    .ok_or_else(|| Status::internal("candidate read header missing"));
            }
            tokio::select! {
                _ = &mut notification => {},
                _ = tokio::time::sleep(Duration::from_millis(100)) => {},
            }
        }
    }

    async fn leader_snapshot(
        &self,
        principal: Principal,
        name: &[u8],
        revision: i64,
    ) -> std::result::Result<pb::RangeResponse, Status> {
        self.range_snapshot(principal, first(&prefix(name), revision))
            .await
    }

    async fn proclaim_value(
        &self,
        principal: Principal,
        leader: &election::LeaderKey,
        value: Vec<u8>,
    ) -> std::result::Result<pb::TxnResponse, Status> {
        let result = self
            .kv_mutation(
                principal,
                KvRequest::Txn(pb::TxnRequest {
                    compare: vec![compare_create(&leader.key, leader.rev)],
                    success: vec![pb::RequestOp {
                        request: Some(pb::request_op::Request::RequestPut(pb::PutRequest {
                            key: leader.key.clone(),
                            value,
                            lease: leader.lease,
                            ..Default::default()
                        })),
                    }],
                    ..Default::default()
                }),
            )
            .await?;
        match result {
            KvReply::Txn(response) if response.succeeded => Ok(response),
            KvReply::Txn(_) => Err(Status::unknown("election: not leader")),
            _ => Err(Status::internal("proclamation returned an invalid reply")),
        }
    }
}

#[tonic::async_trait]
impl lock::lock_server::Lock for Api {
    async fn lock(
        &self,
        request: Request<lock::LockRequest>,
    ) -> std::result::Result<Response<lock::LockResponse>, Status> {
        let principal = self.checked(&request).await?;
        let request = request.into_inner();
        let (candidate, immediate) = self
            .candidate(
                principal.clone(),
                &request.name,
                request.lease,
                Vec::new(),
                true,
            )
            .await?;
        let mut cleanup = PendingCandidate {
            api: self.clone(),
            principal: principal.clone(),
            key: candidate.key.clone(),
            revision: candidate.revision,
            armed: true,
        };
        let header = if immediate {
            candidate.header
        } else {
            Some(
                self.wait_candidate(principal, &request.name, &candidate)
                    .await?,
            )
        };
        cleanup.armed = false;
        self.response(lock::LockResponse {
            header,
            key: candidate.key,
        })
    }

    async fn unlock(
        &self,
        request: Request<lock::UnlockRequest>,
    ) -> std::result::Result<Response<lock::UnlockResponse>, Status> {
        let principal = self.checked(&request).await?;
        match self
            .kv_mutation(
                principal,
                KvRequest::Delete(pb::DeleteRangeRequest {
                    key: request.into_inner().key,
                    ..Default::default()
                }),
            )
            .await?
        {
            KvReply::Delete(response) => self.response(lock::UnlockResponse {
                header: response.header,
            }),
            _ => Err(Status::internal("unlock returned an invalid reply")),
        }
    }
}

#[tonic::async_trait]
impl election::election_server::Election for Api {
    async fn campaign(
        &self,
        request: Request<election::CampaignRequest>,
    ) -> std::result::Result<Response<election::CampaignResponse>, Status> {
        let principal = self.checked(&request).await?;
        let request = request.into_inner();
        let (mut candidate, _) = self
            .candidate(
                principal.clone(),
                &request.name,
                request.lease,
                request.value.clone(),
                false,
            )
            .await?;
        let mut cleanup = PendingCandidate {
            api: self.clone(),
            principal: principal.clone(),
            key: candidate.key.clone(),
            revision: candidate.revision,
            armed: true,
        };
        let own = self
            .range_snapshot(
                principal.clone(),
                pb::RangeRequest {
                    key: candidate.key.clone(),
                    ..Default::default()
                },
            )
            .await?;
        if own.kvs.first().is_some_and(|kv| kv.value != request.value) {
            let leader = election::LeaderKey {
                name: request.name.clone(),
                key: candidate.key.clone(),
                rev: candidate.revision,
                lease: candidate.lease,
            };
            candidate.header = self
                .proclaim_value(principal.clone(), &leader, request.value)
                .await?
                .header;
        }
        self.wait_candidate(principal, &request.name, &candidate)
            .await?;
        cleanup.armed = false;
        self.response(election::CampaignResponse {
            header: candidate.header,
            leader: Some(election::LeaderKey {
                name: request.name,
                key: candidate.key,
                rev: candidate.revision,
                lease: candidate.lease,
            }),
        })
    }

    async fn proclaim(
        &self,
        request: Request<election::ProclaimRequest>,
    ) -> std::result::Result<Response<election::ProclaimResponse>, Status> {
        let principal = self.checked(&request).await?;
        let request = request.into_inner();
        let leader = request
            .leader
            .ok_or_else(|| Status::unknown("\"leader\" field must be provided"))?;
        let reply = self
            .proclaim_value(principal, &leader, request.value)
            .await?;
        self.response(election::ProclaimResponse {
            header: reply.header,
        })
    }

    async fn leader(
        &self,
        request: Request<election::LeaderRequest>,
    ) -> std::result::Result<Response<election::LeaderResponse>, Status> {
        let principal = self.checked(&request).await?;
        let request = request.into_inner();
        self.runtime
            .barrier()
            .await
            .map_err(|error| error.status())?;
        let mut reply = self.leader_snapshot(principal, &request.name, 0).await?;
        if reply.kvs.is_empty() {
            return Err(Status::unknown("election: no leader"));
        }
        self.response(election::LeaderResponse {
            header: reply.header,
            kv: Some(reply.kvs.remove(0)),
        })
    }

    type ObserveStream = ResponseStream<election::LeaderResponse>;
    async fn observe(
        &self,
        request: Request<election::LeaderRequest>,
    ) -> std::result::Result<Response<Self::ObserveStream>, Status> {
        let principal = self.checked(&request).await?;
        let name = request.into_inner().name;
        let key_prefix = prefix(&name);
        let (sender, receiver) = tokio::sync::mpsc::channel(8);
        let api = self.clone();
        tokio::spawn(async move {
            let result: std::result::Result<(), Status> = async {
                api.runtime
                    .barrier()
                    .await
                    .map_err(|error| error.status())?;
                let initial = api.leader_snapshot(principal.clone(), &name, 0).await?;
                let mut revision = initial
                    .header
                    .as_ref()
                    .ok_or_else(|| Status::internal("observer header missing"))?
                    .revision;
                let mut last: Option<KeyValue> = initial.kvs.first().cloned();
                if let Some(kv) = &last
                    && sender
                        .send(Ok(election::LeaderResponse {
                            header: initial.header,
                            kv: Some(kv.clone()),
                        }))
                        .await
                        .is_err()
                {
                    return Ok(());
                }
                loop {
                    let notification = api.runtime.changes.notified();
                    tokio::pin!(notification);
                    notification.as_mut().enable();
                    let scope = principal.clone();
                    let prefix = key_prefix.clone();
                    let history = api
                        .runtime
                        .read(move |store, state| {
                            store.authorize_watch(&scope, &prefix, &prefix_end(&prefix))?;
                            Ok((
                                state.header(store),
                                store.events(revision + 1, &prefix, &prefix_end(&prefix))?,
                            ))
                        })
                        .await
                        .map_err(|error| error.status())?;
                    for (event_revision, _) in history.1 {
                        let mut response = api
                            .leader_snapshot(principal.clone(), &name, event_revision)
                            .await?;
                        let current = response.kvs.first().cloned();
                        if current != last {
                            if let Some(kv) = &current {
                                if let Some(header) = &mut response.header {
                                    header.revision = event_revision;
                                }
                                if sender
                                    .send(Ok(election::LeaderResponse {
                                        header: response.header,
                                        kv: Some(kv.clone()),
                                    }))
                                    .await
                                    .is_err()
                                {
                                    return Ok(());
                                }
                            }
                            last = current;
                        }
                    }
                    revision = history.0.revision;
                    tokio::select! {
                        _ = sender.closed() => return Ok(()),
                        _ = &mut notification => {},
                        _ = tokio::time::sleep(Duration::from_millis(100)) => {},
                    }
                }
            }
            .await;
            if let Err(error) = result
                && sender.send(Err(error)).await.is_err()
            {
                tracing::debug!("election observer error response abandoned after cancellation");
            }
        });
        self.response(Box::pin(ReceiverStream::new(receiver)) as Self::ObserveStream)
    }

    async fn resign(
        &self,
        request: Request<election::ResignRequest>,
    ) -> std::result::Result<Response<election::ResignResponse>, Status> {
        let principal = self.checked(&request).await?;
        let leader = request
            .into_inner()
            .leader
            .ok_or_else(|| Status::unknown("\"leader\" field must be provided"))?;
        match self
            .kv_mutation(
                principal,
                KvRequest::Txn(pb::TxnRequest {
                    compare: vec![compare_create(&leader.key, leader.rev)],
                    success: vec![pb::RequestOp {
                        request: Some(pb::request_op::Request::RequestDeleteRange(
                            pb::DeleteRangeRequest {
                                key: leader.key,
                                ..Default::default()
                            },
                        )),
                    }],
                    ..Default::default()
                }),
            )
            .await?
        {
            KvReply::Txn(response) => self.response(election::ResignResponse {
                header: response.header,
            }),
            _ => Err(Status::internal("resignation returned an invalid reply")),
        }
    }
}

fn compare_create(key: &[u8], revision: i64) -> pb::Compare {
    pb::Compare {
        key: key.to_vec(),
        target: pb::compare::CompareTarget::Create as i32,
        target_union: Some(pb::compare::TargetUnion::CreateRevision(revision)),
        ..Default::default()
    }
}
fn range_op(request: pb::RangeRequest) -> pb::RequestOp {
    pb::RequestOp {
        request: Some(pb::request_op::Request::RequestRange(request)),
    }
}
fn prefix(name: &[u8]) -> Vec<u8> {
    let mut prefix = name.to_vec();
    prefix.push(b'/');
    prefix
}
fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    for index in (0..end.len()).rev() {
        if end[index] < 255 {
            end[index] += 1;
            end.truncate(index + 1);
            return end;
        }
    }
    vec![0]
}
fn first(prefix: &[u8], revision: i64) -> pb::RangeRequest {
    pb::RangeRequest {
        key: prefix.to_vec(),
        range_end: prefix_end(prefix),
        revision,
        limit: 1,
        sort_target: pb::range_request::SortTarget::Create as i32,
        sort_order: pb::range_request::SortOrder::Ascend as i32,
        ..Default::default()
    }
}
