use super::{Api, ResponseStream, fill_header};
use crate::{
    storage::{
        KvReply, KvRequest, MAX_REQUEST_BYTES,
        auth::Principal,
        command::{Mutation, Reply},
    },
    wire::etcdserverpb as pb,
};
use prost::Message;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

impl Api {
    pub(crate) async fn range_snapshot(
        &self,
        principal: Principal,
        request: pb::RangeRequest,
    ) -> std::result::Result<pb::RangeResponse, Status> {
        self.runtime
            .read(move |store, state| {
                let mut response = store.authorized_range(&principal, &request)?;
                fill_header(&mut response.header, state.header(store));
                Ok(response)
            })
            .await
            .map_err(|error| error.status())
    }

    pub(crate) async fn kv_mutation(
        &self,
        principal: Principal,
        request: KvRequest,
    ) -> std::result::Result<KvReply, Status> {
        match self
            .runtime
            .mutate(principal, Mutation::Kv(request))
            .await
            .map_err(|error| error.status())?
        {
            Reply::Kv(reply) => Ok(reply),
            _ => Err(Status::internal("KV command returned the wrong reply kind")),
        }
    }
}

#[tonic::async_trait]
impl pb::kv_server::Kv for Api {
    async fn range(
        &self,
        request: Request<pb::RangeRequest>,
    ) -> std::result::Result<Response<pb::RangeResponse>, Status> {
        let principal = self.checked(&request).await?;
        let request = request.into_inner();
        if !request.serializable {
            self.runtime
                .barrier()
                .await
                .map_err(|error| error.status())?;
        }
        self.response(self.range_snapshot(principal, request).await?)
    }

    type RangeStreamStream = ResponseStream<pb::RangeStreamResponse>;
    async fn range_stream(
        &self,
        request: Request<pb::RangeRequest>,
    ) -> std::result::Result<Response<Self::RangeStreamStream>, Status> {
        let principal = self.checked(&request).await?;
        let mut request = request.into_inner();
        if self
            .runtime
            .read(|store, _| Ok(store.protocol_version() == "3.6.0"))
            .await
            .map_err(|error| error.status())?
        {
            return Err(Status::unimplemented(
                "RangeStream requires the 3.7 protocol profile",
            ));
        }
        if request.sort_order != 0 && !(request.sort_order == 1 && request.sort_target == 0) {
            return Err(Status::unimplemented(
                "RangeStream does not support custom sort orders",
            ));
        }
        if request.min_mod_revision != 0
            || request.max_mod_revision != 0
            || request.min_create_revision != 0
            || request.max_create_revision != 0
        {
            return Err(Status::unimplemented(
                "RangeStream does not support revision filters",
            ));
        }
        if !request.serializable {
            self.runtime
                .barrier()
                .await
                .map_err(|error| error.status())?;
        }
        let (sender, receiver) = tokio::sync::mpsc::channel(8);
        let api = self.clone();
        tokio::spawn(async move {
            let result: std::result::Result<(), Status> = async {
                if request.count_only {
                    let response = api.range_snapshot(principal, request).await?;
                    if sender
                        .send(Ok(pb::RangeStreamResponse {
                            range_response: Some(response),
                        }))
                        .await
                        .is_err()
                    {
                        return Ok(());
                    }
                    return Ok(());
                }
                let limit = if request.limit <= 0 {
                    i64::MAX
                } else {
                    request.limit
                };
                let mut chunk_limit = 10_i64.min(limit);
                let mut count = 0_i64;
                let mut first_header = None;
                let mut total = 0;
                loop {
                    request.limit = chunk_limit.min(limit - count);
                    let response = api
                        .range_snapshot(principal.clone(), request.clone())
                        .await?;
                    let encoded_len = response.encoded_len();
                    if first_header.is_none() {
                        first_header = response.header;
                        total = response.count;
                        if request.revision == 0 {
                            request.revision = response
                                .header
                                .as_ref()
                                .ok_or_else(|| Status::internal("range stream header missing"))?
                                .revision;
                        }
                    }
                    count += i64::try_from(response.kvs.len())
                        .map_err(|_| Status::internal("range stream count overflow"))?;
                    let done = !response.more || count == limit;
                    if response.more {
                        let mut next = response
                            .kvs
                            .last()
                            .ok_or_else(|| {
                                Status::internal("range stream pagination has no last key")
                            })?
                            .key
                            .clone();
                        next.push(0);
                        request.key = next;
                    }
                    let output = pb::RangeResponse {
                        header: if done { first_header } else { None },
                        count: if done { total } else { 0 },
                        more: done && response.more,
                        kvs: response.kvs,
                    };
                    if sender
                        .send(Ok(pb::RangeStreamResponse {
                            range_response: Some(output),
                        }))
                        .await
                        .is_err()
                    {
                        return Ok(());
                    }
                    if done {
                        return Ok(());
                    }
                    if encoded_len < MAX_REQUEST_BYTES / 2 {
                        chunk_limit = chunk_limit.saturating_mul(2);
                    } else if encoded_len > MAX_REQUEST_BYTES * 2 {
                        chunk_limit = (chunk_limit / 2).max(1);
                    }
                }
            }
            .await;
            if let Err(error) = result
                && sender.send(Err(error)).await.is_err()
            {
                tracing::debug!("range stream error response abandoned after cancellation");
            }
        });
        self.response(Box::pin(ReceiverStream::new(receiver)) as Self::RangeStreamStream)
    }

    async fn put(
        &self,
        request: Request<pb::PutRequest>,
    ) -> std::result::Result<Response<pb::PutResponse>, Status> {
        let principal = self.checked(&request).await?;
        match self
            .kv_mutation(principal, KvRequest::Put(request.into_inner()))
            .await?
        {
            KvReply::Put(reply) => self.response(reply),
            _ => Err(Status::internal("invalid put reply")),
        }
    }

    async fn delete_range(
        &self,
        request: Request<pb::DeleteRangeRequest>,
    ) -> std::result::Result<Response<pb::DeleteRangeResponse>, Status> {
        let principal = self.checked(&request).await?;
        match self
            .kv_mutation(principal, KvRequest::Delete(request.into_inner()))
            .await?
        {
            KvReply::Delete(reply) => self.response(reply),
            _ => Err(Status::internal("invalid delete reply")),
        }
    }

    async fn txn(
        &self,
        request: Request<pb::TxnRequest>,
    ) -> std::result::Result<Response<pb::TxnResponse>, Status> {
        let principal = self.checked(&request).await?;
        match self
            .kv_mutation(principal, KvRequest::Txn(request.into_inner()))
            .await?
        {
            KvReply::Txn(reply) => self.response(reply),
            _ => Err(Status::internal("invalid transaction reply")),
        }
    }

    async fn compact(
        &self,
        request: Request<pb::CompactionRequest>,
    ) -> std::result::Result<Response<pb::CompactionResponse>, Status> {
        let principal = self.checked(&request).await?;
        let request = request.into_inner();
        let physical = request.physical;
        match self
            .kv_mutation(principal.clone(), KvRequest::Compact(request))
            .await?
        {
            KvReply::Compact(reply) => {
                if physical {
                    self.runtime
                        .maintenance(move |store, _| store.defragment(&principal))
                        .await
                        .map_err(|error| {
                            Status::new(
                                error.status().code(),
                                format!(
                                    "logical compaction committed; physical maintenance failed: {}",
                                    error.status().message()
                                ),
                            )
                        })?;
                }
                self.response(reply)
            }
            _ => Err(Status::internal("invalid compaction reply")),
        }
    }
}
