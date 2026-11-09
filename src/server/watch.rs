use super::{Api, ResponseStream};
use crate::wire::{etcdserverpb as pb, mvccpb};
use prost::Message;
use std::{
    collections::BTreeMap,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};
use tonic::{Request, Response, Status};

const MAX_WATCHES_PER_STREAM: usize = 128;
const MAX_FRAME_BYTES: usize = 4 << 20;
const FRAGMENT_BYTES: usize = crate::storage::MAX_REQUEST_BYTES;

struct Watch {
    request: pb::WatchCreateRequest,
    next_revision: i64,
    frontier: i64,
    progress: Instant,
}

struct Output {
    receiver: ReceiverStream<std::result::Result<pb::WatchResponse, Status>>,
    terminal: Arc<Mutex<Option<Status>>>,
    finished: bool,
}

impl Stream for Output {
    type Item = std::result::Result<pb::WatchResponse, Status>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }
        match Pin::new(&mut self.receiver).poll_next(context) {
            Poll::Ready(None) => {
                self.finished = true;
                let result = match self.terminal.lock() {
                    Ok(mut terminal) => terminal.take().map(Err),
                    Err(_) => Some(Err(Status::internal("watch termination state failed"))),
                };
                Poll::Ready(result)
            }
            value => value,
        }
    }
}

async fn enqueue(
    sender: &tokio::sync::mpsc::Sender<std::result::Result<pb::WatchResponse, Status>>,
    response: pb::WatchResponse,
) -> std::result::Result<(), Status> {
    if response.encoded_len() > MAX_FRAME_BYTES {
        return Err(Status::resource_exhausted(
            "watch revision exceeds the response limit; enable fragmentation",
        ));
    }
    tokio::time::timeout(Duration::from_secs(5), sender.send(Ok(response)))
        .await
        .map_err(|_| {
            Status::resource_exhausted(
                "watch consumer is too slow; reconnect from retained history",
            )
        })?
        .map_err(|_| Status::cancelled("watch client disconnected"))
}

fn require_permanent_watch_failure(
    error: &crate::error::RpcError,
) -> std::result::Result<(), Status> {
    let status = error.status();
    // Active watch cancellation frames can close official-client channels without an error.
    // Keep temporary writer failures on the RPC path so clients can resume retained history.
    if matches!(
        status.code(),
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded | tonic::Code::ResourceExhausted
    ) {
        return Err(status);
    }
    Ok(())
}

impl Api {
    pub(crate) async fn watch_stream<S>(
        &self,
        principal: crate::storage::auth::Principal,
        mut incoming: S,
    ) -> std::result::Result<ResponseStream<pb::WatchResponse>, Status>
    where
        S: Stream<Item = std::result::Result<pb::WatchRequest, Status>> + Unpin + Send + 'static,
    {
        let permit = self
            .watch_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("too many active watch streams"))?;
        let (sender, receiver) = tokio::sync::mpsc::channel(16);
        let terminal = Arc::new(Mutex::new(None));
        let termination = Arc::clone(&terminal);
        let api = self.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let mut watches = BTreeMap::<i64, Watch>::new();
            let mut next_id = 0_i64;
            let mut progress_requested = false;
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            let result: std::result::Result<(), Status> = async {
                loop {
                    let notification = api.runtime.changes.notified();
                    tokio::pin!(notification);
                    notification.as_mut().enable();
                    let mut canceled = Vec::new();
                    for (&id, watch) in watches.iter_mut() {
                        let start = watch.next_revision;
                        let key = watch.request.key.clone();
                        let end = watch.request.range_end.clone();
                        let principal = principal.clone();
                        let data = api.runtime.read(move |store, state| {
                            store.authorize_watch(&principal, &key, &end)?;
                            Ok((state.header(store), store.compact_revision(), store.events(start, &key, &end)?))
                        }).await;
                        let (physical, compacted, batches) = match data {
                            Ok(data) => data,
                            Err(error) => {
                                require_permanent_watch_failure(&error)?;
                                let compacted = api.runtime.read(|store, _| Ok(store.compact_revision())).await
                                    .map_err(|error| error.status())?;
                                enqueue(&sender, pb::WatchResponse {
                                    header: Some(api.runtime.header().await.map_err(|error| error.status())?),
                                    watch_id: id, canceled: true,
                                    compact_revision: if error.status().code() == tonic::Code::OutOfRange { compacted } else { 0 },
                                    cancel_reason: error.status().message().to_owned(),
                                    ..Default::default()
                                }).await?;
                                canceled.push(id);
                                continue;
                            }
                        };
                        for (revision, mut events) in batches {
                            events.retain(|event| !watch.request.filters.contains(&event.r#type));
                            if !watch.request.prev_kv || revision - 1 <= compacted {
                                for event in &mut events { event.prev_kv = None; }
                            }
                            if events.is_empty() { continue; }
                            let mut header = physical;
                            header.revision = revision;
                            if watch.request.fragment {
                                let mut chunks: Vec<Vec<mvccpb::Event>> = Vec::new();
                                let mut current = Vec::new();
                                let mut bytes = 0;
                                for event in events {
                                    let size = event.encoded_len();
                                    if bytes + size > FRAGMENT_BYTES && !current.is_empty() {
                                        chunks.push(std::mem::take(&mut current));
                                        bytes = 0;
                                    }
                                    bytes += size;
                                    current.push(event);
                                }
                                if !current.is_empty() { chunks.push(current); }
                                let count = chunks.len();
                                for (index, events) in chunks.into_iter().enumerate() {
                                    enqueue(&sender, pb::WatchResponse {
                                        header: Some(header), watch_id: id, events,
                                        fragment: index + 1 < count, ..Default::default()
                                    }).await?;
                                }
                            } else {
                                enqueue(&sender, pb::WatchResponse { header: Some(header), watch_id: id, events, ..Default::default() }).await?;
                            }
                        }
                        watch.next_revision = physical.revision.checked_add(1)
                            .ok_or_else(|| Status::out_of_range("watch revision exhausted"))?.max(watch.next_revision);
                        watch.frontier = physical.revision;
                        if watch.request.progress_notify && watch.progress.elapsed() >= Duration::from_secs(10) {
                            let mut header = physical;
                            header.revision = watch.frontier;
                            enqueue(&sender, pb::WatchResponse { header: Some(header), watch_id: id, ..Default::default() }).await?;
                            watch.progress = Instant::now();
                        }
                    }
                    for id in canceled { watches.remove(&id); }
                    if progress_requested {
                        let mut header = api.runtime.header().await.map_err(|error| error.status())?;
                        if let Some(frontier) = watches.values().map(|watch| watch.frontier).min() {
                            header.revision = frontier;
                        }
                        enqueue(&sender, pb::WatchResponse { header: Some(header), watch_id: -1, ..Default::default() }).await?;
                        progress_requested = false;
                    }
                    tokio::select! {
                        _ = sender.closed() => return Ok(()),
                        _ = &mut notification => {},
                        _ = interval.tick() => {},
                        request = incoming.next() => {
                            let Some(request) = request else { return Ok(()); };
                            match request?.request_union {
                                Some(pb::watch_request::RequestUnion::CreateRequest(create)) => {
                                    if create.key.is_empty() { return Err(Status::invalid_argument("etcdserver: key is not provided")); }
                                    if create.watch_id < 0 || create.start_revision < 0 { return Err(Status::invalid_argument("watch ID and start revision must be nonnegative")); }
                                    if watches.len() >= MAX_WATCHES_PER_STREAM { return Err(Status::resource_exhausted("too many watches on one stream")); }
                                    let id = if create.watch_id != 0 { create.watch_id } else {
                                        while watches.contains_key(&next_id) {
                                            next_id = next_id.checked_add(1).ok_or_else(|| Status::out_of_range("watch ID exhausted"))?;
                                        }
                                        let id = next_id;
                                        next_id = next_id.checked_add(1).ok_or_else(|| Status::out_of_range("watch ID exhausted"))?;
                                        id
                                    };
                                    if watches.contains_key(&id) { return Err(Status::invalid_argument("watch ID is already in use")); }
                                    api.runtime.barrier().await.map_err(|error| error.status())?;
                                    let key = create.key.clone();
                                    let end = create.range_end.clone();
                                    let principal = principal.clone();
                                    let (header, compacted) = api.runtime.read(move |store, state| {
                                        store.authorize_watch(&principal, &key, &end)?;
                                        Ok((state.header(store), store.compact_revision()))
                                    }).await.map_err(|error| error.status())?;
                                    let next_revision = if create.start_revision == 0 {
                                        header.revision.checked_add(1).ok_or_else(|| Status::out_of_range("watch revision exhausted"))?
                                    } else { create.start_revision };
                                    let is_compacted = next_revision <= compacted;
                                    enqueue(&sender, pb::WatchResponse {
                                        header: Some(header), watch_id: id, created: true, canceled: is_compacted,
                                        compact_revision: if is_compacted { compacted } else { 0 }, ..Default::default()
                                    }).await?;
                                    if !is_compacted {
                                        let frontier = (next_revision - 1).min(header.revision);
                                        watches.insert(id, Watch { request: create, next_revision, frontier, progress: Instant::now() });
                                    }
                                }
                                Some(pb::watch_request::RequestUnion::CancelRequest(cancel)) => {
                                    watches.remove(&cancel.watch_id);
                                    enqueue(&sender, pb::WatchResponse {
                                        header: Some(api.runtime.header().await.map_err(|error| error.status())?),
                                        watch_id: cancel.watch_id, canceled: true, ..Default::default()
                                    }).await?;
                                }
                                Some(pb::watch_request::RequestUnion::ProgressRequest(_)) => progress_requested = true,
                                None => return Err(Status::invalid_argument("watch request is empty")),
                            }
                        }
                    }
                }
            }.await;
            if let Err(error) = result {
                tracing::warn!(code = ?error.code(), "watch stream terminated");
                match termination.lock() {
                    Ok(mut terminal) => *terminal = Some(error),
                    Err(_) => tracing::error!("watch terminal state lock failed"),
                }
            }
        });
        Ok(Box::pin(Output {
            receiver: ReceiverStream::new(receiver),
            terminal,
            finished: false,
        }) as ResponseStream<pb::WatchResponse>)
    }
}

#[tonic::async_trait]
impl pb::watch_server::Watch for Api {
    type WatchStream = ResponseStream<pb::WatchResponse>;
    async fn watch(
        &self,
        request: Request<tonic::Streaming<pb::WatchRequest>>,
    ) -> std::result::Result<Response<Self::WatchStream>, Status> {
        let principal = self.principal(&request).await?;
        self.response(self.watch_stream(principal, request.into_inner()).await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_read_failures_are_rpc_errors_not_successful_watch_cancellations() {
        for code in [
            tonic::Code::Unavailable,
            tonic::Code::DeadlineExceeded,
            tonic::Code::ResourceExhausted,
        ] {
            let failure = crate::error::RpcError::new(code, "mock temporary writer failure");
            let status = require_permanent_watch_failure(&failure).unwrap_err();
            assert_eq!(status.code(), code);
            assert_eq!(status.message(), "mock temporary writer failure");
        }
        for code in [
            tonic::Code::PermissionDenied,
            tonic::Code::Unauthenticated,
            tonic::Code::OutOfRange,
            tonic::Code::DataLoss,
        ] {
            let failure = crate::error::RpcError::new(code, "mock permanent watch failure");
            require_permanent_watch_failure(&failure).unwrap();
        }
    }

    #[tokio::test]
    async fn retained_replay_yields_to_a_healthy_receiver_when_the_queue_fills() {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(16);
        let producer = tokio::spawn(async move {
            for id in 1..=64 {
                enqueue(
                    &sender,
                    pb::WatchResponse {
                        watch_id: id,
                        ..Default::default()
                    },
                )
                .await?;
            }
            Ok::<_, Status>(())
        });
        let mut received = Vec::new();
        while let Some(response) = receiver.recv().await {
            received.push(response.unwrap().watch_id);
        }
        producer.await.unwrap().unwrap();
        assert_eq!(received, (1..=64).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn stalled_receiver_has_a_bounded_terminal_error_without_dropping_queued_frames() {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        enqueue(
            &sender,
            pb::WatchResponse {
                watch_id: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let error = enqueue(
            &sender,
            pb::WatchResponse {
                watch_id: 2,
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
        assert_eq!(receiver.recv().await.unwrap().unwrap().watch_id, 1);
    }

    #[tokio::test]
    async fn disconnected_receiver_and_oversized_frames_remain_explicit_errors() {
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        drop(receiver);
        assert_eq!(
            enqueue(&sender, pb::WatchResponse::default())
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Cancelled
        );
        let response = pb::WatchResponse {
            cancel_reason: "x".repeat(MAX_FRAME_BYTES),
            ..Default::default()
        };
        assert_eq!(
            enqueue(&sender, response).await.unwrap_err().code(),
            tonic::Code::ResourceExhausted
        );
    }
}
