use etcqlite::{
    runtime::Runtime,
    storage::{Identity, KvRequest, Store, auth::Principal, command::Mutation},
    wire::etcdserverpb as pb,
};
use std::time::Duration;

#[tokio::test]
async fn a_pinned_bulk_reader_does_not_block_durable_client_mutations() {
    let directory = tempfile::tempdir().unwrap();
    let node = directory.path().join("node");
    drop(
        Store::create(
            &node,
            Identity {
                cluster_id: 42,
                member_id: 1,
                name: "mock-maintenance".into(),
            },
        )
        .unwrap(),
    );
    let (runtime, worker) =
        Runtime::open(node, "http://127.0.0.1:2379".into(), Duration::from_secs(1))
            .await
            .unwrap();
    let reader_runtime = runtime.clone();
    let (ready, started) = tokio::sync::oneshot::channel();
    let (release, waiting) = std::sync::mpsc::channel();
    let task = tokio::spawn(async move {
        reader_runtime
            .maintenance_read(Principal::Internal, move |store, header| {
                ready.send(()).map_err(|_| {
                    etcqlite::error::RpcError::invalid("mock reader waiter stopped")
                })?;
                waiting.recv().map_err(|_| {
                    etcqlite::error::RpcError::invalid("mock reader release stopped")
                })?;
                Ok((
                    header.revision,
                    store
                        .hash_kv(&Principal::Internal, 0)?
                        .header
                        .unwrap()
                        .revision,
                ))
            })
            .await
    });
    started.await.unwrap();
    let mutation = runtime
        .mutate(
            Principal::Anonymous,
            Mutation::Kv(KvRequest::Put(pb::PutRequest {
                key: b"mock/nonblocking".to_vec(),
                value: vec![0, 255],
                ..Default::default()
            })),
        )
        .await;
    release.send(()).unwrap();
    let captured = task.await.unwrap().unwrap();
    worker.shutdown().await.unwrap();
    mutation.unwrap();
    assert_eq!(captured, (1, 1));
}
