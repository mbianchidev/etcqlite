use etcqlite::wire::{self, etcdserverpb as pb, mvccpb};
use prost::Message;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

#[test]
fn inventory_has_exactly_the_pinned_49_unique_rpcs() {
    let descriptor = prost_types::FileDescriptorSet::decode(wire::DESCRIPTORS).unwrap();
    let actual: BTreeSet<_> = descriptor
        .file
        .into_iter()
        .flat_map(|file| {
            let package = file.package.unwrap_or_default();
            file.service.into_iter().flat_map(move |service| {
                let prefix = format!("{}.{}", package, service.name.unwrap());
                service
                    .method
                    .into_iter()
                    .map(move |method| format!("{prefix}/{}", method.name.unwrap()))
            })
        })
        .collect();
    let services = [
        (
            "etcdserverpb.KV",
            "Range RangeStream Put DeleteRange Txn Compact",
        ),
        ("etcdserverpb.Watch", "Watch"),
        (
            "etcdserverpb.Lease",
            "LeaseGrant LeaseRevoke LeaseKeepAlive LeaseTimeToLive LeaseLeases",
        ),
        (
            "etcdserverpb.Cluster",
            "MemberAdd MemberRemove MemberUpdate MemberList MemberPromote",
        ),
        (
            "etcdserverpb.Maintenance",
            "Alarm Status Defragment Hash HashKV Snapshot MoveLeader Downgrade",
        ),
        (
            "etcdserverpb.Auth",
            "AuthEnable AuthDisable AuthStatus Authenticate UserAdd UserGet UserList UserDelete UserChangePassword UserGrantRole UserRevokeRole RoleAdd RoleGet RoleList RoleDelete RoleGrantPermission RoleRevokePermission",
        ),
        ("v3lockpb.Lock", "Lock Unlock"),
        (
            "v3electionpb.Election",
            "Campaign Proclaim Leader Observe Resign",
        ),
    ];
    let expected: BTreeSet<_> = services
        .into_iter()
        .flat_map(|(service, methods)| {
            methods
                .split_whitespace()
                .map(move |method| format!("{service}/{method}"))
        })
        .collect();
    assert_eq!(actual.len(), 49);
    assert_eq!(actual, expected);
}

#[test]
fn wire_preserves_unsigned_ids_and_arbitrary_binary_values() {
    let original = pb::RangeResponse {
        header: Some(pb::ResponseHeader {
            cluster_id: u64::MAX,
            member_id: 1 << 63,
            revision: i64::MAX,
            raft_term: u64::MAX,
        }),
        kvs: vec![mvccpb::KeyValue {
            key: vec![0, 255, 128],
            value: vec![0, 128, 255, 0],
            create_revision: 2,
            mod_revision: i64::MAX,
            version: 1,
            lease: i64::MAX,
        }],
        count: 1,
        ..Default::default()
    };
    let bytes = original.encode_to_vec();
    assert_eq!(
        pb::RangeResponse::decode(bytes.as_slice()).unwrap(),
        original
    );
    let json = serde_json::to_value(&original).unwrap();
    assert_eq!(json["header"]["clusterId"], u64::MAX.to_string());
    assert_eq!(json["header"]["revision"], i64::MAX.to_string());
    assert_eq!(json["kvs"][0]["key"], "AP+A");
    assert_eq!(
        serde_json::from_value::<pb::RangeResponse>(json).unwrap(),
        original
    );
}

#[test]
fn pinned_service_protos_have_not_been_silently_changed() {
    for (bytes, expected) in [
        (
            include_bytes!("../proto/etcd/api/etcdserverpb/rpc.proto").as_slice(),
            "d50fe7e0c2cb9d80fa6fed1b47fb5cadcda06674bef99c445adab40c24feedb2",
        ),
        (
            include_bytes!("../proto/v3lock.proto").as_slice(),
            "22c346b01a877318cbbe2f0c0072100c11545026ced0bbceaad261029fff6f3f",
        ),
        (
            include_bytes!("../proto/v3election.proto").as_slice(),
            "aa7dbe529d28ab9dba1b001c3a14989bb191c3b724c0d22cb6a46c95698cfcba",
        ),
    ] {
        let actual: String = Sha256::digest(bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        assert_eq!(actual, expected);
    }
}
