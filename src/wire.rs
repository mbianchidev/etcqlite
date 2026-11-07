macro_rules! package {
    ($name:ident, $protobuf:literal) => {
        #[allow(clippy::useless_borrows_in_formatting)]
        pub mod $name {
            tonic::include_proto!($protobuf);
            include!(concat!(env!("OUT_DIR"), "/", $protobuf, ".serde.rs"));
        }
    };
}

package!(etcdserverpb, "etcdserverpb");
package!(mvccpb, "mvccpb");
package!(authpb, "authpb");
package!(leasepb, "leasepb");
package!(v3lockpb, "v3lockpb");
package!(v3electionpb, "v3electionpb");

pub const DESCRIPTORS: &[u8] = tonic::include_file_descriptor_set!("api");
pub const ETCD_API_VERSION: &str = "3.7.3";
pub const ETCD_SOURCE_COMMIT: &str = "0a173d0410a96d01091b484daae81365bbcde57e";
pub const BUILD_SHA: &str = env!("ETCQLITE_BUILD_SHA");
pub const ENGINE_VERSION: &str = env!("CARGO_PKG_VERSION");
