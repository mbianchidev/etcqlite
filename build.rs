use std::{env, error::Error, fs, path::PathBuf, process::Command};

fn main() -> Result<(), Box<dyn Error>> {
    println!("cargo:rerun-if-changed=proto");
    println!("cargo:rerun-if-env-changed=ETCQLITE_BUILD_SHA");
    println!("cargo:rerun-if-env-changed=ETCQLITE_BUILD_DIRTY");
    let output = PathBuf::from(env::var("OUT_DIR")?);
    let descriptor = output.join("api.bin");
    tonic_prost_build::configure()
        .file_descriptor_set_path(&descriptor)
        .compile_protos(
            &[
                "proto/etcd/api/etcdserverpb/rpc.proto",
                "proto/etcd/api/mvccpb/kv.proto",
                "proto/etcd/api/authpb/auth.proto",
                "proto/etcd/server/lease/leasepb/lease.proto",
                "proto/v3lock.proto",
                "proto/v3election.proto",
            ],
            &["proto"],
        )?;
    pbjson_build::Builder::new()
        .register_descriptors(&fs::read(descriptor)?)?
        .build(&[
            ".etcdserverpb",
            ".mvccpb",
            ".authpb",
            ".leasepb",
            ".v3lockpb",
            ".v3electionpb",
        ])?;

    let sha = match env::var("ETCQLITE_BUILD_SHA") {
        Ok(sha) => sha,
        Err(env::VarError::NotPresent) => {
            let result = Command::new("git").args(["rev-parse", "HEAD"]).output()?;
            if !result.status.success() {
                return Err("git build identity failed; set ETCQLITE_BUILD_SHA explicitly".into());
            }
            String::from_utf8(result.stdout)?.trim().to_owned()
        }
        Err(error) => return Err(error.into()),
    };
    if sha.len() != 40 || !sha.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("ETCQLITE_BUILD_SHA must be a full 40-character commit ID".into());
    }
    println!("cargo:rustc-env=ETCQLITE_BUILD_SHA={sha}");
    let dirty = match env::var("ETCQLITE_BUILD_DIRTY") {
        Ok(value) if value == "true" || value == "false" => value,
        Ok(_) => return Err("ETCQLITE_BUILD_DIRTY must be true or false".into()),
        Err(env::VarError::NotPresent) => {
            let result = Command::new("git")
                .args(["status", "--porcelain", "--untracked-files=normal"])
                .output()?;
            if !result.status.success() {
                return Err(
                    "git dirty-state lookup failed; set ETCQLITE_BUILD_DIRTY explicitly".into(),
                );
            }
            (!result.stdout.is_empty()).to_string()
        }
        Err(error) => return Err(error.into()),
    };
    println!("cargo:rustc-env=ETCQLITE_BUILD_DIRTY={dirty}");
    Ok(())
}
