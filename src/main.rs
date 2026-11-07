use clap::{Args, Parser, Subcommand};
use etcqlite::{
    error::{Result, RpcError},
    server::{self, Options, TlsOptions},
    storage::{Identity, Store, interchange, snapshot},
    wire,
};
use std::{net::SocketAddr, path::PathBuf, process::ExitCode, time::Duration};
use tonic::Code;
use uuid::Uuid;

#[derive(Parser)]
#[command(
    name = "etcqlite",
    version,
    about = "Experimental SQLite-backed etcd v3 service"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Inspect {
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        include_data: bool,
        #[arg(long)]
        ack_sensitive_data: bool,
    },
    Snapshot {
        #[command(subcommand)]
        command: SnapshotCommand,
    },
    Join {
        #[arg(long)]
        snapshot: PathBuf,
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long)]
        member_id: u64,
        #[arg(long, default_value = "etcqlite")]
        name: String,
    },
    Migrate {
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long, value_parser = clap::value_parser!(u32).range(1..=2))]
        target: u32,
        #[arg(long)]
        backup: PathBuf,
    },
    Init {
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long, default_value = "etcqlite")]
        name: String,
        #[arg(long)]
        cluster_id: Option<u64>,
        #[arg(long)]
        member_id: Option<u64>,
        #[arg(long)]
        cluster_config: Option<PathBuf>,
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..=i64::MAX as u64))]
        quota_bytes: Option<u64>,
    },
    Serve {
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long, default_value = "127.0.0.1:2379")]
        listen: SocketAddr,
        #[arg(long)]
        cert_file: Option<PathBuf>,
        #[arg(long)]
        key_file: Option<PathBuf>,
        #[arg(long)]
        client_ca_file: Option<PathBuf>,
        #[arg(long, default_value_t = 5000, value_parser = clap::value_parser!(u64).range(1..))]
        request_timeout_ms: u64,
        #[arg(long)]
        peer_listen: Option<SocketAddr>,
        #[arg(long)]
        peer_cert_file: Option<PathBuf>,
        #[arg(long)]
        peer_key_file: Option<PathBuf>,
        #[arg(long)]
        peer_ca_file: Option<PathBuf>,
        #[arg(long)]
        snapshot_tool: Option<PathBuf>,
    },
}

#[derive(Args)]
struct RestoreOptions {
    #[arg(long)]
    data_dir: PathBuf,
    #[arg(long, default_value = "etcqlite")]
    name: String,
    #[arg(long)]
    cluster_id: Option<u64>,
    #[arg(long)]
    member_id: Option<u64>,
    #[arg(long, default_value_t = 0, value_parser = clap::value_parser!(i64).range(0..))]
    revision_bump: i64,
    #[arg(long)]
    mark_compacted: bool,
    #[arg(long)]
    fenced_source: bool,
    #[arg(long)]
    cluster_config: Option<PathBuf>,
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..=i64::MAX as u64))]
    quota_bytes: Option<u64>,
}

#[derive(Subcommand)]
enum SnapshotCommand {
    Save {
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    Status {
        #[arg(long)]
        input: PathBuf,
    },
    Restore {
        #[arg(long)]
        input: PathBuf,
        #[command(flatten)]
        options: RestoreOptions,
    },
    Export {
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    Import {
        #[arg(long)]
        input: PathBuf,
        #[command(flatten)]
        options: RestoreOptions,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    let filter = match std::env::var("RUST_LOG") {
        Ok(value) => match tracing_subscriber::EnvFilter::try_new(value) {
            Ok(filter) => filter,
            Err(error) => {
                eprintln!("Invalid RUST_LOG configuration: {error}");
                return ExitCode::FAILURE;
            }
        },
        Err(std::env::VarError::NotPresent) => tracing_subscriber::EnvFilter::new("info"),
        Err(error) => {
            eprintln!("Cannot read RUST_LOG configuration: {error}");
            return ExitCode::FAILURE;
        }
    };
    tracing_subscriber::fmt()
        .json()
        .with_writer(std::io::stderr)
        .with_env_filter(filter)
        .init();
    match run(Cli::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(error = %error, "etcqlite operation failed");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Command::Inspect {
            data_dir,
            output,
            include_data,
            ack_sensitive_data,
        } => {
            if include_data && !ack_sensitive_data {
                return Err(RpcError::new(
                    Code::PermissionDenied,
                    "--include-data requires --ack-sensitive-data",
                ));
            }
            let store = open_recovered(data_dir)?;
            store
                .inspection_projection(&etcqlite::storage::auth::Principal::Internal, include_data)?
                .publish(&output)?;
            println!(
                "{}",
                serde_json::json!({ "event": "projected", "revision": store.revision(), "read_only": true, "includes_data": include_data, "recoverable": false })
            );
            Ok(())
        }
        Command::Snapshot { command } => run_snapshot(command),
        Command::Migrate {
            data_dir,
            target,
            backup,
        } => {
            let mut store = Store::open(data_dir)?;
            println!(
                "{}",
                serde_json::to_string(&store.migrate(target, &backup)?)?
            );
            Ok(())
        }
        Command::Join {
            snapshot: input,
            data_dir,
            member_id,
            name,
        } => {
            let info = snapshot::inspect(&input)?;
            let result = snapshot::join(
                &input,
                &data_dir,
                Identity {
                    cluster_id: info.cluster_id,
                    member_id,
                    name,
                },
            )?;
            println!("{}", serde_json::to_string(&result)?);
            Ok(())
        }
        Command::Init {
            data_dir,
            name,
            cluster_id,
            member_id,
            cluster_config,
            quota_bytes,
        } => {
            let configuration = cluster_config
                .map(|path| -> Result<etcqlite::cluster::Configuration> {
                    let bytes = std::fs::read(path)?;
                    let configuration = serde_json::from_slice(&bytes)?;
                    Ok(configuration)
                })
                .transpose()?;
            if let Some(configuration) = &configuration {
                configuration.validate()?;
                if cluster_id.is_some_and(|id| id != configuration.cluster_id) {
                    return Err(RpcError::invalid(
                        "--cluster-id disagrees with the cluster configuration",
                    ));
                }
                let member = member_id.ok_or_else(|| {
                    RpcError::invalid("cluster initialization requires --member-id")
                })?;
                configuration.member(member)?;
            }
            let identity = Identity {
                cluster_id: configuration.as_ref().map_or_else(
                    || cluster_id.map_or_else(random_id, Ok),
                    |configuration| Ok(configuration.cluster_id),
                )?,
                member_id: member_id.map_or_else(random_id, Ok)?,
                name,
            };
            let mut store = Store::create(data_dir, identity)?;
            if let Some(bytes) = quota_bytes {
                store.bootstrap_quota(bytes)?;
            }
            if let Some(configuration) = &configuration {
                store.configure_cluster(configuration)?;
            }
            println!(
                "{}",
                serde_json::json!({
                    "event": "initialized", "cluster_id": store.identity().cluster_id.to_string(),
                    "member_id": store.identity().member_id.to_string(), "revision": store.revision(),
                })
            );
            Ok(())
        }
        Command::Serve {
            data_dir,
            listen,
            cert_file,
            key_file,
            client_ca_file,
            request_timeout_ms,
            peer_listen,
            peer_cert_file,
            peer_key_file,
            peer_ca_file,
            snapshot_tool,
        } => {
            let tls = match (cert_file, key_file, client_ca_file) {
                (Some(certificate), Some(key), client_ca) => Some(TlsOptions {
                    certificate,
                    key,
                    client_ca,
                }),
                (None, None, None) => None,
                _ => {
                    return Err(RpcError::new(
                        Code::InvalidArgument,
                        "TLS requires both --cert-file and --key-file",
                    ));
                }
            };
            let server = server::start(Options {
                data_dir,
                address: listen,
                tls,
                request_timeout: Duration::from_millis(request_timeout_ms),
                snapshot_tool,
                peer: match (peer_listen, peer_cert_file, peer_key_file, peer_ca_file) {
                    (Some(address), Some(certificate), Some(key), Some(ca)) => Some(etcqlite::peer::Options { address, certificate, key, ca }),
                    (None, None, None, None) => None,
                    _ => return Err(RpcError::invalid("peer serving requires --peer-listen, --peer-cert-file, --peer-key-file, and --peer-ca-file")),
                },
            })
            .await?;
            println!(
                "{}",
                serde_json::json!({
                    "event": "started", "endpoint": server.endpoint,
                    "engine_version": wire::ENGINE_VERSION, "api_version": wire::ETCD_API_VERSION,
                    "build": wire::BUILD_SHA,
                    "dirty": wire::BUILD_DIRTY,
                })
            );
            wait_for_shutdown().await?;
            server.shutdown().await
        }
    }
}

fn open_recovered(path: PathBuf) -> Result<Store> {
    let mut store = Store::open(path)?;
    if store.cluster_configuration()?.is_some() {
        let mut replica = etcqlite::replica::Replica::open(&store)?;
        replica.apply(&mut store)?;
    }
    Ok(store)
}

fn run_snapshot(command: SnapshotCommand) -> Result<()> {
    match command {
        SnapshotCommand::Save { data_dir, output } => {
            let store = open_recovered(data_dir)?;
            let saved = store
                .native_snapshot(&etcqlite::storage::auth::Principal::Internal)?
                .publish(&output)?;
            println!("{}", serde_json::to_string(&saved)?);
        }
        SnapshotCommand::Status { input } => {
            println!("{}", serde_json::to_string(&snapshot::inspect(&input)?)?)
        }
        SnapshotCommand::Export { data_dir, output } => {
            let store = open_recovered(data_dir)?;
            let manifest = store.application_manifest(
                &etcqlite::storage::auth::Principal::Internal,
                u64::from(
                    store
                        .cluster_configuration()?
                        .map_or(1, |configuration| configuration.configuration_id),
                ) << 32,
            )?;
            manifest.write(&output)?;
            println!(
                "{}",
                serde_json::json!({ "event": "exported", "revision": manifest.revision, "compact_revision": manifest.compact_revision, "sensitive": true })
            );
        }
        SnapshotCommand::Restore { input, options }
        | SnapshotCommand::Import { input, options } => {
            if !options.fenced_source {
                return Err(RpcError::invalid(
                    "restore/import requires --fenced-source after stopping or fencing all source writers; the flag is an attestation, not a live fencing mechanism",
                ));
            }
            let configuration: Option<etcqlite::cluster::Configuration> = options
                .cluster_config
                .as_ref()
                .map(|path| -> Result<_> { Ok(serde_json::from_slice(&std::fs::read(path)?)?) })
                .transpose()?;
            if configuration.is_some() && options.member_id.is_none() {
                return Err(RpcError::invalid("cluster restore requires --member-id"));
            }
            if let Some(configuration) = &configuration
                && options
                    .cluster_id
                    .is_some_and(|id| id != configuration.cluster_id)
            {
                return Err(RpcError::invalid(
                    "--cluster-id disagrees with the new restore configuration",
                ));
            }
            let identity = Identity {
                cluster_id: configuration.as_ref().map_or_else(
                    || options.cluster_id.map_or_else(random_id, Ok),
                    |configuration| Ok(configuration.cluster_id),
                )?,
                member_id: options.member_id.map_or_else(random_id, Ok)?,
                name: options.name,
            };
            let mut signature = [0; 16];
            use std::io::Read;
            let mut source = std::fs::File::open(&input)?;
            let count = source.read(&mut signature)?;
            let native = count == 16 && signature == *b"SQLite format 3\0";
            let disk = if native {
                let connection = rusqlite::Connection::open_with_flags(
                    &input,
                    rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
                )?;
                connection.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='interchange_meta')", [], |row| row.get::<_, bool>(0))?
            } else {
                false
            };
            let result = if disk {
                interchange::restore_disk(
                    &input,
                    &options.data_dir,
                    identity,
                    configuration.as_ref(),
                    options.revision_bump,
                    options.mark_compacted,
                    options.quota_bytes,
                )?
            } else if native {
                if options.quota_bytes.is_some() {
                    return Err(RpcError::invalid(
                        "native snapshots retain their source quota; --quota-bytes applies to application interchange imports",
                    ));
                }
                snapshot::restore(
                    &input,
                    &options.data_dir,
                    identity,
                    configuration.as_ref(),
                    options.revision_bump,
                    options.mark_compacted,
                )?
            } else {
                interchange::restore_with_quota(
                    &input,
                    &options.data_dir,
                    identity,
                    configuration.as_ref(),
                    options.revision_bump,
                    options.mark_compacted,
                    options.quota_bytes,
                )?
            };
            println!("{}", serde_json::to_string(&result)?);
        }
    }
    Ok(())
}

fn random_id() -> Result<u64> {
    let bytes: [u8; 8] = Uuid::new_v4().as_bytes()[..8]
        .try_into()
        .map_err(|_| RpcError::corrupt("generated node ID has invalid width"))?;
    Ok(u64::from_be_bytes(bytes).max(1))
}

async fn wait_for_shutdown() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result?,
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    Ok(())
}
