use clap::{Parser, Subcommand};
use etcqlite::{
    error::{Result, RpcError},
    server::{self, Options, TlsOptions},
    storage::{Identity, Store},
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
        Command::Init {
            data_dir,
            name,
            cluster_id,
            member_id,
            cluster_config,
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
                    "event": "ready", "endpoint": server.endpoint,
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
