mod db;
mod server;

use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use clap::{Parser, ValueEnum};
use rmcp::{ServiceExt, transport::stdio};

use crate::db::Database;
use crate::db::clickhouse::ClickhouseBackend;
#[cfg(feature = "clickhouse-native")]
use crate::db::clickhouse_native::ClickhouseNativeBackend;
use crate::db::mysql::MysqlBackend;
use crate::db::postgres::PgBackend;
#[cfg(feature = "sqlite")]
use crate::db::sqlite::SqliteBackend;
use crate::server::DbServer;

#[derive(Parser)]
#[command(
    about = "MCP server for SQL databases",
    version,
    disable_version_flag = true
)]
struct Args {
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,

    #[arg(long, env = "MCP_TRANSPORT", value_enum, default_value_t = Transport::Stdio)]
    transport: Transport,

    /// Listen address for the http transport
    #[cfg(feature = "http")]
    #[arg(long, env = "MCP_BIND", default_value = "127.0.0.1:8080")]
    bind: std::net::SocketAddr,

    /// Accepted Host header values for the http transport (DNS rebinding protection);
    /// defaults to loopback hosts only
    #[cfg(feature = "http")]
    #[arg(long, env = "MCP_ALLOWED_HOSTS", value_delimiter = ',')]
    allowed_hosts: Vec<String>,

    /// Path to an env file with DATABASE_URL (existing env vars win)
    #[arg(long, env = "ENV_FILE")]
    env_file: Option<PathBuf>,

    #[arg(short = 'v', long = "version", action = clap::ArgAction::Version)]
    version: Option<bool>,
}

#[derive(Clone, Copy, ValueEnum)]
enum Transport {
    Stdio,
    #[cfg(feature = "http")]
    Http,
}

/// Loads `--env-file` / `ENV_FILE` before clap parses, so its `env` fallbacks see the values.
/// Variables already present in the environment are not overridden.
fn load_env_file() -> Result<()> {
    let path = pick_env_file(std::env::args_os().skip(1), std::env::var_os("ENV_FILE"));
    let Some(path) = path else { return Ok(()) };
    dotenvy::from_path(&path).with_context(|| format!("failed to load env file {path:?}"))?;
    Ok(())
}

fn pick_env_file<I: Iterator<Item = OsString>>(
    args: I,
    env_var: Option<OsString>,
) -> Option<PathBuf> {
    let mut args = args;
    let mut path = None;
    while let Some(arg) = args.next() {
        let Some(s) = arg.to_str() else { continue };
        if let Some(v) = s.strip_prefix("--env-file=") {
            path = Some(PathBuf::from(v));
        } else if s == "--env-file" {
            path = args.next().map(PathBuf::from);
        }
    }
    path.or_else(|| env_var.map(PathBuf::from))
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();

    load_env_file()?;

    let args = Args::parse();

    let scheme = args
        .database_url
        .split_once("://")
        .map(|(s, _)| s)
        .or_else(|| args.database_url.split_once(':').map(|(s, _)| s))
        .unwrap_or("");

    let backend: Arc<dyn Database> = match scheme {
        "postgres" | "postgresql" => Arc::new(PgBackend::connect(&args.database_url).await?),
        "mysql" => Arc::new(MysqlBackend::connect(&args.database_url).await?),
        "clickhouse+http" | "clickhouse+https" => {
            Arc::new(ClickhouseBackend::connect(&args.database_url).await?)
        }
        #[cfg(feature = "clickhouse-native")]
        "clickhouse" | "clickhouse+native" | "ch" | "clickhouses" | "clickhouse+natives"
        | "chs" => Arc::new(ClickhouseNativeBackend::connect(&args.database_url).await?),
        #[cfg(not(feature = "clickhouse-native"))]
        "clickhouse" | "ch" | "chs" => {
            Arc::new(ClickhouseBackend::connect(&args.database_url).await?)
        }
        #[cfg(feature = "sqlite")]
        "sqlite" => Arc::new(SqliteBackend::open(&args.database_url).await?),
        other => bail!("unsupported database url scheme: {other:?}"),
    };

    tracing::info!("Connected to {}, starting MCP server", backend.name());

    match args.transport {
        Transport::Stdio => serve_stdio(backend).await,
        #[cfg(feature = "http")]
        Transport::Http => serve_http(backend, args.bind, args.allowed_hosts).await,
    }
}

async fn serve_stdio(backend: Arc<dyn Database>) -> Result<()> {
    let service = DbServer::new(backend)
        .serve(stdio())
        .await
        .inspect_err(|e| tracing::error!("MCP server error: {e}"))?;

    service.waiting().await?;
    Ok(())
}

#[cfg(feature = "http")]
async fn serve_http(
    backend: Arc<dyn Database>,
    bind: std::net::SocketAddr,
    allowed_hosts: Vec<String>,
) -> Result<()> {
    use rmcp::transport::streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
    };

    let mut config = StreamableHttpServerConfig::default();
    if !allowed_hosts.is_empty() {
        config = config.with_allowed_hosts(allowed_hosts);
    }
    let cancel = config.cancellation_token.clone();

    let service = StreamableHttpService::new(
        move || Ok(DbServer::new(backend.clone())),
        Arc::new(LocalSessionManager::default()),
        config,
    );
    let router = axum::Router::new().nest_service("/mcp", service);

    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .with_context(|| format!("failed to bind {bind}"))?;
    tracing::info!("Listening on http://{bind}/mcp");

    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            let _ = tokio::signal::ctrl_c().await;
            cancel.cancel();
        })
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(items: &[&str]) -> std::vec::IntoIter<OsString> {
        items
            .iter()
            .map(OsString::from)
            .collect::<Vec<_>>()
            .into_iter()
    }

    #[test]
    fn flag_forms_and_env_fallback() {
        assert_eq!(pick_env_file(args(&[]), None), None);
        assert_eq!(
            pick_env_file(args(&[]), Some(OsString::from("/from/env"))),
            Some(PathBuf::from("/from/env"))
        );
        assert_eq!(
            pick_env_file(args(&["--env-file", "/a.env"]), None),
            Some(PathBuf::from("/a.env"))
        );
        assert_eq!(
            pick_env_file(args(&["--env-file=/b.env"]), None),
            Some(PathBuf::from("/b.env"))
        );
        // флаг приоритетнее переменной окружения
        assert_eq!(
            pick_env_file(
                args(&["--env-file=/b.env"]),
                Some(OsString::from("/from/env"))
            ),
            Some(PathBuf::from("/b.env"))
        );
        // висящий --env-file без значения
        assert_eq!(pick_env_file(args(&["--env-file"]), None), None);
    }

    #[test]
    fn loads_values_without_overriding_existing() {
        let dir = std::env::temp_dir().join(format!("db-mcp-env-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("t.env");
        std::fs::write(
            &path,
            "DB_MCP_TEST_NEW=from_file\nDB_MCP_TEST_SET=from_file\n",
        )
        .unwrap();

        // SAFETY: тест однопоточный по этим переменным, имена уникальны для этого модуля.
        unsafe { std::env::set_var("DB_MCP_TEST_SET", "from_env") };
        dotenvy::from_path(&path).unwrap();

        assert_eq!(std::env::var("DB_MCP_TEST_NEW").unwrap(), "from_file");
        assert_eq!(std::env::var("DB_MCP_TEST_SET").unwrap(), "from_env");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn missing_file_is_an_error() {
        assert!(dotenvy::from_path(std::path::Path::new("/nope/missing.env")).is_err());
    }
}
