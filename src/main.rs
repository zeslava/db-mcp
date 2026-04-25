mod db;
mod server;

use std::sync::Arc;

use anyhow::{Result, bail};
use clap::Parser;
use rmcp::{ServiceExt, transport::stdio};

use crate::db::Database;
use crate::server::DbServer;

#[derive(Parser)]
#[command(about = "MCP server for SQL databases")]
struct Args {
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,
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

    let args = Args::parse();

    let scheme = args
        .database_url
        .split_once("://")
        .map(|(s, _)| s)
        .or_else(|| args.database_url.split_once(':').map(|(s, _)| s))
        .unwrap_or("");

    let backend: Arc<dyn Database> = match scheme {
        #[cfg(feature = "postgres")]
        "postgres" | "postgresql" => {
            Arc::new(crate::db::postgres::PgBackend::connect(&args.database_url).await?)
        }
        #[cfg(feature = "sqlite")]
        "sqlite" => Arc::new(crate::db::sqlite::SqliteBackend::open(&args.database_url).await?),
        other => bail!("unsupported database url scheme: {other:?}"),
    };

    tracing::info!("Connected to {}, starting MCP server", backend.name());

    let service = DbServer::new(backend)
        .serve(stdio())
        .await
        .inspect_err(|e| tracing::error!("MCP server error: {e}"))?;

    service.waiting().await?;
    Ok(())
}
