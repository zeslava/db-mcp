use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use rmcp::{
    ErrorData as McpError, ServerHandler, ServiceExt,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::*,
    schemars, tool, tool_handler, tool_router,
    transport::stdio,
};
use serde::Deserialize;
use tokio_postgres::Client;
use tokio_postgres::types::{FromSql, Kind, Type};

struct RawBytes(Vec<u8>);

impl<'a> FromSql<'a> for RawBytes {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(RawBytes(raw.to_vec()))
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }
}

#[derive(Parser)]
#[command(about = "MCP server for PostgreSQL")]
struct Args {
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct QueryParams {
    /// SQL SELECT query to execute
    sql: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct DescribeParams {
    /// Table name
    table: String,
    /// Schema name (default: public)
    #[serde(default = "default_schema")]
    schema: String,
}

fn default_schema() -> String {
    "public".to_string()
}

#[derive(Clone)]
#[allow(dead_code)]
struct PgServer {
    client: Arc<Client>,
    tool_router: ToolRouter<PgServer>,
}

#[tool_router]
impl PgServer {
    fn new(client: Client) -> Self {
        Self {
            client: Arc::new(client),
            tool_router: Self::tool_router(),
        }
    }

    /// Execute a read-only SELECT query and return rows as JSON array
    #[tool(
        description = "Execute a SELECT query against the PostgreSQL database and return rows as a JSON array"
    )]
    async fn query(
        &self,
        Parameters(p): Parameters<QueryParams>,
    ) -> Result<CallToolResult, McpError> {
        let trimmed = p.sql.trim();
        if !trimmed.to_uppercase().starts_with("SELECT") {
            return Err(McpError::invalid_params(
                "Only SELECT queries are allowed",
                None,
            ));
        }

        let rows = self
            .client
            .query(trimmed, &[])
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        let result: Vec<serde_json::Value> = rows
            .iter()
            .map(|row| {
                let mut obj = serde_json::Map::new();
                for (i, col) in row.columns().iter().enumerate() {
                    let val = pg_value_to_json(row, i);
                    obj.insert(col.name().to_string(), val);
                }
                serde_json::Value::Object(obj)
            })
            .collect();

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&result).unwrap_or_default(),
        )]))
    }

    /// List all user tables in the database
    #[tool(description = "List all user-defined tables in the PostgreSQL database")]
    async fn list_tables(&self) -> Result<CallToolResult, McpError> {
        let rows = self
            .client
            .query(
                "SELECT table_schema, table_name \
                 FROM information_schema.tables \
                 WHERE table_schema NOT IN ('pg_catalog', 'information_schema') \
                 ORDER BY table_schema, table_name",
                &[],
            )
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        let result: Vec<serde_json::Value> = rows
            .iter()
            .map(|row| {
                serde_json::json!({
                    "schema": row.get::<_, &str>(0),
                    "table": row.get::<_, &str>(1),
                })
            })
            .collect();

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&result).unwrap_or_default(),
        )]))
    }

    /// Describe the columns of a table
    #[tool(description = "Describe columns, types, and nullability of a PostgreSQL table")]
    async fn describe_table(
        &self,
        Parameters(p): Parameters<DescribeParams>,
    ) -> Result<CallToolResult, McpError> {
        let rows = self
            .client
            .query(
                "SELECT column_name, data_type, is_nullable \
                 FROM information_schema.columns \
                 WHERE table_name = $1 AND table_schema = $2 \
                 ORDER BY ordinal_position",
                &[&p.table, &p.schema],
            )
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        let result: Vec<serde_json::Value> = rows
            .iter()
            .map(|row| {
                serde_json::json!({
                    "column": row.get::<_, &str>(0),
                    "type": row.get::<_, &str>(1),
                    "nullable": row.get::<_, &str>(2) == "YES",
                })
            })
            .collect();

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&result).unwrap_or_default(),
        )]))
    }
}

#[tool_handler]
impl ServerHandler for PgServer {
    fn get_info(&self) -> ServerInfo {
        let mut info = ServerInfo::default();
        info.capabilities = ServerCapabilities::builder().enable_tools().build();
        info.with_instructions(
            "PostgreSQL MCP server. Use query for SELECT, list_tables to browse schema, describe_table for column details.",
        )
    }
}

fn pg_value_to_json(row: &tokio_postgres::Row, idx: usize) -> serde_json::Value {
    let col_type = row.columns()[idx].type_();
    match col_type {
        &Type::BOOL => row
            .try_get::<_, Option<bool>>(idx)
            .ok()
            .flatten()
            .map(serde_json::Value::Bool)
            .unwrap_or(serde_json::Value::Null),
        &Type::INT2 => row
            .try_get::<_, Option<i16>>(idx)
            .ok()
            .flatten()
            .map(|v| serde_json::Value::Number(v.into()))
            .unwrap_or(serde_json::Value::Null),
        &Type::INT4 => row
            .try_get::<_, Option<i32>>(idx)
            .ok()
            .flatten()
            .map(|v| serde_json::Value::Number(v.into()))
            .unwrap_or(serde_json::Value::Null),
        &Type::INT8 => row
            .try_get::<_, Option<i64>>(idx)
            .ok()
            .flatten()
            .map(|v| serde_json::Value::Number(v.into()))
            .unwrap_or(serde_json::Value::Null),
        &Type::FLOAT4 => row
            .try_get::<_, Option<f32>>(idx)
            .ok()
            .flatten()
            .and_then(|v| serde_json::Number::from_f64(v as f64))
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        &Type::FLOAT8 => row
            .try_get::<_, Option<f64>>(idx)
            .ok()
            .flatten()
            .and_then(serde_json::Number::from_f64)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        &Type::JSON | &Type::JSONB => row
            .try_get::<_, Option<serde_json::Value>>(idx)
            .ok()
            .flatten()
            .unwrap_or(serde_json::Value::Null),
        _ => fallback_to_json(row, idx, col_type),
    }
}

fn fallback_to_json(
    row: &tokio_postgres::Row,
    idx: usize,
    col_type: &Type,
) -> serde_json::Value {
    if let Ok(opt) = row.try_get::<_, Option<&str>>(idx) {
        return opt
            .map(|s| serde_json::Value::String(s.to_string()))
            .unwrap_or(serde_json::Value::Null);
    }

    let raw = match row.try_get::<_, Option<RawBytes>>(idx) {
        Ok(Some(v)) => v,
        Ok(None) => return serde_json::Value::Null,
        Err(_) => return serde_json::Value::Null,
    };

    // Enums and domains over text are sent as UTF-8 label bytes in the binary protocol.
    if matches!(col_type.kind(), Kind::Enum(_) | Kind::Domain(_))
        && let Ok(s) = std::str::from_utf8(&raw.0)
    {
        return serde_json::Value::String(s.to_string());
    }

    serde_json::Value::Null
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

    let (client, connection) = tokio_postgres::connect(&args.database_url, tokio_postgres::NoTls)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect to PostgreSQL: {e}"))?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!("PostgreSQL connection error: {e}");
        }
    });

    tracing::info!("Connected to PostgreSQL, starting MCP server");

    let service = PgServer::new(client)
        .serve(stdio())
        .await
        .inspect_err(|e| tracing::error!("MCP server error: {e}"))?;

    service.waiting().await?;
    Ok(())
}
