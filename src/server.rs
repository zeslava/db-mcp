use std::collections::BTreeMap;
use std::sync::Arc;

use rmcp::{
    ErrorData as McpError, ServerHandler,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::*,
    schemars, tool, tool_handler, tool_router,
};
use serde::Deserialize;

use crate::db::Database;

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct QueryParams {
    /// SQL SELECT query to execute
    pub sql: String,
    /// Database name from list_databases. Optional when only one database is configured.
    #[serde(default)]
    pub database: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ExplainParams {
    /// SQL SELECT query to explain
    pub sql: String,
    /// Run the query and report actual timings (EXPLAIN ANALYZE). Unsupported on ClickHouse;
    /// ignored on SQLite.
    #[serde(default)]
    pub analyze: bool,
    /// Database name from list_databases. Optional when only one database is configured.
    #[serde(default)]
    pub database: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ListTablesParams {
    /// Database name from list_databases. Optional when only one database is configured.
    #[serde(default)]
    pub database: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct DescribeParams {
    /// Table name
    pub table: String,
    /// Schema name. Defaults: PostgreSQL=public, MySQL=current database, SQLite=ignored.
    #[serde(default)]
    pub schema: Option<String>,
    /// Database name from list_databases. Optional when only one database is configured.
    #[serde(default)]
    pub database: Option<String>,
}

pub struct NamedDb {
    pub backend: Arc<dyn Database>,
    pub description: Option<String>,
}

fn ensure_select(sql: &str) -> Result<&str, McpError> {
    let trimmed = sql.trim();
    if !trimmed.to_uppercase().starts_with("SELECT") {
        return Err(McpError::invalid_params(
            "Only SELECT queries are allowed",
            None,
        ));
    }
    Ok(trimmed)
}

#[derive(Clone)]
#[allow(dead_code)]
pub struct DbServer {
    dbs: Arc<BTreeMap<String, NamedDb>>,
    tool_router: ToolRouter<DbServer>,
}

impl DbServer {
    /// No implicit default with several databases: a query must never land in the wrong one.
    fn resolve(&self, name: Option<&str>) -> Result<&dyn Database, McpError> {
        let found = match name {
            Some(n) => self.dbs.get(n),
            None if self.dbs.len() == 1 => self.dbs.values().next(),
            None => None,
        };
        found.map(|d| d.backend.as_ref()).ok_or_else(|| {
            let names = self.dbs.keys().map(String::as_str).collect::<Vec<_>>();
            let msg = match name {
                Some(n) => format!("unknown database {n:?}; available: {}", names.join(", ")),
                None => format!("`database` is required; available: {}", names.join(", ")),
            };
            McpError::invalid_params(msg, None)
        })
    }
}

#[tool_router]
impl DbServer {
    pub fn new(dbs: Arc<BTreeMap<String, NamedDb>>) -> Self {
        Self {
            dbs,
            tool_router: Self::tool_router(),
        }
    }

    #[tool(description = "List configured databases with their engine and description")]
    async fn list_databases(&self) -> Result<CallToolResult, McpError> {
        let value: Vec<serde_json::Value> = self
            .dbs
            .iter()
            .map(|(name, d)| {
                serde_json::json!({
                    "name": name,
                    "engine": d.backend.name(),
                    "description": d.description,
                })
            })
            .collect();

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&value).unwrap_or_default(),
        )]))
    }

    #[tool(
        description = "Execute a SELECT query against the database and return rows as a JSON array"
    )]
    async fn query(
        &self,
        Parameters(p): Parameters<QueryParams>,
    ) -> Result<CallToolResult, McpError> {
        let trimmed = ensure_select(&p.sql)?;

        let rows = self
            .resolve(p.database.as_deref())?
            .query(trimmed)
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        let value =
            serde_json::Value::Array(rows.into_iter().map(serde_json::Value::Object).collect());

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&value).unwrap_or_default(),
        )]))
    }

    #[tool(
        description = "Show the query execution plan for a SELECT query. Set analyze=true to run it and get actual timings (PostgreSQL/MySQL only)"
    )]
    async fn explain(
        &self,
        Parameters(p): Parameters<ExplainParams>,
    ) -> Result<CallToolResult, McpError> {
        let trimmed = ensure_select(&p.sql)?;

        let rows = self
            .resolve(p.database.as_deref())?
            .explain(trimmed, p.analyze)
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        let value =
            serde_json::Value::Array(rows.into_iter().map(serde_json::Value::Object).collect());

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&value).unwrap_or_default(),
        )]))
    }

    #[tool(description = "List all user-defined tables in the database")]
    async fn list_tables(
        &self,
        Parameters(p): Parameters<ListTablesParams>,
    ) -> Result<CallToolResult, McpError> {
        let tables = self
            .resolve(p.database.as_deref())?
            .list_tables()
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        let value: Vec<serde_json::Value> = tables
            .iter()
            .map(|t| {
                serde_json::json!({
                    "schema": t.schema,
                    "table": t.table,
                })
            })
            .collect();

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&value).unwrap_or_default(),
        )]))
    }

    #[tool(description = "Describe columns, types, and nullability of a database table")]
    async fn describe_table(
        &self,
        Parameters(p): Parameters<DescribeParams>,
    ) -> Result<CallToolResult, McpError> {
        let cols = self
            .resolve(p.database.as_deref())?
            .describe_table(p.schema.as_deref(), &p.table)
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        let value: Vec<serde_json::Value> = cols
            .iter()
            .map(|c| {
                serde_json::json!({
                    "column": c.name,
                    "type": c.data_type,
                    "nullable": c.nullable,
                })
            })
            .collect();

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&value).unwrap_or_default(),
        )]))
    }
}

#[tool_handler]
impl ServerHandler for DbServer {
    fn get_info(&self) -> ServerInfo {
        let mut info = ServerInfo::default();
        info.capabilities = ServerCapabilities::builder().enable_tools().build();
        let dbs = self
            .dbs
            .iter()
            .map(|(name, d)| match &d.description {
                Some(desc) => format!("- {name} ({}): {desc}", d.backend.name()),
                None => format!("- {name} ({})", d.backend.name()),
            })
            .collect::<Vec<_>>()
            .join("\n");
        let instructions = format!(
            "SQL MCP server. Use query for SELECT, explain for execution plans, list_tables to browse schema, describe_table for column details.\n\
             Databases:\n{dbs}\n\
             When more than one database is listed, pass `database` to every tool; use list_databases to refresh this list."
        );
        info.with_instructions(instructions)
    }
}
