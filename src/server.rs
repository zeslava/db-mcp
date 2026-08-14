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
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ExplainParams {
    /// SQL SELECT query to explain
    pub sql: String,
    /// Run the query and report actual timings (EXPLAIN ANALYZE). Unsupported on ClickHouse;
    /// ignored on SQLite.
    #[serde(default)]
    pub analyze: bool,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct DescribeParams {
    /// Table name
    pub table: String,
    /// Schema name. Defaults: PostgreSQL=public, MySQL=current database, SQLite=ignored.
    #[serde(default)]
    pub schema: Option<String>,
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
    backend: Arc<dyn Database>,
    tool_router: ToolRouter<DbServer>,
}

#[tool_router]
impl DbServer {
    pub fn new(backend: Arc<dyn Database>) -> Self {
        Self {
            backend,
            tool_router: Self::tool_router(),
        }
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
            .backend
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
            .backend
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
    async fn list_tables(&self) -> Result<CallToolResult, McpError> {
        let tables = self
            .backend
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
            .backend
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
        let instructions = format!(
            "{} MCP server. Use query for SELECT, explain for execution plans, list_tables to browse schema, describe_table for column details.",
            self.backend.name()
        );
        info.with_instructions(instructions)
    }
}
