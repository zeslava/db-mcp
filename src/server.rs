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
pub struct DescribeParams {
    /// Table name
    pub table: String,
    /// Schema name (default: public)
    #[serde(default = "default_schema")]
    pub schema: String,
}

fn default_schema() -> String {
    "public".to_string()
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
        let trimmed = p.sql.trim();
        if !trimmed.to_uppercase().starts_with("SELECT") {
            return Err(McpError::invalid_params(
                "Only SELECT queries are allowed",
                None,
            ));
        }

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
            .describe_table(&p.schema, &p.table)
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
            "{} MCP server. Use query for SELECT, list_tables to browse schema, describe_table for column details.",
            self.backend.name()
        );
        info.with_instructions(instructions)
    }
}
