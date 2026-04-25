#![allow(dead_code)]

use anyhow::{Context, Result, anyhow};
use rmcp::{
    ServiceExt,
    model::{CallToolRequestParams, CallToolResult, RawContent},
    service::{RoleClient, RunningService},
    transport::TokioChildProcess,
};
use serde_json::{Map, Value};
use tokio::process::Command;

pub struct McpClient {
    pub service: RunningService<RoleClient, ()>,
}

impl McpClient {
    pub async fn spawn(database_url: &str) -> Result<Self> {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_db-mcp"));
        cmd.arg("--database-url").arg(database_url);
        let transport = TokioChildProcess::new(cmd).context("spawn db-mcp child process")?;
        let service = ().serve(transport).await.context("MCP handshake with db-mcp")?;
        Ok(Self { service })
    }

    pub async fn call(&self, name: &'static str, args: Value) -> Result<CallToolResult> {
        let arguments = match args {
            Value::Object(m) => Some(m),
            Value::Null => None,
            other => return Err(anyhow!("tool args must be object, got {other:?}")),
        };
        let mut params = CallToolRequestParams::new(name);
        if let Some(args) = arguments {
            params = params.with_arguments(args);
        }
        Ok(self.service.peer().call_tool(params).await?)
    }

    pub async fn call_text(&self, name: &'static str, args: Value) -> Result<String> {
        let res = self.call(name, args).await?;
        if res.is_error.unwrap_or(false) {
            return Err(anyhow!("tool {name} returned error: {:?}", res.content));
        }
        let txt = res
            .content
            .into_iter()
            .find_map(|c| match c.raw {
                RawContent::Text(t) => Some(t.text),
                _ => None,
            })
            .ok_or_else(|| anyhow!("no text content in tool result"))?;
        Ok(txt)
    }

    pub async fn call_json(&self, name: &'static str, args: Value) -> Result<Value> {
        let txt = self.call_text(name, args).await?;
        Ok(serde_json::from_str(&txt)?)
    }

    pub async fn shutdown(self) {
        let _ = self.service.cancel().await;
    }
}

pub fn json_obj(pairs: &[(&str, Value)]) -> Value {
    let mut m = Map::new();
    for (k, v) in pairs {
        m.insert((*k).to_string(), v.clone());
    }
    Value::Object(m)
}
