#![allow(dead_code)]

use anyhow::{Context, Result, anyhow};
use rmcp::{
    ServiceExt,
    model::{CallToolRequestParams, CallToolResult, RawContent},
    service::{RoleClient, RunningService},
    transport::{StreamableHttpClientTransport, TokioChildProcess},
};
use serde_json::{Map, Value};
use tokio::process::{Child, Command};

pub struct McpClient {
    pub service: RunningService<RoleClient, ()>,
    /// Server process in http mode; killed on drop.
    child: Option<Child>,
}

impl McpClient {
    pub async fn spawn(database_url: &str) -> Result<Self> {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_db-mcp"));
        cmd.arg("--database-url").arg(database_url);
        let transport = TokioChildProcess::new(cmd).context("spawn db-mcp child process")?;
        let service = ().serve(transport).await.context("MCP handshake with db-mcp")?;
        Ok(Self {
            service,
            child: None,
        })
    }

    /// Starts db-mcp with `--transport http` on a free loopback port and connects to `/mcp`.
    /// Returns the client and the server base address (`127.0.0.1:<port>`).
    pub async fn spawn_http(database_url: &str) -> Result<(Self, String)> {
        let addr = std::net::TcpListener::bind("127.0.0.1:0")?.local_addr()?;
        let child = Command::new(env!("CARGO_BIN_EXE_db-mcp"))
            .arg("--database-url")
            .arg(database_url)
            .arg("--transport")
            .arg("http")
            .arg("--bind")
            .arg(addr.to_string())
            .kill_on_drop(true)
            .spawn()
            .context("spawn db-mcp http server")?;

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        while tokio::net::TcpStream::connect(addr).await.is_err() {
            if tokio::time::Instant::now() > deadline {
                return Err(anyhow!("db-mcp did not start listening on {addr}"));
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        let transport = StreamableHttpClientTransport::from_uri(format!("http://{addr}/mcp"));
        let service = ().serve(transport).await.context("MCP handshake over http")?;
        Ok((
            Self {
                service,
                child: Some(child),
            },
            addr.to_string(),
        ))
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

    pub async fn shutdown(mut self) {
        let _ = self.service.cancel().await;
        if let Some(mut child) = self.child.take() {
            let _ = child.kill().await;
        }
    }
}

/// Склеивает все значения всех строк в одну строку — форма вывода EXPLAIN различается
/// между движками, поэтому в тестах проверяем только наличие подстрок.
pub fn flatten_values(rows: &Value) -> String {
    rows.as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|r| r.as_object())
                .flat_map(|o| o.values())
                .map(|v| match v {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default()
}

pub fn json_obj(pairs: &[(&str, Value)]) -> Value {
    let mut m = Map::new();
    for (k, v) in pairs {
        m.insert((*k).to_string(), v.clone());
    }
    Value::Object(m)
}
