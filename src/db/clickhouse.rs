use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use reqwest::Url;
use serde_json::Value;

use super::{Column, Database, Row as JsonRow, TableRef};

pub struct ClickhouseBackend {
    client: reqwest::Client,
    endpoint: Url,
    user: String,
    password: String,
}

impl ClickhouseBackend {
    pub async fn connect(url: &str) -> Result<Self> {
        let parsed = Url::parse(url).context("invalid ClickHouse url")?;
        let (http_scheme, default_port) = match parsed.scheme() {
            "clickhouse" | "clickhouse+http" | "ch" => ("http", 8123u16),
            "clickhouse+https" | "chs" => ("https", 8443),
            other => bail!("unsupported clickhouse scheme: {other}"),
        };
        let host = parsed.host_str().context("clickhouse url missing host")?;
        let port = parsed.port().unwrap_or(default_port);
        let database = parsed.path().trim_start_matches('/').to_string();
        let user = match parsed.username() {
            "" => "default".to_string(),
            u => percent_decode(u),
        };
        let password = parsed.password().map(percent_decode).unwrap_or_default();

        let mut endpoint = Url::parse(&format!("{http_scheme}://{host}:{port}/"))?;
        {
            let mut q = endpoint.query_pairs_mut();
            if !database.is_empty() {
                q.append_pair("database", &database);
            }
            q.append_pair("default_format", "JSONEachRow");
            q.append_pair("output_format_json_quote_64bit_integers", "0");
        }

        let client = reqwest::Client::builder()
            .build()
            .context("Failed to build HTTP client")?;

        let backend = Self {
            client,
            endpoint,
            user,
            password,
        };
        backend
            .post("SELECT 1", &[])
            .await
            .context("Failed to connect to ClickHouse")?;
        Ok(backend)
    }

    async fn post(&self, sql: &str, params: &[(&str, &str)]) -> Result<String> {
        let mut url = self.endpoint.clone();
        if !params.is_empty() {
            let mut q = url.query_pairs_mut();
            for (k, v) in params {
                q.append_pair(&format!("param_{k}"), v);
            }
        }
        let resp = self
            .client
            .post(url)
            .basic_auth(&self.user, Some(&self.password))
            .body(sql.to_string())
            .send()
            .await?;
        let status = resp.status();
        let body = resp.text().await?;
        if !status.is_success() {
            bail!("ClickHouse HTTP {status}: {}", body.trim());
        }
        Ok(body)
    }

    async fn query_rows(&self, sql: &str, params: &[(&str, &str)]) -> Result<Vec<JsonRow>> {
        let body = self.post(sql, params).await?;
        let mut out = Vec::new();
        for line in body.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            match serde_json::from_str::<Value>(line)? {
                Value::Object(obj) => out.push(obj),
                other => bail!("unexpected ClickHouse row shape: {other}"),
            }
        }
        Ok(out)
    }
}

#[async_trait]
impl Database for ClickhouseBackend {
    fn name(&self) -> &'static str {
        "ClickHouse"
    }

    async fn query(&self, sql: &str) -> Result<Vec<JsonRow>> {
        self.query_rows(sql, &[]).await
    }

    async fn list_tables(&self) -> Result<Vec<TableRef>> {
        let rows = self
            .query_rows(
                "SELECT database, name FROM system.tables \
                 WHERE database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema') \
                 ORDER BY database, name",
                &[],
            )
            .await?;
        Ok(rows
            .into_iter()
            .filter_map(|mut r| {
                let schema = r.remove("database")?.as_str()?.to_string();
                let table = r.remove("name")?.as_str()?.to_string();
                Some(TableRef { schema, table })
            })
            .collect())
    }

    async fn describe_table(&self, schema: Option<&str>, table: &str) -> Result<Vec<Column>> {
        let db = schema.unwrap_or("");
        let rows = self
            .query_rows(
                "SELECT name, type FROM system.columns \
                 WHERE table = {tbl:String} \
                   AND database = if({db:String} = '', currentDatabase(), {db:String}) \
                 ORDER BY position",
                &[("tbl", table), ("db", db)],
            )
            .await?;
        Ok(rows
            .into_iter()
            .filter_map(|mut r| {
                let name = r.remove("name")?.as_str()?.to_string();
                let data_type = r.remove("type")?.as_str()?.to_string();
                let nullable = data_type.starts_with("Nullable(");
                Some(Column {
                    name,
                    data_type,
                    nullable,
                })
            })
            .collect())
    }
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Some(h), Some(l)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2]))
        {
            out.push((h << 4) | l);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(out).unwrap_or_else(|_| s.to_string())
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}
