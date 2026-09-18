use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use klickhouse::{
    Client, ClientOptions, FromSql, ParsedQuery, QueryBuilder, Type, Value, block::Block,
};
use serde_json::{Number, Value as Json};
use url::Url;

use super::{Column, Database, Row as JsonRow, TableRef};

pub struct ClickhouseNativeBackend {
    client: Client,
}

impl ClickhouseNativeBackend {
    pub async fn connect(url: &str) -> Result<Self> {
        let parsed = Url::parse(url).context("invalid ClickHouse url")?;
        let tls = match parsed.scheme() {
            "clickhouse" | "clickhouse+native" | "ch" => false,
            "clickhouses" | "clickhouse+natives" | "chs" => true,
            other => bail!("unsupported clickhouse native scheme: {other}"),
        };
        let host = parsed.host_str().context("clickhouse url missing host")?;
        let port = parsed.port().unwrap_or(if tls { 9440 } else { 9000 });
        let options = ClientOptions {
            username: match percent_decode(parsed.username()) {
                u if u.is_empty() => "default".to_string(),
                u => u,
            },
            password: parsed.password().map(percent_decode).unwrap_or_default(),
            default_database: percent_decode(parsed.path().trim_start_matches('/')),
            ..Default::default()
        };

        let client = if tls {
            connect_tls(host, port, options).await
        } else {
            Client::connect((host, port), options)
                .await
                .map_err(anyhow::Error::from)
        }
        .context("Failed to connect to ClickHouse")?;

        Ok(Self { client })
    }

    async fn collect(
        &self,
        query: impl TryInto<ParsedQuery, Error = klickhouse::KlickhouseError>,
    ) -> Result<Vec<JsonRow>> {
        let mut blocks = self.client.query_raw(query).await?.into_inner();
        let mut out = Vec::new();
        while let Some(block) = blocks.recv().await {
            let block: Block = block?;
            if block.rows == 0 {
                continue;
            }
            for row in block.into_iter_rows() {
                let mut obj = JsonRow::new();
                for (name, (type_, value)) in row {
                    obj.insert(name, value_to_json(&type_, value));
                }
                out.push(obj);
            }
        }
        Ok(out)
    }
}

#[async_trait]
impl Database for ClickhouseNativeBackend {
    fn name(&self) -> &'static str {
        "ClickHouse"
    }

    async fn query(&self, sql: &str) -> Result<Vec<JsonRow>> {
        self.collect(sql).await
    }

    async fn explain(&self, sql: &str, analyze: bool) -> Result<Vec<JsonRow>> {
        if analyze {
            bail!("ClickHouse does not support EXPLAIN ANALYZE");
        }
        self.collect(format!("EXPLAIN {sql}")).await
    }

    async fn list_tables(&self) -> Result<Vec<TableRef>> {
        let rows = self
            .collect(
                "SELECT database, name FROM system.tables \
                 WHERE database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema') \
                 ORDER BY database, name",
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
        let query = QueryBuilder::new(
            "SELECT name, type FROM system.columns \
             WHERE table = $1 AND database = if($2 = '', currentDatabase(), $2) \
             ORDER BY position",
        )
        .arg(table)
        .arg(db);
        let rows = self.collect(query).await?;
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

async fn connect_tls(host: &str, port: u16, options: ClientOptions) -> Result<Client> {
    use std::sync::Arc;

    let roots = rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
    let name = rustls_pki_types::ServerName::try_from(host.to_string())
        .context("invalid TLS server name")?;
    Ok(Client::connect_tls((host, port), options, name, &connector).await?)
}

fn value_to_json(type_: &Type, value: Value) -> Json {
    match value {
        Value::Null => Json::Null,
        Value::Int8(x) => x.into(),
        Value::Int16(x) => x.into(),
        Value::Int32(x) => x.into(),
        Value::Int64(x) => x.into(),
        Value::UInt8(x) => x.into(),
        Value::UInt16(x) => x.into(),
        Value::UInt32(x) => x.into(),
        Value::UInt64(x) => x.into(),
        // wider than JSON numbers can carry without loss
        Value::Int128(x) => x.to_string().into(),
        Value::UInt128(x) => x.to_string().into(),
        Value::Int256(x) => x.to_string().into(),
        Value::UInt256(x) => x.to_string().into(),
        Value::Float32(x) => float_to_json(x as f64),
        Value::Float64(x) => float_to_json(x),
        Value::BFloat16(x) => float_to_json(x.to_f64()),
        Value::Decimal32(p, x) => decimal_to_json(p, x.to_string()),
        Value::Decimal64(p, x) => decimal_to_json(p, x.to_string()),
        Value::Decimal128(p, x) => decimal_to_json(p, x.to_string()),
        Value::Decimal256(p, x) => decimal_to_json(p, x.to_string()),
        Value::String(bytes) => String::from_utf8_lossy(&bytes).into_owned().into(),
        Value::Uuid(x) => x.to_string().into(),
        Value::Ipv4(x) => x.to_string().into(),
        Value::Ipv6(x) => x.to_string().into(),
        Value::Date(_) | Value::DateTime(_) | Value::DateTime64(_) => {
            datetime_to_json(type_, value)
        }
        Value::Enum8(x) => enum_to_json(type_, x as i16),
        Value::Enum16(x) => enum_to_json(type_, x),
        Value::Array(items) => {
            let inner = type_.strip_null().unarray().unwrap_or(&Type::String);
            Json::Array(items.into_iter().map(|v| value_to_json(inner, v)).collect())
        }
        Value::Tuple(items) => {
            let inner = type_.strip_null().untuple();
            Json::Array(
                items
                    .into_iter()
                    .enumerate()
                    .map(|(i, v)| {
                        value_to_json(inner.and_then(|t| t.get(i)).unwrap_or(&Type::String), v)
                    })
                    .collect(),
            )
        }
        Value::Map(keys, values) => {
            let (kt, vt) = type_
                .strip_null()
                .unmap()
                .unwrap_or((&Type::String, &Type::String));
            let mut obj = serde_json::Map::new();
            for (k, v) in keys.into_iter().zip(values) {
                let key = match value_to_json(kt, k) {
                    Json::String(s) => s,
                    other => other.to_string(),
                };
                obj.insert(key, value_to_json(vt, v));
            }
            Json::Object(obj)
        }
        Value::Point(p) => point_to_json(&p),
        Value::Ring(r) => ring_to_json(&r),
        Value::Polygon(p) => Json::Array(p.0.iter().map(ring_to_json).collect()),
        Value::MultiPolygon(mp) => Json::Array(
            mp.0.iter()
                .map(|p| Json::Array(p.0.iter().map(ring_to_json).collect()))
                .collect(),
        ),
    }
}

fn point_to_json(p: &klickhouse::Point) -> Json {
    Json::Array(vec![float_to_json(p.0[0]), float_to_json(p.0[1])])
}

fn ring_to_json(r: &klickhouse::Ring) -> Json {
    Json::Array(r.0.iter().map(point_to_json).collect())
}

fn float_to_json(x: f64) -> Json {
    Number::from_f64(x).map(Json::Number).unwrap_or(Json::Null)
}

/// ClickHouse decimals arrive as a scaled integer; render them as an exact
/// decimal string rather than a lossy f64.
fn decimal_to_json(scale: usize, raw: String) -> Json {
    if scale == 0 {
        return raw.into();
    }
    let (sign, digits) = match raw.strip_prefix('-') {
        Some(rest) => ("-", rest),
        None => ("", &raw[..]),
    };
    let padded = format!("{digits:0>width$}", width = scale + 1);
    let split = padded.len() - scale;
    format!("{sign}{}.{}", &padded[..split], &padded[split..]).into()
}

fn datetime_to_json(type_: &Type, value: Value) -> Json {
    let type_ = type_.strip_null();
    if let Value::Date(date) = value {
        return chrono::NaiveDate::from(date).to_string().into();
    }
    match chrono::DateTime::<klickhouse::Tz>::from_sql(type_, value) {
        Ok(dt) => dt
            .to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true)
            .into(),
        Err(_) => Json::Null,
    }
}

fn enum_to_json(type_: &Type, value: i16) -> Json {
    let names = match type_.strip_null() {
        Type::Enum8(pairs) => pairs
            .iter()
            .find(|(_, v)| *v as i16 == value)
            .map(|(n, _)| n.clone()),
        Type::Enum16(pairs) => pairs
            .iter()
            .find(|(_, v)| *v == value)
            .map(|(n, _)| n.clone()),
        _ => None,
    };
    match names {
        Some(name) => name.into(),
        None => value.into(),
    }
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 3 <= bytes.len()
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
