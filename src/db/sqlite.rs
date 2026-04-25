use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use rusqlite::types::ValueRef;
use rusqlite::{Connection, params_from_iter};
use serde_json::{Map, Value};
use tokio::sync::Mutex;
use tokio::task;

use super::{Column, Database, Row, TableRef};

pub struct SqliteBackend {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteBackend {
    pub async fn open(url: &str) -> Result<Self> {
        let path = parse_url(url)?;
        let conn = task::spawn_blocking(move || -> Result<Connection> {
            let c = if path.as_os_str() == ":memory:" {
                Connection::open_in_memory()?
            } else {
                Connection::open(&path)?
            };
            Ok(c)
        })
        .await
        .context("sqlite open task panicked")??;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }
}

fn parse_url(url: &str) -> Result<PathBuf> {
    let rest = url
        .strip_prefix("sqlite://")
        .or_else(|| url.strip_prefix("sqlite:"))
        .context("sqlite url must start with sqlite:// or sqlite:")?;
    if rest == ":memory:" || rest.is_empty() {
        return Ok(PathBuf::from(":memory:"));
    }
    Ok(PathBuf::from(rest))
}

#[async_trait]
impl Database for SqliteBackend {
    fn name(&self) -> &'static str {
        "SQLite"
    }

    async fn query(&self, sql: &str) -> Result<Vec<Row>> {
        let conn = self.conn.clone();
        let sql = sql.to_string();
        task::spawn_blocking(move || -> Result<Vec<Row>> {
            let conn = conn.blocking_lock();
            let mut stmt = conn.prepare(&sql)?;
            let col_names: Vec<String> =
                stmt.column_names().into_iter().map(String::from).collect();
            let mut rows =
                stmt.query(params_from_iter(std::iter::empty::<&dyn rusqlite::ToSql>()))?;
            let mut out = Vec::new();
            while let Some(row) = rows.next()? {
                let mut obj = Map::new();
                for (i, name) in col_names.iter().enumerate() {
                    obj.insert(name.clone(), value_ref_to_json(row.get_ref(i)?));
                }
                out.push(obj);
            }
            Ok(out)
        })
        .await
        .context("sqlite query task panicked")?
    }

    async fn list_tables(&self) -> Result<Vec<TableRef>> {
        let conn = self.conn.clone();
        task::spawn_blocking(move || -> Result<Vec<TableRef>> {
            let conn = conn.blocking_lock();
            let mut stmt = conn.prepare(
                "SELECT name FROM sqlite_master \
                 WHERE type = 'table' AND name NOT LIKE 'sqlite_%' \
                 ORDER BY name",
            )?;
            let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
            let mut out = Vec::new();
            for r in rows {
                out.push(TableRef {
                    schema: "main".to_string(),
                    table: r?,
                });
            }
            Ok(out)
        })
        .await
        .context("sqlite list_tables task panicked")?
    }

    async fn describe_table(&self, _schema: &str, table: &str) -> Result<Vec<Column>> {
        let conn = self.conn.clone();
        let table = table.to_string();
        task::spawn_blocking(move || -> Result<Vec<Column>> {
            let conn = conn.blocking_lock();
            let pragma = format!("PRAGMA table_info({})", quote_ident(&table));
            let mut stmt = conn.prepare(&pragma)?;
            let rows = stmt.query_map([], |r| {
                Ok(Column {
                    name: r.get::<_, String>(1)?,
                    data_type: r.get::<_, String>(2)?,
                    nullable: r.get::<_, i64>(3)? == 0,
                })
            })?;
            let mut out = Vec::new();
            for r in rows {
                out.push(r?);
            }
            Ok(out)
        })
        .await
        .context("sqlite describe_table task panicked")?
    }
}

// PRAGMA table_info doesn't accept bound parameters — quote the identifier instead.
fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn value_ref_to_json(v: ValueRef<'_>) -> Value {
    match v {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(i) => Value::Number(i.into()),
        ValueRef::Real(f) => serde_json::Number::from_f64(f)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        ValueRef::Text(bytes) => match std::str::from_utf8(bytes) {
            Ok(s) => Value::String(s.to_string()),
            Err(_) => Value::Null,
        },
        ValueRef::Blob(bytes) => Value::String(format!("\\x{}", hex_encode(bytes))),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}
