use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde_json::Map;
use tokio_postgres::types::Type;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

use super::{Column, Database, Row, TableRef};

pub struct PgBackend {
    client: Arc<Client>,
}

impl PgBackend {
    pub async fn connect(url: &str) -> Result<Self> {
        let (client, connection) = tokio_postgres::connect(url, NoTls)
            .await
            .context("Failed to connect to PostgreSQL")?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("PostgreSQL connection error: {e}");
            }
        });

        Ok(Self {
            client: Arc::new(client),
        })
    }
}

#[async_trait]
impl Database for PgBackend {
    fn name(&self) -> &'static str {
        "PostgreSQL"
    }

    async fn query(&self, sql: &str) -> Result<Vec<Row>> {
        let stmt = self.client.prepare(sql).await?;
        let col_names: Vec<String> = stmt
            .columns()
            .iter()
            .map(|c| c.name().to_string())
            .collect();
        let col_types: Vec<Type> = stmt.columns().iter().map(|c| c.type_().clone()).collect();

        let messages = self.client.simple_query(sql).await?;

        let mut result: Vec<Row> = Vec::new();
        for msg in messages {
            if let SimpleQueryMessage::Row(row) = msg {
                let mut obj = Map::new();
                for (i, name) in col_names.iter().enumerate() {
                    obj.insert(name.clone(), text_to_json(row.get(i), col_types.get(i)));
                }
                result.push(obj);
            }
        }
        Ok(result)
    }

    async fn list_tables(&self) -> Result<Vec<TableRef>> {
        let rows = self
            .client
            .query(
                "SELECT table_schema, table_name \
                 FROM information_schema.tables \
                 WHERE table_schema NOT IN ('pg_catalog', 'information_schema') \
                 ORDER BY table_schema, table_name",
                &[],
            )
            .await?;

        Ok(rows
            .iter()
            .map(|r| TableRef {
                schema: r.get::<_, &str>(0).to_string(),
                table: r.get::<_, &str>(1).to_string(),
            })
            .collect())
    }

    async fn describe_table(&self, schema: &str, table: &str) -> Result<Vec<Column>> {
        let rows = self
            .client
            .query(
                "SELECT column_name, data_type, is_nullable \
                 FROM information_schema.columns \
                 WHERE table_name = $1 AND table_schema = $2 \
                 ORDER BY ordinal_position",
                &[&table, &schema],
            )
            .await?;

        Ok(rows
            .iter()
            .map(|r| Column {
                name: r.get::<_, &str>(0).to_string(),
                data_type: r.get::<_, &str>(1).to_string(),
                nullable: r.get::<_, &str>(2) == "YES",
            })
            .collect())
    }
}

fn text_to_json(text: Option<&str>, col_type: Option<&Type>) -> serde_json::Value {
    let Some(s) = text else {
        return serde_json::Value::Null;
    };
    let Some(ty) = col_type else {
        return serde_json::Value::String(s.to_string());
    };
    match ty {
        &Type::BOOL => match s {
            "t" | "true" => serde_json::Value::Bool(true),
            "f" | "false" => serde_json::Value::Bool(false),
            _ => serde_json::Value::String(s.to_string()),
        },
        &Type::INT2 | &Type::INT4 | &Type::INT8 | &Type::OID => s
            .parse::<i64>()
            .map(|v| serde_json::Value::Number(v.into()))
            .unwrap_or_else(|_| serde_json::Value::String(s.to_string())),
        &Type::FLOAT4 | &Type::FLOAT8 => s
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map(serde_json::Value::Number)
            .unwrap_or_else(|| serde_json::Value::String(s.to_string())),
        &Type::JSON | &Type::JSONB => {
            serde_json::from_str(s).unwrap_or_else(|_| serde_json::Value::String(s.to_string()))
        }
        _ => serde_json::Value::String(s.to_string()),
    }
}
