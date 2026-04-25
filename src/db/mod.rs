use async_trait::async_trait;
use serde_json::{Map, Value};

#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "sqlite")]
pub mod sqlite;

pub type Row = Map<String, Value>;

pub struct TableRef {
    pub schema: String,
    pub table: String,
}

pub struct Column {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

#[async_trait]
pub trait Database: Send + Sync {
    fn name(&self) -> &'static str;
    async fn query(&self, sql: &str) -> anyhow::Result<Vec<Row>>;
    async fn list_tables(&self) -> anyhow::Result<Vec<TableRef>>;
    async fn describe_table(&self, schema: &str, table: &str) -> anyhow::Result<Vec<Column>>;
}
