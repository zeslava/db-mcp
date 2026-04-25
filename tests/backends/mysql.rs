use mysql_async::{Pool, prelude::Queryable};
use serde_json::json;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::mysql::Mysql;

use crate::common::McpClient;

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn mysql_e2e() {
    let container = Mysql::default()
        .start()
        .await
        .expect("start mysql container");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(3306).await.unwrap();
    let db_name = "test";
    let url = format!("mysql://root@{host}:{port}/{db_name}");

    seed(&url).await;

    let client = McpClient::spawn(&url).await.expect("spawn mcp");

    let tables = client.call_json("list_tables", json!({})).await.unwrap();
    let tables = tables.as_array().unwrap();
    assert!(
        tables
            .iter()
            .any(|t| t["schema"] == db_name && t["table"] == "users"),
        "expected {db_name}.users in {tables:?}"
    );

    let cols = client
        .call_json("describe_table", json!({"table": "users"}))
        .await
        .unwrap();
    let names: Vec<&str> = cols
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["column"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["id", "name", "created_at", "payload"]);

    let rows = client
        .call_json(
            "query",
            json!({"sql": "SELECT id, name, payload FROM users ORDER BY id"}),
        )
        .await
        .unwrap();
    let rows = rows.as_array().unwrap();
    assert_eq!(rows.len(), 2);
    let id0 = &rows[0]["id"];
    assert!(
        id0.as_i64() == Some(1) || id0.as_str() == Some("1"),
        "unexpected id repr: {id0:?}"
    );
    assert_eq!(rows[0]["name"], "alice");
    assert_eq!(rows[0]["payload"]["role"], "admin");

    let bad = client
        .call(
            "query",
            json!({"sql": "INSERT INTO users (id, name) VALUES (3, 'eve')"}),
        )
        .await;
    assert!(bad.is_err(), "expected SELECT-only rejection, got {bad:?}");

    client.shutdown().await;
}

async fn seed(url: &str) {
    let pool = Pool::new(url);
    let mut conn = pool.get_conn().await.expect("mysql connect");
    conn.query_drop(
        "CREATE TABLE users (
            id INT PRIMARY KEY,
            name VARCHAR(64) NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            payload JSON
         )",
    )
    .await
    .expect("create users");
    conn.query_drop(
        "INSERT INTO users (id, name, payload) VALUES
            (1, 'alice', JSON_OBJECT('role','admin')),
            (2, 'bob',   JSON_OBJECT('role','user'))",
    )
    .await
    .expect("insert users");
    drop(conn);
    let _ = pool.disconnect().await;
}
