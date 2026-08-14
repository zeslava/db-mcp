use serde_json::json;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::NoTls;

use crate::common::{McpClient, flatten_values};

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn postgres_e2e() {
    let container = Postgres::default()
        .start()
        .await
        .expect("start postgres container");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    seed(&url).await;

    let client = McpClient::spawn(&url).await.expect("spawn mcp");

    let tables = client
        .call_json("list_tables", json!({}))
        .await
        .expect("list_tables");
    let tables = tables.as_array().unwrap();
    assert!(
        tables
            .iter()
            .any(|t| t["schema"] == "public" && t["table"] == "users"),
        "expected public.users in {tables:?}"
    );

    let cols = client
        .call_json(
            "describe_table",
            json!({"table": "users", "schema": "public"}),
        )
        .await
        .expect("describe_table");
    let cols = cols.as_array().unwrap();
    let names: Vec<&str> = cols.iter().map(|c| c["column"].as_str().unwrap()).collect();
    assert_eq!(names, vec!["id", "name", "created_at", "payload"]);

    let rows = client
        .call_json(
            "query",
            json!({"sql": "SELECT id, name, payload FROM users ORDER BY id"}),
        )
        .await
        .expect("query");
    let rows = rows.as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["id"], 1);
    assert_eq!(rows[0]["name"], "alice");
    assert_eq!(rows[0]["payload"]["role"], "admin");

    let plan = client
        .call_json(
            "explain",
            json!({"sql": "SELECT id, name FROM users ORDER BY id"}),
        )
        .await
        .expect("explain");
    assert!(!plan.as_array().unwrap().is_empty(), "empty plan");
    let plan_text = flatten_values(&plan);
    assert!(plan_text.contains("users"), "unexpected plan: {plan_text}");

    let analyzed = client
        .call_json(
            "explain",
            json!({"sql": "SELECT id, name FROM users ORDER BY id", "analyze": true}),
        )
        .await
        .expect("explain analyze");
    let analyzed_text = flatten_values(&analyzed);
    assert!(
        analyzed_text.contains("actual time"),
        "expected ANALYZE timings, got: {analyzed_text}"
    );

    let bad = client
        .call(
            "query",
            json!({"sql": "INSERT INTO users (id, name) VALUES (3, 'eve')"}),
        )
        .await;
    assert!(bad.is_err(), "expected SELECT-only rejection, got {bad:?}");

    let bad = client
        .call(
            "explain",
            json!({"sql": "INSERT INTO users (id, name) VALUES (3, 'eve')"}),
        )
        .await;
    assert!(bad.is_err(), "expected SELECT-only rejection, got {bad:?}");

    client.shutdown().await;
}

async fn seed(url: &str) {
    let (cli, conn) = tokio_postgres::connect(url, NoTls)
        .await
        .expect("connect pg");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    cli.batch_execute(
        "CREATE TABLE users (
            id INT PRIMARY KEY,
            name TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            payload JSONB
         );
         INSERT INTO users (id, name, payload) VALUES
            (1, 'alice', '{\"role\":\"admin\"}'),
            (2, 'bob',   '{\"role\":\"user\"}');",
    )
    .await
    .expect("seed pg");
}
