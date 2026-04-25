use serde_json::json;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::clickhouse::ClickHouse;

use crate::common::McpClient;

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn clickhouse_e2e() {
    let container = ClickHouse::default()
        .start()
        .await
        .expect("start clickhouse container");
    let host = container.get_host().await.unwrap();
    let http_port = container.get_host_port_ipv4(8123).await.unwrap();
    let db_url = format!("clickhouse://default@{host}:{http_port}/default");
    let http_url = format!("http://{host}:{http_port}/");

    seed(&http_url).await;

    let client = McpClient::spawn(&db_url).await.expect("spawn mcp");

    let tables = client.call_json("list_tables", json!({})).await.unwrap();
    assert!(
        tables
            .as_array()
            .unwrap()
            .iter()
            .any(|t| t["schema"] == "default" && t["table"] == "users"),
        "expected default.users in {tables:?}"
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
    assert_eq!(rows[0]["id"], 1);
    assert_eq!(rows[0]["name"], "alice");

    let bad = client
        .call(
            "query",
            json!({"sql": "INSERT INTO users VALUES (3, 'eve', now(), '')"}),
        )
        .await;
    assert!(bad.is_err(), "expected SELECT-only rejection, got {bad:?}");

    client.shutdown().await;
}

async fn seed(http_url: &str) {
    let http = reqwest::Client::new();
    let exec = |sql: &'static str| {
        let http = http.clone();
        let url = http_url.to_string();
        async move {
            let resp = http.post(&url).body(sql).send().await.expect("ch http");
            assert!(
                resp.status().is_success(),
                "clickhouse seed failed: {} {}",
                resp.status(),
                resp.text().await.unwrap_or_default()
            );
        }
    };
    exec(
        "CREATE TABLE users (
            id UInt32,
            name String,
            created_at DateTime,
            payload String
         ) ENGINE = MergeTree ORDER BY id",
    )
    .await;
    exec(
        "INSERT INTO users VALUES \
         (1, 'alice', now(), '{\"role\":\"admin\"}'), \
         (2, 'bob',   now(), '{\"role\":\"user\"}')",
    )
    .await;
}
