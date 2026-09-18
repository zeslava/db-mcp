use serde_json::json;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::clickhouse::ClickHouse;

use crate::common::McpClient;

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn clickhouse_http_e2e() {
    let container = ClickHouse::default()
        .start()
        .await
        .expect("start clickhouse container");
    let host = container.get_host().await.unwrap();
    let http_port = container.get_host_port_ipv4(8123).await.unwrap();
    seed(&format!("http://{host}:{http_port}/")).await;

    run_suite(&format!(
        "clickhouse+http://default@{host}:{http_port}/default"
    ))
    .await;
}

#[cfg(feature = "clickhouse-native")]
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn clickhouse_native_e2e() {
    let container = ClickHouse::default()
        .start()
        .await
        .expect("start clickhouse container");
    let host = container.get_host().await.unwrap();
    let http_port = container.get_host_port_ipv4(8123).await.unwrap();
    let native_port = container.get_host_port_ipv4(9000).await.unwrap();
    seed(&format!("http://{host}:{http_port}/")).await;

    let db_url = format!("clickhouse://default@{host}:{native_port}/default");
    run_suite(&db_url).await;

    // native protocol decodes typed columns itself, unlike the JSON-over-HTTP path
    let client = McpClient::spawn(&db_url).await.expect("spawn mcp");
    let rows = client
        .call_json(
            "query",
            json!({"sql": "SELECT price, kind, tags, meta, day FROM typed ORDER BY price"}),
        )
        .await
        .unwrap();
    let rows = rows.as_array().unwrap();
    assert_eq!(rows[0]["price"], "123.45");
    assert_eq!(rows[0]["kind"], "b");
    assert_eq!(rows[0]["tags"], json!(["x", "y"]));
    assert_eq!(rows[0]["meta"], json!({"k": 7}));
    assert_eq!(rows[0]["day"], "2024-05-01");
    client.shutdown().await;
}

async fn run_suite(db_url: &str) {
    let client = McpClient::spawn(db_url).await.expect("spawn mcp");

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

    let plan = client
        .call_json("explain", json!({"sql": "SELECT id, name FROM users"}))
        .await
        .expect("explain");
    assert!(!plan.as_array().unwrap().is_empty(), "empty plan");

    let unsupported = client
        .call(
            "explain",
            json!({"sql": "SELECT id, name FROM users", "analyze": true}),
        )
        .await;
    assert!(
        unsupported.is_err(),
        "expected EXPLAIN ANALYZE rejection, got {unsupported:?}"
    );

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
    exec(
        "CREATE TABLE typed (
            price Decimal(10, 2),
            kind Enum8('a' = 1, 'b' = 2),
            tags Array(String),
            meta Map(String, UInt32),
            day Date
         ) ENGINE = MergeTree ORDER BY price",
    )
    .await;
    exec("INSERT INTO typed VALUES (123.45, 'b', ['x', 'y'], {'k': 7}, '2024-05-01')").await;
}
