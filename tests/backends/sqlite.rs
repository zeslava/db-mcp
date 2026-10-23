use serde_json::json;
use tempfile::NamedTempFile;

use crate::common::{McpClient, flatten_values};

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn sqlite_e2e() {
    let file = seeded_db();
    let client = McpClient::spawn(&url(&file)).await.expect("spawn mcp");
    run_suite(&client).await;
    client.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn sqlite_http_e2e() {
    let file = seeded_db();
    let (client, addr) = McpClient::spawn_http(&url(&file))
        .await
        .expect("spawn mcp http");
    run_suite(&client).await;

    // DNS rebinding protection: only loopback hosts are accepted by default
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/mcp"))
        .header("Host", "evil.example")
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .body("{}")
        .send()
        .await
        .expect("send request");
    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);

    client.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn sqlite_multi_db_e2e() {
    let users = seeded_db();
    let orders = NamedTempFile::new().expect("tempfile");
    rusqlite::Connection::open(orders.path())
        .expect("open sqlite")
        .execute_batch(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER NOT NULL);
             INSERT INTO orders (id, total) VALUES (10, 500);",
        )
        .expect("seed sqlite");

    let mut config = NamedTempFile::new().expect("tempfile");
    serde_json::to_writer(
        &mut config,
        &json!({"databases": {
            "users": {"url": "sqlite://${USERS_DB_PATH}", "description": "accounts"},
            "orders": {"url_env": "ORDERS_DB_URL"}
        }}),
    )
    .expect("write config");

    let users_path = users.path().display().to_string();
    let orders_url = url(&orders);
    let client = McpClient::spawn_config(
        config.path(),
        &[
            ("USERS_DB_PATH", users_path.as_str()),
            ("ORDERS_DB_URL", orders_url.as_str()),
        ],
    )
    .await
    .expect("spawn mcp");

    let dbs = client.call_json("list_databases", json!({})).await.unwrap();
    assert_eq!(
        dbs,
        json!([
            {"name": "orders", "engine": "SQLite", "description": null},
            {"name": "users", "engine": "SQLite", "description": "accounts"}
        ])
    );

    let rows = client
        .call_json(
            "query",
            json!({"sql": "SELECT total FROM orders", "database": "orders"}),
        )
        .await
        .unwrap();
    assert_eq!(rows, json!([{"total": 500}]));

    let cols = client
        .call_json(
            "describe_table",
            json!({"table": "users", "database": "users"}),
        )
        .await
        .unwrap();
    assert_eq!(cols.as_array().unwrap().len(), 4);

    let missing = client.call("query", json!({"sql": "SELECT 1"})).await;
    assert!(
        missing.is_err(),
        "expected `database` required, got {missing:?}"
    );

    let unknown = client
        .call("list_tables", json!({"database": "nope"}))
        .await;
    assert!(
        unknown.is_err(),
        "expected unknown database error, got {unknown:?}"
    );

    client.shutdown().await;
}

fn url(file: &NamedTempFile) -> String {
    format!("sqlite://{}", file.path().display())
}

fn seeded_db() -> NamedTempFile {
    let file = NamedTempFile::new().expect("tempfile");
    {
        let conn = rusqlite::Connection::open(file.path()).expect("open sqlite");
        conn.execute_batch(
            "CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                payload TEXT
             );
             INSERT INTO users (id, name, payload) VALUES
                (1, 'alice', '{\"role\":\"admin\"}'),
                (2, 'bob',   '{\"role\":\"user\"}');",
        )
        .expect("seed sqlite");
    }
    file
}

async fn run_suite(client: &McpClient) {
    let tables = client.call_json("list_tables", json!({})).await.unwrap();
    assert!(
        tables
            .as_array()
            .unwrap()
            .iter()
            .any(|t| t["table"] == "users"),
        "expected users in {tables:?}"
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
    let plan_rows = plan.as_array().unwrap();
    assert!(!plan_rows.is_empty(), "empty plan");
    assert!(
        plan_rows.iter().all(|r| r.get("detail").is_some()),
        "expected EXPLAIN QUERY PLAN shape, got {plan_rows:?}"
    );
    assert!(flatten_values(&plan).contains("users"));

    let analyzed = client
        .call_json(
            "explain",
            json!({"sql": "SELECT id, name FROM users", "analyze": true}),
        )
        .await
        .expect("explain analyze");
    assert_eq!(analyzed, plan, "analyze flag must be a no-op on SQLite");

    let bad = client
        .call(
            "query",
            json!({"sql": "INSERT INTO users (id, name) VALUES (3, 'eve')"}),
        )
        .await;
    assert!(bad.is_err(), "expected SELECT-only rejection, got {bad:?}");
}
