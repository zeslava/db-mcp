use serde_json::json;
use tempfile::NamedTempFile;

use crate::common::McpClient;

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn sqlite_e2e() {
    let file = NamedTempFile::new().expect("tempfile");
    let path = file.path().to_path_buf();
    let url = format!("sqlite://{}", path.display());

    {
        let conn = rusqlite::Connection::open(&path).expect("open sqlite");
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

    let client = McpClient::spawn(&url).await.expect("spawn mcp");

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

    let bad = client
        .call(
            "query",
            json!({"sql": "INSERT INTO users (id, name) VALUES (3, 'eve')"}),
        )
        .await;
    assert!(bad.is_err(), "expected SELECT-only rejection, got {bad:?}");

    client.shutdown().await;
}
