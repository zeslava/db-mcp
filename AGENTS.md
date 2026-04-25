# AGENTS.md

Guidance for AI coding agents working in this repository.

## Project

Single-binary stdio MCP server exposing read-only SQL tools across multiple database engines.

- Entry point: `src/main.rs` — CLI parsing, URL-scheme dispatch to a backend, server bootstrap.
- Server layer: `src/server.rs` — `DbServer` holding `Arc<dyn Database>` and a `ToolRouter<DbServer>`. Tools are engine-agnostic.
- Backends: `src/db/<engine>.rs` — each implements the `Database` trait from `src/db/mod.rs`. Engines are gated by Cargo features.
- Stack: `rmcp` 1.5 (MCP SDK), `clap` for CLI/env config, `async-trait` for the dyn-compatible backend port.
- Transport: stdio. Tracing writes to stderr — never log to stdout, it corrupts the JSON-RPC stream.

## Commands

```bash
cargo build                    # default features
cargo build --no-default-features --features postgres
cargo run -- --database-url postgres://user:pass@host/db
cargo run -- --database-url sqlite:///absolute/path/to.db
DATABASE_URL=postgres://user:pass@host/db cargo run
cargo fmt --all                # run before every commit
cargo fmt --all -- --check     # CI gate
cargo clippy --all-targets --all-features -- -D warnings
```

CI runs `cargo fmt --all -- --check` — formatting failures break the build.

## Architecture

### Port — `db::Database` (`src/db/mod.rs`)

```rust
#[async_trait]
pub trait Database: Send + Sync {
    fn name(&self) -> &'static str;
    async fn query(&self, sql: &str) -> anyhow::Result<Vec<Row>>;
    async fn list_tables(&self) -> anyhow::Result<Vec<TableRef>>;
    async fn describe_table(&self, schema: &str, table: &str) -> anyhow::Result<Vec<Column>>;
}
```

`Row` is `serde_json::Map<String, Value>` — backends are responsible for converting their native types into JSON values.

### Adapters

Each backend lives in its own file behind a Cargo feature:

- `src/db/postgres.rs` (feature `postgres`) — `tokio-postgres`, text protocol via `simple_query`, post-processing in `text_to_json`.
- `src/db/sqlite.rs` (feature `sqlite`) — `rusqlite` driven via `spawn_blocking`.

Adding a new engine:
1. Add the driver dep as `optional = true` in `Cargo.toml` and a feature that pulls it in.
2. Create `src/db/<engine>.rs` with a `Backend` struct and `impl Database`.
3. Wire the URL scheme in `main::main`.
4. Document the type-mapping in README.

### Server

`src/server.rs::DbServer` declares the tools (`query`, `list_tables`, `describe_table`) once with `#[tool_router]` / `#[tool_handler]`. The SELECT-only enforcement lives here, not in adapters.

## Conventions

- Match existing style; run `cargo fmt` before committing.
- Keep adapters self-contained — no leaking driver types through the `Database` trait.
- No comments stating the obvious; only document non-obvious decisions.
- Don't add error handling, logging, or tests unless asked.
- Don't add new dependencies without justification.

## Commits

- Format: `(feat|fix|refactor|chore): short message`
- No Claude co-author trailer.
- No bullet-point bodies unless requested.

## Safety

- SELECT-only enforcement in `DbServer::query` is load-bearing — do not relax it. It rejects anything not starting with `SELECT` (case-insensitive after trim), so CTEs with `INSERT ... RETURNING` are blocked.
- Always parameterized queries, never string-concatenated SQL — applies to every adapter.
- Don't push to `main` without the user's explicit instruction in the current turn.
