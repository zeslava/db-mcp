# AGENTS.md

Guidance for AI coding agents working in this repository.

## Project

Single-binary stdio MCP server exposing read-only SQL tools across multiple database engines.

- Entry point: `src/main.rs` — CLI parsing, URL-scheme dispatch to a backend, server bootstrap.
- Server layer: `src/server.rs` — `DbServer` holding `Arc<dyn Database>` and a `ToolRouter<DbServer>`. Tools are engine-agnostic.
- Backends: `src/db/<engine>.rs` — each implements the `Database` trait from `src/db/mod.rs`. Most engines are always compiled in; SQLite is behind the `sqlite` Cargo feature (enabled by default).
- Stack: `rmcp` 1.5 (MCP SDK), `clap` for CLI/env config, `async-trait` for the dyn-compatible backend port. Drivers: `tokio-postgres`, `mysql_async`, `rusqlite` (optional, feature `sqlite`), `reqwest` (ClickHouse HTTP).
- Release Linux targets use `*-unknown-linux-musl` for fully static binaries (no glibc at runtime). Cross-compilation via `taiki-e/setup-cross-toolchain-action`.
- Transport: stdio. Tracing writes to stderr — never log to stdout, it corrupts the JSON-RPC stream.

## Commands

```bash
cargo build
cargo run -- --database-url postgres://user:pass@host/db
cargo run -- --database-url mysql://user:pass@host/db
cargo run -- --database-url sqlite:///absolute/path/to.db
cargo run -- --database-url clickhouse://default:pass@host:8123/db
DATABASE_URL=postgres://user:pass@host/db cargo run
cargo fmt --all                # run before every commit
cargo fmt --all -- --check     # CI gate
cargo clippy --all-targets -- -D warnings
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
    async fn describe_table(&self, schema: Option<&str>, table: &str)
        -> anyhow::Result<Vec<Column>>;
}
```

`Row` is `serde_json::Map<String, Value>` — backends are responsible for converting their native types into JSON values.

`schema = None` means "use the engine's natural default": PostgreSQL → `public`, MySQL → `DATABASE()` (current connection DB), SQLite → ignored (no schema concept).

### Adapters

- `src/db/postgres.rs` — `tokio-postgres`, text protocol via `simple_query`, post-processing in `text_to_json`.
- `src/db/mysql.rs` — `mysql_async` pool, binary protocol via `Row`/`Value`, JSON columns parsed when `ColumnType::MYSQL_TYPE_JSON`.
- `src/db/sqlite.rs` — `rusqlite` driven via `spawn_blocking`.
- `src/db/clickhouse.rs` — HTTP interface via `reqwest`, `JSONEachRow` format with `output_format_json_quote_64bit_integers=0`, parameterized via `param_*` URL params. Schemes: `clickhouse://` (HTTP, port 8123) / `clickhouse+https://` (HTTPS, port 8443).

Adding a new engine:
1. Add the driver dep to `Cargo.toml`.
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

## Integration tests

E2E-тесты в `tests/` поднимают БД в Docker через `testcontainers` (`postgres`, `mysql`, `clickhouse`) либо во временном файле (`sqlite`), накатывают схему и сидят данные родным драйвером, запускают `db-mcp` как дочерний процесс и общаются по stdio через `rmcp`-клиента.

```bash
cargo test --test integration -- --ignored
cargo test --test integration postgres -- --ignored
```

Все тесты `#[ignore]` — обычный `cargo test` их не трогает. Требует доступного Docker для трёх контейнерных бэкендов.

## Safety

- SELECT-only enforcement in `DbServer::query` is load-bearing — do not relax it. It rejects anything not starting with `SELECT` (case-insensitive after trim), so CTEs with `INSERT ... RETURNING` are blocked.
- Always parameterized queries, never string-concatenated SQL — applies to every adapter.
- Don't push to `main` without the user's explicit instruction in the current turn.
