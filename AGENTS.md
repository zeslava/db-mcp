# AGENTS.md

Guidance for AI coding agents working in this repository.

## Project

Single-binary stdio MCP server exposing read-only PostgreSQL tools.

- Entry point: `src/main.rs` (no modules — keep it that way unless the file grows beyond ~500 lines).
- Stack: `rmcp` 1.5 (MCP SDK), `tokio-postgres`, `clap` for CLI/env config.
- Transport: stdio. Tracing writes to stderr — never log to stdout, it corrupts the JSON-RPC stream.

## Commands

```bash
cargo build
cargo run -- --database-url postgres://user:pass@host/db
DATABASE_URL=postgres://user:pass@host/db cargo run
cargo fmt --all                # run before every commit
cargo fmt --all -- --check     # CI gate
cargo clippy --all-targets -- -D warnings
```

CI runs `cargo fmt --all -- --check` — formatting failures break the build.

## Architecture

- `PgServer` holds `Arc<tokio_postgres::Client>` and a `ToolRouter<PgServer>`.
- Tools are declared with `#[tool]` inside an `impl PgServer` block annotated `#[tool_router]`.
- `ServerHandler` impl uses `#[tool_handler]` to wire routing.

### Tools

- `query` — SELECT-only (rejected otherwise), returns JSON array of row objects.
- `list_tables` — `information_schema.tables`, excludes `pg_catalog` / `information_schema`.
- `describe_table` — `information_schema.columns`, args: `table` + `schema` (default `public`).

### Type mapping (`pg_value_to_json`)

- Explicit branches for `BOOL`, `INT2/4/8`, `FLOAT4/8`, `JSON`, `JSONB`.
- All branches use `Option<T>` via `try_get` so SQL NULL is preserved as `Value::Null` and decoding errors don't masquerade as NULL.
- Fallback (`fallback_to_json`):
  1. Try `&str` (covers `TEXT`, `VARCHAR`, `UUID`-as-text, etc).
  2. For `Kind::Enum` / `Kind::Domain`, decode raw binary bytes as UTF-8 — Postgres sends enum labels as UTF-8 in the binary protocol.
  3. Otherwise return `Value::Null`.
- The `RawBytes` wrapper (`FromSql` accepting any type) exists solely to extract raw bytes for the enum/domain path. Don't widen its use without thinking — raw bytes for arbitrary types are not generally valid UTF-8.

When adding a new explicit type branch, follow the `Option<T>` + `.ok().flatten()` pattern already in place.

## Conventions

- Match existing style; run `cargo fmt` before committing.
- Keep `src/main.rs` flat — no modules, no premature abstraction.
- No comments stating the obvious; only document non-obvious decisions (see the enum/domain comment in `fallback_to_json`).
- Don't add error handling, logging, or tests unless asked.
- Don't add new dependencies without justification.

## Commits

- Format: `(feat|fix|refactor|chore): short message`
- No Claude co-author trailer.
- No bullet-point bodies unless requested.

## Safety

- SELECT-only enforcement in `query` is load-bearing — do not relax it.
- Always parameterized queries, never string-concatenated SQL.
- Don't push to `main` without the user's explicit instruction in the current turn.
