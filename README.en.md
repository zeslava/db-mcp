# db-mcp

[Русский](./README.md) | **English**

MCP server for SQL databases with read-only access over stdio or HTTP (Streamable HTTP). A single binary with all engines built in — the engine is selected by URL scheme.

Supported engines:

| URL scheme | Engine | Driver |
|------------|--------|--------|
| `postgres://` / `postgresql://` | PostgreSQL | `tokio-postgres` |
| `mysql://` | MySQL / MariaDB | `mysql_async` |
| `sqlite://` / `sqlite:` | SQLite | `rusqlite` (bundled) |
| `clickhouse://` / `clickhouses://` | ClickHouse | native protocol (`klickhouse`) |
| `clickhouse+http://` / `clickhouse+https://` | ClickHouse | HTTP (`reqwest`) |

## Installation

### Quick install (Linux and macOS Apple Silicon)

```bash
curl --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/zeslava/db-mcp/main/install.sh | sh
```

The script detects the OS and architecture, downloads the latest release, verifies the checksum, and by default installs the binary to `~/.local/bin` (no `sudo`). Override the install location with `INSTALL_DIR`:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/zeslava/db-mcp/main/install.sh \
  | INSTALL_DIR=/usr/local/bin sh
```

### Manually from releases

Prebuilt binaries for Linux (x86_64, aarch64), macOS (arm64) and Windows (x86_64) are published on the [Releases page](https://github.com/zeslava/db-mcp/releases).

```bash
TARGET=x86_64-unknown-linux-gnu
curl -sSL "https://github.com/zeslava/db-mcp/releases/latest/download/db-mcp-${TARGET}.tar.gz" \
  | tar -xz
install -m 755 "db-mcp-${TARGET}/db-mcp" "$HOME/.local/bin/db-mcp"
```

Checksum verification:

```bash
curl -sSL -O "https://github.com/zeslava/db-mcp/releases/latest/download/db-mcp-${TARGET}.tar.gz.sha256"
shasum -a 256 -c "db-mcp-${TARGET}.tar.gz.sha256"
```

Older releases stay on the Releases page as history, but their assets are removed — use `latest/download/...`.

### From source

```bash
cargo build --release
# binary: ./target/release/db-mcp
```

## Running

The URL is passed via the `--database-url` flag or the `DATABASE_URL` variable. The URL scheme determines which adapter is initialized.

```bash
./target/release/db-mcp --database-url postgres://user:pass@localhost:5432/mydb
./target/release/db-mcp --database-url mysql://user:pass@localhost:3306/mydb
./target/release/db-mcp --database-url sqlite:///absolute/path/to/data.db
./target/release/db-mcp --database-url clickhouse://default:pass@localhost:9000/default
./target/release/db-mcp --database-url clickhouses://user:pass@host.cloud:9440/default
./target/release/db-mcp --database-url clickhouse+http://default:pass@localhost:8123/default
./target/release/db-mcp --database-url clickhouse+https://user:pass@host.cloud:8443/default
DATABASE_URL=sqlite::memory: ./target/release/db-mcp
```

To keep credentials out of the client config, put the URL in an env file and pass it via `--env-file` or `ENV_FILE`:

```bash
echo 'DATABASE_URL=postgres://user:pass@localhost:5432/mydb' > ~/.config/db-mcp.env
./target/release/db-mcp --env-file ~/.config/db-mcp.env
ENV_FILE=~/.config/db-mcp.env ./target/release/db-mcp
```

Variables already set in the environment take precedence over values from the file.

### Multiple databases in one process

Via a JSON config (`--config` / `DB_MCP_CONFIG`, mutually exclusive with `DATABASE_URL`):

```json
{
  "databases": {
    "billing": {
      "url": "postgres://ro:${BILLING_PASS}@billing-db:5432/billing",
      "description": "payments, invoices, subscriptions"
    },
    "auth": {
      "url_env": "AUTH_DATABASE_URL",
      "description": "users, sessions, roles"
    }
  }
}
```

```bash
./target/release/db-mcp --config db-mcp.json --env-file .env
```

- `url` supports `${VAR}` substitution; `url_env` takes the whole URL from a variable. Exactly one of the two is required. Secrets can live in `--env-file` while the config is committed.
- `description` goes into the server instructions and `list_databases` — the model uses it to pick which database to query. Describe what data the database holds.
- The server connects to all databases at startup; if any of them is unreachable, it does not start.
- With more than one database, the `database` parameter is required in tools; omitting it or passing an unknown name returns an error listing the available ones.
- With `DATABASE_URL` there is a single database named `default`; `database` may be omitted.

### HTTP mode

By default the server runs over stdio. For Streamable HTTP:

```bash
./target/release/db-mcp --transport http --bind 127.0.0.1:8080
# or MCP_TRANSPORT=http MCP_BIND=127.0.0.1:8080
```

The endpoint is `http://<bind>/mcp`. There is one connection per database per process, shared by all sessions; with `--config`, every client sees all configured databases.

The server validates the `Host` header (DNS rebinding protection): by default only `localhost`, `127.0.0.1`, `::1` are accepted. When listening on an external interface, set your own hosts via `--allowed-hosts` / `MCP_ALLOWED_HOSTS` (comma-separated, `host:port` allowed).

HTTP mode has no authentication — do not expose the port without a reverse proxy with authorization.

Logs go to stderr, JSON-RPC to stdout. Log level is set via `RUST_LOG` (e.g. `RUST_LOG=debug`).

## Connecting clients

### Claude Code (CLI)

```bash
claude mcp add db \
  --env DATABASE_URL=postgres://user:pass@localhost:5432/mydb \
  -- /absolute/path/to/target/release/db-mcp
```

Or manually in `~/.claude.json` (`mcpServers` section):

```json
{
  "mcpServers": {
    "db": {
      "command": "/absolute/path/to/target/release/db-mcp",
      "args": [],
      "env": {
        "DATABASE_URL": "postgres://user:pass@localhost:5432/mydb"
      }
    }
  }
}
```

For HTTP mode (server running separately):

```bash
claude mcp add --transport http db http://127.0.0.1:8080/mcp
```

### Claude Desktop

`~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or
`%APPDATA%\Claude\claude_desktop_config.json` (Windows):

```json
{
  "mcpServers": {
    "db": {
      "command": "/absolute/path/to/target/release/db-mcp",
      "env": {
        "DATABASE_URL": "postgres://user:pass@localhost:5432/mydb"
      }
    }
  }
}
```

### Cursor / Windsurf / Zed

```json
{
  "db-mcp": {
    "command": "/absolute/path/to/target/release/db-mcp",
    "env": { "DATABASE_URL": "sqlite:///absolute/path/to/data.db" }
  }
}
```

### Multiple databases

Either a single server with `--config` (see [above](#multiple-databases-in-one-process)), or a separate server per database:

```json
{
  "mcpServers": {
    "pg-prod": {
      "command": "/path/to/db-mcp",
      "env": { "DATABASE_URL": "postgres://ro:***@prod-host/app" }
    },
    "sqlite-local": {
      "command": "/path/to/db-mcp",
      "env": { "DATABASE_URL": "sqlite:///home/me/notes.db" }
    }
  }
}
```

## Tools

Tools are shared across all engines. All except `list_databases` accept `database?: string` — required when more than one database is configured.

| Tool | Parameters | Description |
|------|------------|-------------|
| `list_databases` | — | Configured databases: `name`, `engine`, `description`. |
| `query` | `sql: string` | Runs a SELECT and returns a JSON array of rows. Non-SELECT queries are rejected. |
| `explain` | `sql: string`, `analyze?: bool` | Execution plan of a SELECT. `analyze=true` → `EXPLAIN ANALYZE` (actually runs the query): PG and MySQL 8.0.18+; ClickHouse returns an error, SQLite ignores it (always `EXPLAIN QUERY PLAN`). |
| `list_tables` | — | List of user tables. |
| `describe_table` | `table: string`, `schema?: string` | Columns, types, nullability. Default `schema`: PG → `public`, MySQL → current database, SQLite → unused. |

### Type mapping

#### PostgreSQL

`query` uses the PostgreSQL text protocol (`simple_query`), so any type is supported. Post-processing:

- `bool` → JSON `true` / `false`
- `int2` / `int4` / `int8` / `oid` → JSON number
- `float4` / `float8` → JSON number
- `json` / `jsonb` → parsed JSON
- everything else (`uuid`, `numeric`, `date`, `time`, `timestamp[tz]`, `interval`, `inet`, `cidr`, `macaddr`, `bytea`, arrays, `range`, composite types, `enum`, geometric, `tsvector`, `hstore`, …) — string in PostgreSQL's canonical representation.
- `NULL` → JSON `null`.

#### MySQL

- `TINYINT`/`SMALLINT`/`MEDIUMINT`/`INT`/`BIGINT` → JSON number (signed/unsigned per type)
- `FLOAT`/`DOUBLE` → JSON number (NaN/Inf → `null`)
- `DECIMAL`/`NUMERIC` → string (precision preserved)
- `JSON` → parsed JSON
- `CHAR`/`VARCHAR`/`TEXT`/`ENUM`/`SET` → string
- `BINARY`/`VARBINARY`/`BLOB` → `\x<hex>` string if not valid UTF-8
- `DATE` → `YYYY-MM-DD`
- `DATETIME`/`TIMESTAMP` → `YYYY-MM-DD HH:MM:SS[.ffffff]`
- `TIME` → `[-]HH:MM:SS[.ffffff]`
- `NULL` → JSON `null`.

#### SQLite

- `INTEGER` → JSON number
- `REAL` → JSON number (NaN/Inf → `null`)
- `TEXT` → JSON string
- `BLOB` → `\x<hex>` string
- `NULL` → JSON `null`.

`describe_table` for SQLite uses `PRAGMA table_info`; the `schema` parameter is ignored (SQLite handles databases via `ATTACH`, not schemas).

#### ClickHouse

Two transports:

| Scheme | Transport | Default port |
|--------|-----------|--------------|
| `clickhouse://`, `clickhouse+native://`, `ch://` | native | 9000 |
| `clickhouses://`, `clickhouse+natives://`, `chs://` | native + TLS | 9440 |
| `clickhouse+http://` | HTTP | 8123 |
| `clickhouse+https://` | HTTPS | 8443 |

The native backend (`klickhouse`) is built under the `clickhouse-native` feature (enabled by default); when built without it, the `clickhouse://` / `ch://` / `chs://` schemes fall back to the HTTP backend, as before. The HTTP backend uses the `JSONEachRow` format with `output_format_json_quote_64bit_integers=0`. Type mapping is native to ClickHouse:

- `Int*` / `UInt*` (including 64-bit) → JSON number
- `Float32` / `Float64` → JSON number (NaN/Inf → string)
- `Decimal*` → JSON number
- `String` / `FixedString` / `UUID` / `Enum*` → string
- `Date` / `DateTime[64]` → string in canonical CH format
- `Array(T)` → JSON array, `Map(K,V)` / `Tuple(...)` / `JSON` → JSON object/array
- `Nullable(T)` → value or `null`; in `describe_table` the column is marked `nullable=true` and the type is reported as `Nullable(T)`.

`describe_table` reads `system.columns`; `schema` is optional — without it the current database from `currentDatabase()` is used. `list_tables` excludes `system`, `information_schema`, `INFORMATION_SCHEMA`.

### Examples

```jsonc
// query
{ "sql": "SELECT id, email FROM users WHERE created_at > now() - interval '7 days' LIMIT 10" }

// describe_table (PG)
{ "table": "orders", "schema": "public" }

// describe_table (MySQL — schema optional, defaults to current database)
{ "table": "orders" }

// describe_table (SQLite)
{ "table": "orders" }
```

## Security recommendations

- For PostgreSQL — a dedicated role with `SELECT`-only privileges:
  ```sql
  CREATE ROLE mcp_ro LOGIN PASSWORD '***';
  GRANT CONNECT ON DATABASE mydb TO mcp_ro;
  GRANT USAGE ON SCHEMA public TO mcp_ro;
  GRANT SELECT ON ALL TABLES IN SCHEMA public TO mcp_ro;
  ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO mcp_ro;
  ```
- For SQLite — open a read-only copy of the file or use file permissions; server-side `SELECT` filtering protects against accidents, not deliberate bypass (CTEs with `INSERT ... RETURNING` are rejected, but don't rely on that alone).
- HTTP mode does not authenticate clients — listen on `127.0.0.1` or put an authorizing proxy in front of it.
- Don't commit `DATABASE_URL` with real credentials into client configs tracked by git.

## Integration tests

E2E tests spin up a database container via [`testcontainers`](https://crates.io/crates/testcontainers), apply a migration, launch `db-mcp` as a stdio service and call tools through an MCP client. Requires a running Docker (SQLite needs no container).

```bash
cargo test --test integration -- --ignored
# single engine:
cargo test --test integration postgres -- --ignored
```

Tests are marked `#[ignore]`, so a plain `cargo test` does not run them.

## Debugging

```bash
RUST_LOG=debug DATABASE_URL=postgres://... ./target/release/db-mcp
```

On the client side, check its logs (Claude Desktop: `~/Library/Logs/Claude/mcp*.log`).
