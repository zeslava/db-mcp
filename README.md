# db-mcp

MCP-сервер для SQL-баз с read-only доступом через stdio. Один бинарник со всеми движками — выбор по схеме URL.

Поддерживаемые движки:

| Схема URL | Движок | Драйвер |
|-----------|--------|---------|
| `postgres://` / `postgresql://` | PostgreSQL | `tokio-postgres` |
| `mysql://` | MySQL / MariaDB | `mysql_async` |
| `sqlite://` / `sqlite:` | SQLite | `rusqlite` (bundled) |
| `clickhouse://` / `clickhouse+https://` | ClickHouse | HTTP (`reqwest`) |

## Установка

### Быстрая установка (Linux и macOS Apple Silicon)

```bash
curl --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/zeslava/db-mcp/main/install.sh | sh
```

Скрипт автоматически определяет ОС и архитектуру, скачивает последний релиз, проверяет контрольную сумму и по умолчанию устанавливает бинарник в `~/.local/bin` (без `sudo`). Переопределить путь установки можно переменной `INSTALL_DIR`:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/zeslava/db-mcp/main/install.sh \
  | INSTALL_DIR=/usr/local/bin sh
```

### Из релизов вручную

Готовые бинарники для Linux (x86_64, aarch64), macOS (arm64) и Windows (x86_64) публикуются на [странице Releases](https://github.com/zeslava/db-mcp/releases).

```bash
TARGET=x86_64-unknown-linux-gnu
curl -sSL "https://github.com/zeslava/db-mcp/releases/latest/download/db-mcp-${TARGET}.tar.gz" \
  | tar -xz
install -m 755 "db-mcp-${TARGET}/db-mcp" "$HOME/.local/bin/db-mcp"
```

Проверка контрольной суммы:

```bash
curl -sSL -O "https://github.com/zeslava/db-mcp/releases/latest/download/db-mcp-${TARGET}.tar.gz.sha256"
shasum -a 256 -c "db-mcp-${TARGET}.tar.gz.sha256"
```

Старые релизы остаются на странице Releases как история, но ассеты у них удаляются — используйте `latest/download/...`.

### Из исходников

```bash
cargo build --release
# бинарник: ./target/release/db-mcp
```

## Запуск

URL передаётся флагом `--database-url` или переменной `DATABASE_URL`. Схема URL определяет, какой адаптер инициализируется.

```bash
./target/release/db-mcp --database-url postgres://user:pass@localhost:5432/mydb
./target/release/db-mcp --database-url mysql://user:pass@localhost:3306/mydb
./target/release/db-mcp --database-url sqlite:///absolute/path/to/data.db
./target/release/db-mcp --database-url clickhouse://default:pass@localhost:8123/default
./target/release/db-mcp --database-url clickhouse+https://user:pass@host.cloud:8443/default
DATABASE_URL=sqlite::memory: ./target/release/db-mcp
```

Логи — в stderr, JSON-RPC — в stdout. Уровень логов через `RUST_LOG` (например, `RUST_LOG=debug`).

## Подключение к клиентам

### Claude Code (CLI)

```bash
claude mcp add db \
  --env DATABASE_URL=postgres://user:pass@localhost:5432/mydb \
  -- /absolute/path/to/target/release/db-mcp
```

Или вручную в `~/.claude.json` (секция `mcpServers`):

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

### Claude Desktop

`~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) или
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

### Несколько баз

Регистрируется отдельный сервер на каждую БД:

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

Тулзы общие для всех движков:

| Tool | Параметры | Описание |
|------|-----------|----------|
| `query` | `sql: string` | Выполняет SELECT, возвращает JSON-массив строк. Не-SELECT запросы отклоняются. |
| `list_tables` | — | Список пользовательских таблиц. |
| `describe_table` | `table: string`, `schema?: string` | Колонки, типы, nullability. Дефолт `schema`: PG → `public`, MySQL → текущая БД, SQLite → не используется. |

### Преобразование типов

#### PostgreSQL

`query` использует текстовый протокол PostgreSQL (`simple_query`), поэтому поддерживается любой тип. Пост-обработка:

- `bool` → JSON `true` / `false`
- `int2` / `int4` / `int8` / `oid` → JSON number
- `float4` / `float8` → JSON number
- `json` / `jsonb` → распарсенный JSON
- остальное (`uuid`, `numeric`, `date`, `time`, `timestamp[tz]`, `interval`, `inet`, `cidr`, `macaddr`, `bytea`, массивы, `range`, композитные типы, `enum`, геометрические, `tsvector`, `hstore`, …) — строка в каноническом представлении PostgreSQL.
- `NULL` → JSON `null`.

#### MySQL

- `TINYINT`/`SMALLINT`/`MEDIUMINT`/`INT`/`BIGINT` → JSON number (signed/unsigned по типу)
- `FLOAT`/`DOUBLE` → JSON number (NaN/Inf → `null`)
- `DECIMAL`/`NUMERIC` → строка (точность сохраняется)
- `JSON` → распарсенный JSON
- `CHAR`/`VARCHAR`/`TEXT`/`ENUM`/`SET` → строка
- `BINARY`/`VARBINARY`/`BLOB` → строка `\x<hex>` если не валидный UTF-8
- `DATE` → `YYYY-MM-DD`
- `DATETIME`/`TIMESTAMP` → `YYYY-MM-DD HH:MM:SS[.ffffff]`
- `TIME` → `[-]HH:MM:SS[.ffffff]`
- `NULL` → JSON `null`.

#### SQLite

- `INTEGER` → JSON number
- `REAL` → JSON number (NaN/Inf → `null`)
- `TEXT` → JSON string
- `BLOB` → строка `\x<hex>`
- `NULL` → JSON `null`.

`describe_table` для SQLite использует `PRAGMA table_info`; параметр `schema` игнорируется (SQLite оперирует базами через `ATTACH`, а не схемами).

#### ClickHouse

Используется HTTP-интерфейс, формат `JSONEachRow` с `output_format_json_quote_64bit_integers=0`. Дефолтный порт `8123` (HTTP) и `8443` (HTTPS, схема `clickhouse+https://`). Тип-маппинг — родной для ClickHouse:

- `Int*` / `UInt*` (включая 64-битные) → JSON number
- `Float32` / `Float64` → JSON number (NaN/Inf → строка)
- `Decimal*` → JSON number
- `String` / `FixedString` / `UUID` / `Enum*` → строка
- `Date` / `DateTime[64]` → строка в каноническом формате CH
- `Array(T)` → JSON array, `Map(K,V)` / `Tuple(...)` / `JSON` → JSON object/array
- `Nullable(T)` → значение либо `null`; в `describe_table` колонка помечается `nullable=true`, тип отдаётся как `Nullable(T)`.

`describe_table` читает `system.columns`; `schema` опционален — без него используется текущая БД из `currentDatabase()`. `list_tables` исключает `system`, `information_schema`, `INFORMATION_SCHEMA`.

### Примеры

```jsonc
// query
{ "sql": "SELECT id, email FROM users WHERE created_at > now() - interval '7 days' LIMIT 10" }

// describe_table (PG)
{ "table": "orders", "schema": "public" }

// describe_table (MySQL — schema опционален, по умолчанию текущая БД)
{ "table": "orders" }

// describe_table (SQLite)
{ "table": "orders" }
```

## Рекомендации по безопасности

- Для PostgreSQL — отдельная роль с правами только `SELECT`:
  ```sql
  CREATE ROLE mcp_ro LOGIN PASSWORD '***';
  GRANT CONNECT ON DATABASE mydb TO mcp_ro;
  GRANT USAGE ON SCHEMA public TO mcp_ro;
  GRANT SELECT ON ALL TABLES IN SCHEMA public TO mcp_ro;
  ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO mcp_ro;
  ```
- Для SQLite — открывайте read-only копию файла или используйте файловые права; серверная фильтрация по `SELECT` — защита от случайностей, не от обхода (CTE с `INSERT ... RETURNING` отклоняются, но полагаться только на это не стоит).
- Не коммитьте `DATABASE_URL` с реальными креденшелами в конфиги клиентов, попадающие в git.

## Интеграционные тесты

E2E-тесты поднимают контейнер с БД через [`testcontainers`](https://crates.io/crates/testcontainers), накатывают миграцию, запускают `db-mcp` как stdio-сервис и дёргают tools через MCP-клиента. Требуется работающий Docker (для SQLite контейнер не нужен).

```bash
cargo test --test integration -- --ignored
# отдельный движок:
cargo test --test integration postgres -- --ignored
```

Тесты помечены `#[ignore]`, поэтому обычный `cargo test` их не запускает.

## Отладка

```bash
RUST_LOG=debug DATABASE_URL=postgres://... ./target/release/db-mcp
```

Из клиента смотрите его логи (Claude Desktop: `~/Library/Logs/Claude/mcp*.log`).
