# db-mcp

MCP-сервер для SQL-баз с read-only доступом через stdio. Один бинарник, движок выбирается по схеме URL.

Поддерживаемые движки:

| Схема URL | Движок | Cargo feature |
|-----------|--------|---------------|
| `postgres://` / `postgresql://` | PostgreSQL (через `tokio-postgres`) | `postgres` |
| `sqlite://` / `sqlite:` | SQLite (через `rusqlite`) | `sqlite` |

По умолчанию собираются обе фичи. Чтобы получить минимальный бинарник под одну БД — `--no-default-features --features <engine>`.

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
VERSION=v0.1.3
TARGET=x86_64-unknown-linux-gnu
curl -sSL "https://github.com/zeslava/db-mcp/releases/download/${VERSION}/db-mcp-${VERSION}-${TARGET}.tar.gz" \
  | tar -xz
install -m 755 "db-mcp-${VERSION}-${TARGET}/db-mcp" "$HOME/.local/bin/db-mcp"
```

Проверка контрольной суммы:

```bash
curl -sSL -O "https://github.com/zeslava/db-mcp/releases/download/${VERSION}/db-mcp-${VERSION}-${TARGET}.tar.gz.sha256"
shasum -a 256 -c "db-mcp-${VERSION}-${TARGET}.tar.gz.sha256"
```

### Из исходников

```bash
cargo build --release                                       # все движки (default)
cargo build --release --no-default-features --features postgres
cargo build --release --no-default-features --features sqlite
# бинарник: ./target/release/db-mcp
```

## Запуск

URL передаётся флагом `--database-url` или переменной `DATABASE_URL`. Схема URL определяет, какой адаптер инициализируется.

```bash
./target/release/db-mcp --database-url postgres://user:pass@localhost:5432/mydb
./target/release/db-mcp --database-url sqlite:///absolute/path/to/data.db
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
| `describe_table` | `table: string`, `schema?: string` (default `public`) | Колонки, типы, nullability. |

### Преобразование типов

#### PostgreSQL

`query` использует текстовый протокол PostgreSQL (`simple_query`), поэтому поддерживается любой тип. Пост-обработка:

- `bool` → JSON `true` / `false`
- `int2` / `int4` / `int8` / `oid` → JSON number
- `float4` / `float8` → JSON number
- `json` / `jsonb` → распарсенный JSON
- остальное (`uuid`, `numeric`, `date`, `time`, `timestamp[tz]`, `interval`, `inet`, `cidr`, `macaddr`, `bytea`, массивы, `range`, композитные типы, `enum`, геометрические, `tsvector`, `hstore`, …) — строка в каноническом представлении PostgreSQL.
- `NULL` → JSON `null`.

#### SQLite

- `INTEGER` → JSON number
- `REAL` → JSON number (NaN/Inf → `null`)
- `TEXT` → JSON string
- `BLOB` → строка `\x<hex>`
- `NULL` → JSON `null`.

`describe_table` для SQLite использует `PRAGMA table_info`; параметр `schema` игнорируется (SQLite оперирует базами через `ATTACH`, а не схемами).

### Примеры

```jsonc
// query
{ "sql": "SELECT id, email FROM users WHERE created_at > now() - interval '7 days' LIMIT 10" }

// describe_table (PG)
{ "table": "orders", "schema": "public" }

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

## Отладка

```bash
RUST_LOG=debug DATABASE_URL=postgres://... ./target/release/db-mcp
```

Из клиента смотрите его логи (Claude Desktop: `~/Library/Logs/Claude/mcp*.log`).
