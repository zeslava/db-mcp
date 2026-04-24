# db-mcp

MCP server для PostgreSQL. Предоставляет read-only доступ к БД через stdio-транспорт.

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

Если целевой каталог требует повышенных прав, скрипт запросит `sudo`. В конце предупредит, если каталог не в `PATH`.

### Из релизов вручную

Готовые бинарники для Linux (x86_64, aarch64), macOS (arm64) и Windows (x86_64) публикуются на [странице Releases](https://github.com/zeslava/db-mcp/releases).

```bash
# Linux x86_64 — пример для последнего релиза
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
cargo build --release
# бинарник: ./target/release/db-mcp
```

## Запуск

Сервер принимает URL БД через флаг `--database-url` или переменную окружения `DATABASE_URL`.

```bash
./target/release/db-mcp --database-url postgres://user:pass@localhost:5432/mydb
# или
DATABASE_URL=postgres://user:pass@localhost:5432/mydb ./target/release/db-mcp
```

Логи пишутся в stderr, JSON-RPC — в stdout. Уровень логов — через `RUST_LOG` (например, `RUST_LOG=debug`).

## Подключение к клиентам

### Claude Code (CLI)

```bash
claude mcp add postgres \
  --env DATABASE_URL=postgres://user:pass@localhost:5432/mydb \
  -- /absolute/path/to/target/release/db-mcp
```

Или вручную в `~/.claude.json` (секция `mcpServers`):

```json
{
  "mcpServers": {
    "postgres": {
      "command": "/absolute/path/to/target/release/db-mcp",
      "args": [],
      "env": {
        "DATABASE_URL": "postgres://user:pass@localhost:5432/mydb"
      }
    }
  }
}
```

Проверка: `claude mcp list`.

### Claude Desktop

`~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) или
`%APPDATA%\Claude\claude_desktop_config.json` (Windows):

```json
{
  "mcpServers": {
    "postgres": {
      "command": "/absolute/path/to/target/release/db-mcp",
      "env": {
        "DATABASE_URL": "postgres://user:pass@localhost:5432/mydb"
      }
    }
  }
}
```

Перезапустить Claude Desktop.

### Cursor / Windsurf / Zed

Формат аналогичный — `command` + `env`. В настройках MCP соответствующего редактора добавить:

```json
{
  "db-mcp": {
    "command": "/absolute/path/to/target/release/db-mcp",
    "env": { "DATABASE_URL": "postgres://user:pass@localhost:5432/mydb" }
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
    "pg-staging": {
      "command": "/path/to/db-mcp",
      "env": { "DATABASE_URL": "postgres://ro:***@staging-host/app" }
    }
  }
}
```

## Tools

| Tool | Параметры | Описание |
|------|-----------|----------|
| `query` | `sql: string` | Выполняет SELECT, возвращает JSON-массив строк. Не-SELECT запросы отклоняются. |
| `list_tables` | — | Список пользовательских таблиц (исключая `pg_catalog`, `information_schema`). |
| `describe_table` | `table: string`, `schema?: string` (default `public`) | Колонки, типы, nullability. |

### Преобразование типов

`query` использует текстовый протокол PostgreSQL (`simple_query`), поэтому поддерживается любой тип данных. Для удобства клиента применяется пост-обработка:

- `bool` → JSON `true` / `false`
- `int2` / `int4` / `int8` / `oid` → JSON number
- `float4` / `float8` → JSON number
- `json` / `jsonb` → распарсенный JSON
- всё остальное (`uuid`, `numeric`, `date`, `time`, `timestamp`, `timestamptz`, `interval`, `inet`, `cidr`, `macaddr`, `bytea`, массивы, `range`, композитные типы, `enum`, геометрические, `tsvector`, `hstore`, …) — строка в каноническом представлении PostgreSQL.
- `NULL` → JSON `null`.

### Примеры вызовов

```jsonc
// query
{ "sql": "SELECT id, email FROM users WHERE created_at > now() - interval '7 days' LIMIT 10" }

// describe_table
{ "table": "orders", "schema": "public" }
```

## Рекомендации по безопасности

- Используйте отдельного пользователя PostgreSQL с правами только `SELECT`:
  ```sql
  CREATE ROLE mcp_ro LOGIN PASSWORD '***';
  GRANT CONNECT ON DATABASE mydb TO mcp_ro;
  GRANT USAGE ON SCHEMA public TO mcp_ro;
  GRANT SELECT ON ALL TABLES IN SCHEMA public TO mcp_ro;
  ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO mcp_ro;
  ```
- Не коммитьте `DATABASE_URL` с реальными креденшелами в конфиги клиентов, которые попадают в git.
- Фильтр `SELECT` на стороне сервера — защита от случайностей, не от намеренного обхода (например, CTE с `INSERT ... RETURNING` будет отклонён, т.к. начинается не с `SELECT`, но полагаться только на это не стоит — используйте read-only роль).

## Отладка

```bash
# проверить, что сервер стартует и отвечает на initialize
RUST_LOG=debug DATABASE_URL=postgres://... ./target/release/db-mcp
```

Из клиента смотреть логи (Claude Desktop: `~/Library/Logs/Claude/mcp*.log`).
