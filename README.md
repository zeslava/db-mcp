# mcp-postgres

MCP server для PostgreSQL. Предоставляет read-only доступ к БД через stdio-транспорт.

## Установка

### Быстрая установка (Linux и macOS Apple Silicon)

```bash
curl --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/zeslava/mcp-postgres/main/install.sh | sh
```

Скрипт автоматически определяет ОС и архитектуру, скачивает последний релиз, проверяет контрольную сумму и устанавливает бинарник в `/usr/local/bin` (или `~/.local/bin`, если `/usr/local/bin` недоступен без `sudo`).

### Из релизов вручную

Готовые бинарники для Linux (x86_64, aarch64), macOS (arm64) и Windows (x86_64) публикуются на [странице Releases](https://github.com/zeslava/mcp-postgres/releases).

```bash
# Linux x86_64 — пример для последнего релиза
VERSION=v0.1.0
TARGET=x86_64-unknown-linux-gnu
curl -sSL "https://github.com/zeslava/mcp-postgres/releases/download/${VERSION}/mcp-postgres-${VERSION}-${TARGET}.tar.gz" \
  | tar -xz
sudo mv "mcp-postgres-${VERSION}-${TARGET}/mcp-postgres" /usr/local/bin/
```

Проверка контрольной суммы:

```bash
curl -sSL -O "https://github.com/zeslava/mcp-postgres/releases/download/${VERSION}/mcp-postgres-${VERSION}-${TARGET}.tar.gz.sha256"
shasum -a 256 -c "mcp-postgres-${VERSION}-${TARGET}.tar.gz.sha256"
```

### Из исходников

```bash
cargo build --release
# бинарник: ./target/release/mcp-postgres
```

## Запуск

Сервер принимает URL БД через флаг `--database-url` или переменную окружения `DATABASE_URL`.

```bash
./target/release/mcp-postgres --database-url postgres://user:pass@localhost:5432/mydb
# или
DATABASE_URL=postgres://user:pass@localhost:5432/mydb ./target/release/mcp-postgres
```

Логи пишутся в stderr, JSON-RPC — в stdout. Уровень логов — через `RUST_LOG` (например, `RUST_LOG=debug`).

## Подключение к клиентам

### Claude Code (CLI)

```bash
claude mcp add postgres \
  --env DATABASE_URL=postgres://user:pass@localhost:5432/mydb \
  -- /absolute/path/to/target/release/mcp-postgres
```

Или вручную в `~/.claude.json` (секция `mcpServers`):

```json
{
  "mcpServers": {
    "postgres": {
      "command": "/absolute/path/to/target/release/mcp-postgres",
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
      "command": "/absolute/path/to/target/release/mcp-postgres",
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
  "mcp-postgres": {
    "command": "/absolute/path/to/target/release/mcp-postgres",
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
      "command": "/path/to/mcp-postgres",
      "env": { "DATABASE_URL": "postgres://ro:***@prod-host/app" }
    },
    "pg-staging": {
      "command": "/path/to/mcp-postgres",
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
RUST_LOG=debug DATABASE_URL=postgres://... ./target/release/mcp-postgres
```

Из клиента смотреть логи (Claude Desktop: `~/Library/Logs/Claude/mcp*.log`).
