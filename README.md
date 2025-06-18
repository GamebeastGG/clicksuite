# Clicksuite - ClickHouse Migration Tool

[![Tests](https://github.com/GamebeastGG/clicksuite/actions/workflows/test.yml/badge.svg)](https://github.com/GamebeastGG/clicksuite/actions/workflows/test.yml)
[![NPM Version](https://img.shields.io/npm/v/clicksuite.svg)](https://www.npmjs.com/package/clicksuite)
[![NPM Downloads](https://img.shields.io/npm/dm/clicksuite.svg)](https://www.npmjs.com/package/clicksuite)
[![codecov](https://codecov.io/gh/GamebeastGG/clicksuite/branch/main/graph/badge.svg)](https://codecov.io/gh/GamebeastGG/clicksuite)
[![License: ISC](https://img.shields.io/badge/License-ISC-blue.svg)](https://opensource.org/licenses/ISC)

A robust CLI tool for managing ClickHouse database migrations with environment-specific configurations and TypeScript support.

## Key Features

*   **Environment-aware migrations** - separate SQL for development, test, and production
*   **Multiple query support** - execute multiple SQL statements in a single migration
*   **Environment variable interpolation** - secure credential management with `${ENV_VAR}` syntax
*   **Auto-generated schema.sql** - complete database schema tracking
*   **Full TypeScript support** - exported types for programmatic usage
*   **Comprehensive migration management** - apply, rollback, reset, and status tracking

## Prerequisites

*   Node.js (v16+ recommended)
*   npm or yarn
*   A running ClickHouse instance

## Quick Start

### Installation

**Global installation (recommended for CLI usage):**
```bash
npm install -g clicksuite
```

**Or use without installing:**
```bash
npx clicksuite init
```

**For projects (with programmatic usage):**
```bash
npm install --save-dev clicksuite
```

### Initial Setup

1. **Initialize in your project:**
   ```bash
   clicksuite init
   ```

2. **Configure your environment (`.env` file):**
   ```env
   CLICKHOUSE_URL=http://default@localhost:8123/my_database
   ```

3. **Generate your first migration:**
   ```bash
   clicksuite generate create_users_table
   ```

4. **Apply migrations:**
   ```bash
   clicksuite migrate
   ```

## Configuration

Configure Clicksuite using environment variables in a `.env` file:

```env
# Required: ClickHouse connection URL
CLICKHOUSE_URL=http://default@localhost:8123/my_database

# Optional: For clustered deployments  
CLICKHOUSE_CLUSTER='{cluster}'

# Optional: Custom migrations directory (defaults to './migrations')
CLICKSUITE_MIGRATIONS_DIR=./db_migrations

# Optional: Environment (defaults to 'development')
CLICKSUITE_ENVIRONMENT=production
```

**Connection URL Examples:**
- Local: `http://default@localhost:8123/my_database`
- Remote: `https://user:pass@clickhouse.example.com:8443/prod_db`
- Docker: `http://default@clickhouse:8123/analytics`

## Usage

Once installed and configured, you can use the `clicksuite` CLI:

```bash
clicksuite <command> [options]
```

### Global Options

*   `--non-interactive`, `-y`: Run in non-interactive mode, automatically confirming prompts (e.g., for `migrate:reset`). Useful for CI environments.

### Commands

*   **`clicksuite init`**
    *   Initializes Clicksuite for the current project.
    *   Creates the migrations directory (e.g., `<CLICKSUITE_MIGRATIONS_DIR>/migrations/`).
    *   Creates the `__clicksuite_migrations` table in the `default` database to track migrations.
    *   Tests the connection to ClickHouse.

*   **`clicksuite generate <migration_name>`**
    *   Generates a new migration YAML file in the migrations directory.
    *   Example: `clicksuite generate create_users_table`
    *   The generated file will have a timestamped version and sections for `development`, `test`, and `production` environments.

*   **`clicksuite migrate:status`**
    *   Shows the status of all migrations (APPLIED, PENDING, INACTIVE) for the current `CLICKSUITE_ENVIRONMENT`.

*   **`clicksuite migrate`**
    *   Runs all pending migrations for the current environment. Equivalent to `clicksuite migrate:up`.

*   **`clicksuite migrate:up [migrationVersion]`**
    *   Applies migrations for the current environment.
    *   If `migrationVersion` is omitted, applies all pending migrations.
    *   If `migrationVersion` is specified, applies all pending migrations up to and including that version.
    *   Example: `clicksuite migrate:up 20230101120000`

*   **`clicksuite migrate:down [migrationVersion]`**
    *   Rolls back migrations for the current environment.
    *   If `migrationVersion` is omitted, rolls back the single last applied migration.
    *   If `migrationVersion` is specified, rolls back all migrations applied *after* that version, making the specified version the new latest applied migration. Prompts for confirmation if multiple migrations will be rolled back.
    *   Example (roll back last): `clicksuite migrate:down`
    *   Example (roll back to version): `clicksuite migrate:down 20230101120000`

*   **`clicksuite migrate:reset`**
    *   Rolls back **all** applied migrations for the current environment by executing their `downSQL`.
    *   Clears the `__clicksuite_migrations` table.
    *   Requires confirmation unless `--non-interactive` is used.
    *   **Caution**: This is a destructive operation for your migration history tracking and potentially your data if `downSQL` scripts are destructive.

*   **`clicksuite schema:load`**
    *   Marks all local migration files as APPLIED in the `__clicksuite_migrations` table **without** running their `upSQL`.
    *   Useful for initializing Clicksuite on an existing database where the schema changes have already been applied manually or by another process.

## Migration File Structure

Migration files are YAML (`.yml`) and should be placed in the `<CLICKSUITE_MIGRATIONS_DIR>/migrations/` directory. The filename format is `YYYYMMDDHHMMSS_description.yml`.

Each migration file supports environment-specific SQL and settings. The `{table}` and `{database}` placeholders in `up` or `down` SQL will be replaced by the values of the `table` and `database` fields from the YAML.

**Example `YYYYMMDDHHMMSS_create_widgets.yml`:**

```yaml
version: "20240115103000"
name: "create widgets table"
table: "widgets_table"
database: "analytics_db"

development: &development_defaults
  up: |
    -- Create database if it doesn't exist
    CREATE DATABASE IF NOT EXISTS {database};
    
    -- Create table in the specified database
    CREATE TABLE IF NOT EXISTS {database}.{table} (
      id UInt64,
      name String,
      dev_notes String DEFAULT 'dev only'
    ) ENGINE = MergeTree() ORDER BY id;
  down: |
    -- SQL for development down
    DROP TABLE IF EXISTS {database}.{table};
  settings:
    allow_experimental_object_type: 1 # Example setting

test:
  <<: *development_defaults
  # up: |
  #   -- Override SQL for test up if needed
  #   ALTER TABLE {database}.{table} ADD COLUMN test_flag UInt8 DEFAULT 1;
  # down: |
  #   -- Override SQL for test down if needed

production:
  # Typically, you might not want to inherit DROP TABLE for production down by default
  # Or provide a very specific, non-destructive down script.
  up: |
    -- Create database if it doesn't exist
    CREATE DATABASE IF NOT EXISTS {database};
    
    -- SQL for production up
    CREATE TABLE IF NOT EXISTS {database}.{table} (
      id UInt64,
      name String,
      critical_prod_field String
    ) ENGINE = MergeTree() ORDER BY id;
  down: |
    -- SQL for production down (be cautious with DROP TABLE in prod down scripts)
    -- Consider ALTER TABLE to make a field nullable, or a no-op if rollback is too risky.
    SELECT 'Manual rollback required or specific ALTER TABLE statement for production';
  settings:
    # Production specific settings
    # send_timeout: 600
```

**Field Reference:**

*   The `version` and `name` fields are primarily for display and tracking.
*   The `table` field is optional but useful for string replacement in your SQL if your migration targets a specific table.
*   The `database` field is optional but allows you to target different databases. If specified, use `{database}.{table}` format in your SQL.
*   **Migration Tracking**: All migrations are tracked centrally in the `default.__clicksuite_migrations` table, regardless of which database they target.
*   **Database Creation**: You can create databases in your migrations using `CREATE DATABASE IF NOT EXISTS {database}` - this will be tracked in the generated `schema.sql`.
*   Each environment (`development`, `test`, `production`) can define its own `up` SQL, `down` SQL, and `settings` (ClickHouse settings to apply during execution).
*   YAML anchors (`&anchor_name`) and aliases (`<<: *anchor_name`) can be used to reduce redundancy (e.g., `test` and `production` can inherit from `development_defaults`). `js-yaml` (used internally) resolves these aliases upon loading.

### Multiple Query Support

Clicksuite supports executing multiple SQL statements in a single migration by separating them with semicolons. This is particularly useful since ClickHouse doesn't natively support multiple queries in a single request.

**Example migration with multiple queries:**

```yaml
version: "20240115103000"
name: "create users and orders tables"
table: "users"
database: "ecommerce"

development:
  up: |
    -- Create the database
    CREATE DATABASE IF NOT EXISTS {database};
    
    -- Create users table
    CREATE TABLE {database}.users (
      id UInt32,
      email String,
      created_at DateTime64
    ) ENGINE = MergeTree() ORDER BY id;
    
    -- Create orders table
    CREATE TABLE {database}.orders (
      id UInt32,
      user_id UInt32,
      amount Decimal(10,2),
      created_at DateTime64
    ) ENGINE = MergeTree() ORDER BY id;
    
    -- Insert initial admin user
    INSERT INTO {database}.users VALUES (1, 'admin@example.com', now64());
  down: |
    -- Drop tables in reverse order
    DROP TABLE IF EXISTS {database}.orders;
    DROP TABLE IF EXISTS {database}.users;
    DROP DATABASE IF EXISTS {database};
```

**Key features of multiple query support:**

- **Automatic splitting**: Queries are automatically split by semicolons and executed individually
- **Error handling**: If any query fails, the entire migration fails and subsequent queries are not executed
- **Logging**: Each query is logged separately with progress indicators (e.g., "Query 1/3", "Query 2/3")
- **Settings preservation**: All ClickHouse settings specified in the migration are applied to each individual query
- **Trailing semicolons**: Trailing semicolons are safely ignored and won't cause empty query errors
- **Works for both directions**: Multiple queries work for both `up` and `down` migrations

**Console output example:**
```
Executing 4 migration queries:
  Query 1/4: CREATE DATABASE IF NOT EXISTS ecommerce
  Query 2/4: CREATE TABLE ecommerce.users (id UInt32, email String, created_at DateTime64) ENGINE = MergeTree() ORDER BY id
  Query 3/4: CREATE TABLE ecommerce.orders (id UInt32, user_id UInt32, amount Decimal(10,2), created_at DateTime64) ENGINE = MergeTree() ORDER BY id
  Query 4/4: INSERT INTO ecommerce.users VALUES (1, 'admin@example.com', now64())
```

### Environment Variable Interpolation

Clicksuite supports interpolating environment variables into your SQL migrations using the `${ENV_VAR_NAME}` syntax. This is particularly useful for sensitive data like database credentials that shouldn't be hardcoded in migration files.

**Example migration with environment variable interpolation:**

```yaml
version: "20240115104000"
name: "create organization dictionary"
table: "organization_info"
database: "gamebeast"

development:
  up: |
    CREATE DICTIONARY IF NOT EXISTS {database}.{table} (
      id UInt64,
      name String,
      created_at DateTime,
      created_by String
    ) PRIMARY KEY id
    LIFETIME(MIN 600 MAX 900)
    SOURCE(POSTGRESQL(
        port ${POSTGRES_PORT}
        host '${POSTGRES_HOST}'
        user '${POSTGRES_USER}'
        password '${POSTGRES_PASSWORD}'
        db '${POSTGRES_DATABASE}'
        table 'organizations'
    ))
    LAYOUT(HASHED());
  down: |
    DROP DICTIONARY IF EXISTS {database}.{table};
```

**Required environment variables for the above example:**
```bash
export POSTGRES_HOST=host.docker.internal
export POSTGRES_PORT=5432
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=postgres
export POSTGRES_DATABASE=gamebeast
```

**Key features of environment variable interpolation:**

- **Secure credentials**: Keep sensitive data like passwords out of your migration files
- **Environment-specific values**: Use different database hosts, ports, or credentials per environment
- **Automatic interpolation**: Variables are replaced during migration execution
- **Warning for missing variables**: If an environment variable is not set, a warning is displayed and an empty string is used
- **Works with existing placeholders**: Environment variables work alongside `{table}` and `{database}` placeholders
- **Multiple variables**: You can use as many environment variables as needed in a single migration

**Supported syntax:**
- `${VARIABLE_NAME}` - Standard environment variable interpolation
- `${DB_HOST}` - Simple variable names
- `${DB_CONNECTION_USER_NAME}` - Complex variable names with underscores

**Best practices:**
- Use environment variables for sensitive data (passwords, API keys, hosts)
- Document required environment variables in your project's README
- Use descriptive variable names (e.g., `POSTGRES_HOST` instead of `HOST`)
- Set default values in your deployment scripts when appropriate

## Testing

Clicksuite includes a comprehensive test suite built with Jest that covers all core functionality.

### Running Tests

```bash
# Run all tests
npm test

# Run tests in watch mode during development
npm run test:watch

# Run tests with coverage report
npm run test:coverage

# Run tests in CI mode (used by GitHub Actions)
npm run test:ci
```

### Test Coverage

The test suite includes:

- **Unit Tests**: Core functionality, database operations, CLI commands
- **Integration Tests**: End-to-end migration scenarios, file system operations
- **Type Tests**: TypeScript interface validation
- **Error Handling Tests**: Database errors, file system errors, validation failures

Current test coverage:
- 121 test cases across 6 test suites
- 66.87% overall statement coverage
- 86.59% coverage for core migration runner logic

### Test Files

- `tests/db.test.ts` - Database operations and ClickHouse client
- `tests/runner.test.ts` - Migration runner and command execution
- `tests/index.test.ts` - CLI interface and argument parsing
- `tests/types.test.ts` - TypeScript type definitions
- `tests/integration.test.ts` - End-to-end integration scenarios
- `tests/utils.test.ts` - Utility functions and helpers

## Development

1.  **Install dependencies:**
    ```bash
    npm install
    ```

2.  **Build (and watch for changes):**
    ```bash
    npm run build
    # or for continuous compilation during development:
    # tsc -w
    ```

3.  **Run tests:**
    ```bash
    npm test
    # or run with coverage
    npm run test:coverage
    ```

4.  **Local Execution:**
    After building, you can run the CLI using `npm link` (as described in Installation) or by directly executing the compiled JavaScript:
    ```bash
    node dist/index.js <command>
    ```

### GitHub Actions

This project uses GitHub Actions for:

- **Continuous Testing**: Runs tests on Node.js 18.x, 20.x, and 22.x
- **Code Coverage**: Uploads coverage reports to Codecov
- **NPM Publishing**: Automated publishing to NPM on releases
- **Type Checking**: Validates TypeScript types
- **Security Scanning**: Daily vulnerability scans and dependency reviews

### Contributing Guidelines

When contributing:

1. Write tests for new functionality
2. Ensure all tests pass: `npm test`
3. Maintain or improve test coverage
4. Follow existing code patterns and conventions
5. Update documentation as needed

## Programmatic Usage

Clicksuite can also be used programmatically in your Node.js/TypeScript applications:

```typescript
import { Runner, Db, Context } from 'clicksuite';

// Create a context for your ClickHouse configuration
const context: Context = {
  url: 'http://default@localhost:8123/my_database',
  database: 'my_database', // Extracted from URL
  migrationsDir: '/path/to/migrations',
  environment: 'development',
  nonInteractive: false
};

// Or use the getContext helper function with environment variables
import { getContext } from 'clicksuite';

// Set CLICKHOUSE_URL environment variable first
process.env.CLICKHOUSE_URL = 'http://default@localhost:8123/my_database';
const context = getContext({});

// Run migrations programmatically
const runner = new Runner(context);
await runner.init();
await runner.migrate();

// Direct database access
const db = new Db(context);
const tables = await db.getDatabaseTables();
await db.close();
```

### Available Types

All TypeScript types are exported for easy integration:

- `Context` - Configuration interface
- `MigrationFile` - Represents a migration file
- `MigrationRecord` - Database migration record
- `MigrationStatus` - Migration status information
- `MigrationState` - Migration state enum
- `RawMigrationFileContent` - Raw YAML migration content

See the [examples directory](./examples/) for more detailed usage examples.

## Contributing

Contributions are welcome! Please feel free to open an issue or submit a pull request on the [GitHub repository](https://github.com/GamebeastGG/clicksuite).

## License

This project is licensed under the MIT License.