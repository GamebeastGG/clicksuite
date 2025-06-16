# Clicksuite - ClickHouse Migration Tool

[![Tests](https://github.com/gamebeastgg/clicksuite/actions/workflows/test.yml/badge.svg)](https://github.com/gamebeastgg/clicksuite/actions/workflows/test.yml)
[![GitHub Package Version](https://img.shields.io/github/package-json/v/gamebeastgg/clicksuite?label=package)](https://github.com/gamebeastgg/clicksuite/packages)
[![GitHub Release](https://img.shields.io/github/v/release/gamebeastgg/clicksuite)](https://github.com/gamebeastgg/clicksuite/releases)
[![codecov](https://codecov.io/gh/gamebeastgg/clicksuite/branch/main/graph/badge.svg)](https://codecov.io/gh/gamebeastgg/clicksuite)
[![License: ISC](https://img.shields.io/badge/License-ISC-blue.svg)](https://opensource.org/licenses/ISC)

A CLI tool for managing ClickHouse database migrations, inspired by [Houseplant](https://github.com/pnuckowski/houseplant).

Clicksuite helps you manage your ClickHouse schema changes in a structured and environment-aware manner.

## Features

*   Initialize migration tracking in your project.
*   Generate new migration files with environment-specific sections (development, test, production).
*   View the status of all migrations (pending, applied, inactive).
*   Apply pending migrations up to the latest, or to a specific version.
*   Roll back the last applied migration, or roll back to a specific version.
*   Completely reset the database by rolling back all migrations and clearing the tracking table.
*   Load an existing schema state by marking all local migrations as applied without running them.
*   Configuration via `.env` file.

## Prerequisites

*   Node.js (v16+ recommended)
*   npm or yarn
*   A running ClickHouse instance

## Installation

### From GitHub Packages

1. **Configure npm to use GitHub Packages for the `@gamebeastgg` scope:**
   ```bash
   npm config set @gamebeastgg:registry https://npm.pkg.github.com
   ```

2. **Authenticate with GitHub Packages:**
   ```bash
   npm login --scope=@gamebeastgg --registry=https://npm.pkg.github.com
   ```
   Use your GitHub username and a personal access token with `read:packages` permission.

3. **Install the package globally:**
   ```bash
   npm install -g @gamebeastgg/clicksuite
   ```

4. **Or install locally in your project:**
   ```bash
   npm install --save-dev @gamebeastgg/clicksuite
   ```

### For Development

1. **Clone the repository:**
   ```bash
   git clone https://github.com/gamebeastgg/clicksuite.git
   cd clicksuite
   ```

2. **Install dependencies:**
   ```bash
   npm install
   ```

3. **Build the TypeScript project:**
   ```bash
   npm run build
   ```

4. **Make the CLI globally available (for development):**
   ```bash
   npm link
   ```

### Alternative Installation Methods

- **Using npx** (no installation required):
  ```bash
  npx @gamebeastgg/clicksuite init
  ```

- **Direct execution** (after cloning and building):
  ```bash
  node dist/index.js init
  ```

## Configuration

Clicksuite is configured using environment variables, typically loaded from a `.env` file in the root of your project. Create a `.env` file and populate it with your ClickHouse connection details.

**Example `.env` file:**

```env
# ClickHouse Connection Details
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=8123
CLICKHOUSE_PROTOCOL=http # or https
CLICKHOUSE_USERNAME=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_DATABASE=default
CLICKHOUSE_CLUSTER= # Optional: specify your ClickHouse cluster name if applicable

# Clicksuite Settings
# Base directory for Clicksuite files. Actual migration .yml files go into a 'migrations' subdirectory.
# Defaults to '.' (project root) if not set.
CLICKSUITE_MIGRATIONS_DIR=./db_migrations

# Current working environment. Affects which SQL is run from migration files.
# Options: development, test, production. Defaults to 'development'.
CLICKSUITE_ENVIRONMENT=development
```

**Important Notes:**

*   The actual migration `.yml` files are stored in a subdirectory named `migrations` under the path specified by `CLICKSUITE_MIGRATIONS_DIR`.
    *   For example, if `CLICKSUITE_MIGRATIONS_DIR=./db_configs`, then migrations will be in `./db_configs/migrations/`.
    *   If `CLICKSUITE_MIGRATIONS_DIR` is not set, it defaults to the project root (`.`), so migrations will be in `./migrations/`.
*   The `__clicksuite_migrations` table will be created in the specified `CLICKHOUSE_DATABASE` to track migration statuses.

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
    *   Creates the `__clicksuite_migrations` table in your ClickHouse database.
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

Each migration file supports environment-specific SQL and settings. The `{table}` placeholder in `up` or `down` SQL will be replaced by the value of the `table` field from the YAML.

**Example `YYYYMMDDHHMMSS_create_widgets.yml`:**

```yaml
version: "20240115103000"
name: "create widgets table"
table: "widgets_table_prod" # Or widgets_table_dev etc.

development: &development_defaults
  up: |
    -- SQL for development up
    CREATE TABLE IF NOT EXISTS {table} (
      id UInt64,
      name String,
      dev_notes String DEFAULT 'dev only'
    ) ENGINE = MergeTree() ORDER BY id;
  down: |
    -- SQL for development down
    DROP TABLE IF EXISTS {table};
  settings:
    allow_experimental_object_type: 1 # Example setting

test:
  <<: *development_defaults
  # up: |
  #   -- Override SQL for test up if needed
  #   ALTER TABLE {table} ADD COLUMN test_flag UInt8 DEFAULT 1;
  # down: |
  #   -- Override SQL for test down if needed

production:
  # Typically, you might not want to inherit DROP TABLE for production down by default
  # Or provide a very specific, non-destructive down script.
  up: |
    -- SQL for production up
    CREATE TABLE IF NOT EXISTS {table} (
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

*   The `version` and `name` fields are primarily for display and tracking.
*   The `table` field is optional but useful for string replacement in your SQL if your `up`/`down` scripts for a migration primarily target a single table.
*   Each environment (`development`, `test`, `production`) can define its own `up` SQL, `down` SQL, and `settings` (ClickHouse settings to apply during execution).
*   YAML anchors (`&anchor_name`) and aliases (`<<: *anchor_name`) can be used to reduce redundancy (e.g., `test` and `production` can inherit from `development_defaults`). `js-yaml` (used internally) resolves these aliases upon loading.

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
- **GitHub Packages Publishing**: Automated publishing to GitHub Packages on releases
- **Type Checking**: Validates TypeScript types
- **Security Scanning**: Daily vulnerability scans and dependency reviews

### Contributing Guidelines

When contributing:

1. Write tests for new functionality
2. Ensure all tests pass: `npm test`
3. Maintain or improve test coverage
4. Follow existing code patterns and conventions
5. Update documentation as needed

## Contributing

Contributions are welcome! Please feel free to open an issue or submit a pull request on the [GitHub repository](https://github.com/gamebeastgg/clicksuite).

## License

This project is licensed under the MIT License.