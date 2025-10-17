import { createClient, ClickHouseClient } from '@clickhouse/client';
import { Context } from './types';
import chalk from 'chalk';

export class Db {
  private client: ClickHouseClient;
  private context: Context;

  constructor(context: Context) {
    this.client = createClient({
      url: context.url,
    });

    this.context = context;
  }

  async ping() {
    return this.client.ping();
  }

  async initMigrationsTable() {
    const clusterClause = this.context.cluster ? `ON CLUSTER ${this.context.cluster}` : '';
    const tableEngine = this.context.cluster ? `ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/__clicksuite_migrations', '{replica}')` : 'ReplacingMergeTree()';
    const migrationsDatabase = this.context.migrationsDatabase || 'default';

    // 1) Ensure the migrations database exists (when not default)
    if (migrationsDatabase !== 'default') {
      const createDbQuery = `CREATE DATABASE IF NOT EXISTS ${migrationsDatabase} ${clusterClause}`;
      try {
        if (this.context.verbose) {
          console.log(chalk.gray('🔍 Ensuring migrations database exists:'), chalk.gray(createDbQuery.replace(/\n\s*/g, ' ').trim()));
        }
        await this.client.command({
          query: createDbQuery,
          clickhouse_settings: { wait_end_of_query: 1 },
        });
      } catch (error) {
        console.error(chalk.bold.red(`❌ Failed to create migrations database '${migrationsDatabase}':`), error);
        throw error;
      }
    }

    // 2) Ensure the migrations table exists
    try {
      const createTableQuery = `
          CREATE TABLE IF NOT EXISTS ${migrationsDatabase}.__clicksuite_migrations ${clusterClause} (
            version LowCardinality(String),
            active UInt8 NOT NULL DEFAULT 1,
            created_at DateTime64(6, 'UTC') NOT NULL DEFAULT now64()
          )
          ENGINE = ${tableEngine}
          PRIMARY KEY (version)
          ORDER BY (version)
        `;
      if (this.context.verbose) {
        console.log(chalk.gray('🔍 Executing initMigrationsTable query:'), chalk.gray(createTableQuery.replace(/\n\s*/g, ' ').trim()));
      }
      await this.client.command({
        query: createTableQuery,
        clickhouse_settings: {
          wait_end_of_query: 1,
        },
      });
      console.log(chalk.green(`✅ Successfully ensured __clicksuite_migrations table exists in ${migrationsDatabase} database.`));
    } catch (error) {
      console.error(chalk.bold.red('❌ Failed to create __clicksuite_migrations table:'), error);
      throw error;
    }
  }

  async getAppliedMigrations(): Promise<Array<{ version: string, active: number, created_at: string }>> {
    try {
      const migrationsDatabase = this.context.migrationsDatabase || 'default';
      const resultSet = await this.client.query({
        query: `SELECT version, active, created_at FROM ${migrationsDatabase}.__clicksuite_migrations WHERE active = 1 ORDER BY version ASC`,
      });
      const response = await resultSet.json();
      const migrations = response.data as Array<{ version: string, active: number, created_at: string }>;
      return migrations;
    } catch (error) {
      console.error(chalk.bold.red('❌ Failed to get applied migrations:'), error);
      return [];
    }
  }
  
  async getAllMigrationRecords(): Promise<Array<{ version: string, active: number, created_at: string }>> {
    try {
      const migrationsDatabase = this.context.migrationsDatabase || 'default';  
      const resultSet = await this.client.query({
        query: `SELECT version, active, created_at FROM ${migrationsDatabase}.__clicksuite_migrations ORDER BY version ASC`,
      });
      const response = await resultSet.json();
      const migrations = response.data as Array<{ version: string, active: number, created_at: string }>;
      return migrations;
    } catch (error) {
      console.error(chalk.bold.red('❌ Failed to get all migration records:'), error);
      return [];
    }
  }

  private splitQueries(sql: string): string[] {
    // Split by semicolon and filter out empty queries
    return sql
      .split(';')
      .map(query => query.trim())
      .filter(query => query.length > 0);
  }

  async executeMigration(query: string, query_settings?: Record<string, any>) {
    try {
      const queries = this.splitQueries(query);
      
      if (queries.length === 0) {
        console.warn(chalk.yellow('⚠️ No queries found to execute'));
        return;
      }

      if (queries.length === 1) {
        if (this.context.verbose) {
          console.log(chalk.gray('🔍 Executing migration query:'), chalk.gray(query.replace(/\n\s*/g, ' ').trim()));
        }
        await this.client.command({
          query: query,
          clickhouse_settings: {
            ...query_settings,
            wait_end_of_query: 1,
          },
        });
      } else {
        if (this.context.verbose) {
          console.log(chalk.gray(`🔍 Executing ${queries.length} migration queries:`));
          for (let i = 0; i < queries.length; i++) {
            const individualQuery = queries[i];
            console.log(chalk.gray(`🔍   Query ${i + 1}/${queries.length}:`), chalk.gray(individualQuery.replace(/\n\s*/g, ' ').trim()));
          }
        }
        for (let i = 0; i < queries.length; i++) {
          const individualQuery = queries[i];
          await this.client.command({
            query: individualQuery,
            clickhouse_settings: {
              ...query_settings,
              wait_end_of_query: 1,
            },
          });
        }
      }
    } catch (error) {
      console.error(chalk.bold.red('❌ Failed to execute migration query:'), error);
      throw error;
    }
  }

  async markMigrationApplied(version: string) {
    try {
      if (this.context.verbose) {
        console.log(chalk.gray('🔍 Marking migration applied with version:'), chalk.gray(version));
      }
      const migrationsDatabase = this.context.migrationsDatabase || 'default';
      await this.client.insert({
        table: `${migrationsDatabase}.__clicksuite_migrations`,
        values: [{ version, active: 1, created_at: new Date().toISOString() }],
        format: 'JSONEachRow',
        clickhouse_settings: {
          date_time_input_format: 'best_effort'
        }
      });

      await this.optimizeMigrationTable();
    } catch (error) {
      console.error(chalk.bold.red('❌ Failed to mark migration as applied:'), error);
      throw error;
    }
  }

  async markMigrationRolledBack(version: string) {
    try {
      if (this.context.verbose) {
        console.log(chalk.gray('🔍 Marking migration rolled back for version:'), chalk.gray(version));
      }
      const migrationsDatabase = this.context.migrationsDatabase || 'default';
      await this.client.insert({
        table: `${migrationsDatabase}.__clicksuite_migrations`,
        values: [{ version, active: 0, created_at: new Date().toISOString() }],
        format: 'JSONEachRow',
        clickhouse_settings: {
          date_time_input_format: 'best_effort'
        }
      });

      await this.optimizeMigrationTable();
    } catch (error) {
      console.error(chalk.bold.red('❌ Failed to mark migration as rolled back:'), error);
      throw error;
    }
  }

  async getDatabaseSchema(): Promise<Record<string, string>> {
    const schema: Record<string, string> = {};
    const tables = await this.getDatabaseTables();
    for (const table of tables) {
        try {
            schema[`table/${table.database}.${table.name}`] = await this.getCreateTableQueryForDb(table.name, table.database, 'TABLE');
        } catch (e) { console.warn(chalk.yellow(`⚠️ Could not get CREATE TABLE for ${table.database}.${table.name}`), e); }
    }
    const views = await this.getDatabaseMaterializedViews();
    for (const view of views) {
        try {
            schema[`view/${view.database}.${view.name}`] = await this.getCreateTableQueryForDb(view.name, view.database, 'VIEW');
        } catch (e) { console.warn(chalk.yellow(`⚠️ Could not get CREATE VIEW for ${view.database}.${view.name}`), e); }
    }
    const dictionaries = await this.getDatabaseDictionaries();
    for (const dict of dictionaries) {
        try {
            schema[`dictionary/${dict.database}.${dict.name}`] = await this.getCreateTableQueryForDb(dict.name, dict.database, 'DICTIONARY');
        } catch (e) { console.warn(chalk.yellow(`⚠️ Could not get CREATE DICTIONARY for ${dict.database}.${dict.name}`), e); }
    }
    return schema;
  }

  async getLatestMigration(): Promise<string | undefined> {
     const applied = await this.getAppliedMigrations();
     // Sort by version descending to get the truly latest one, assuming versions are sortable strings like timestamps
     applied.sort((a, b) => b.version.localeCompare(a.version));
     return applied.length > 0 ? applied[0].version : undefined;
  }

  async getDatabaseTables(): Promise<{name: string, database: string}[]> {
    try {
      const resultSet = await this.client.query({
        query: `SELECT name, database FROM system.tables WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA') AND engine NOT LIKE '%View' AND engine != 'MaterializedView' AND table NOT LIKE '%.tmp%'`,
      });
      const response = await resultSet.json();
      return response.data as {name: string, database: string}[];
    } catch (error) {
      console.error(chalk.bold.red('❌ Failed to get database tables:'), error);
      return [];
    }
  }

  async getDatabaseMaterializedViews(): Promise<{name: string, database: string}[]> {
     try {
      const resultSet = await this.client.query({
        query: `SELECT name, database FROM system.tables WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA') AND engine = 'MaterializedView' AND table NOT LIKE '%.tmp%'`,
      });
      const response = await resultSet.json();
      return response.data as {name: string, database: string}[];
    } catch (error) {
      console.error(chalk.bold.red('❌ Failed to get materialized views:'), error);
      return [];
    }
  }

  async getDatabaseDictionaries(): Promise<{name: string, database: string}[]> {
    try {
      const resultSet = await this.client.query({
        query: `SELECT name, database FROM system.dictionaries WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA') AND name NOT LIKE '%.tmp%'`,
      });
      const response = await resultSet.json();
      return response.data as {name: string, database: string}[];
    } catch (error) {
      console.error(chalk.bold.red('❌ Failed to get dictionaries:'), error);
      return [];
    }
  }

  async getDatabaseTablesForDb(database: string): Promise<{name: string}[]> {
    try {
      const resultSet = await this.client.query({
        query: `SELECT name FROM system.tables WHERE database = '${database}' AND engine NOT LIKE '%View' AND engine != 'MaterializedView' AND name NOT LIKE '%.tmp%'`,
      });
      const response = await resultSet.json();
      return response.data as {name: string}[];
    } catch (error) {
      console.error(chalk.bold.red(`❌ Failed to get tables for database ${database}:`), error);
      return [];
    }
  }

  async getDatabaseMaterializedViewsForDb(database: string): Promise<{name: string}[]> {
    try {
      const resultSet = await this.client.query({
        query: `SELECT name FROM system.tables WHERE database = '${database}' AND engine = 'MaterializedView' AND name NOT LIKE '%.tmp%'`,
      });
      const response = await resultSet.json();
      return response.data as {name: string}[];
    } catch (error) {
      console.error(chalk.bold.red(`❌ Failed to get materialized views for database ${database}:`), error);
      return [];
    }
  }

  async getDatabaseDictionariesForDb(database: string): Promise<{name: string}[]> {
    try {
      const resultSet = await this.client.query({
        query: `SELECT name FROM system.dictionaries WHERE database = '${database}' AND name NOT LIKE '%.tmp%'`,
      });
      const response = await resultSet.json();
      return response.data as {name: string}[];
    } catch (error) {
      console.error(chalk.bold.red(`❌ Failed to get dictionaries for database ${database}:`), error);
      return [];
    }
  }
  

  async getCreateTableQueryForDb(name: string, database: string, type: 'TABLE' | 'VIEW' | 'DICTIONARY'): Promise<string> {
    try {
      // For materialized views, we need to use SHOW CREATE TABLE, not SHOW CREATE MATERIALIZED VIEW
      const objectType = type === 'VIEW' ? 'TABLE' : type;
      const showQuery = `SHOW CREATE ${objectType} ${database}.${name}`;
      if (this.context.verbose) {
        console.log(chalk.gray(`🔍  Executing schema query: ${showQuery}`));
      }
      const resultSet = await this.client.query({ query: showQuery });
      const response = await resultSet.json<{statement: string}>();
      if (response.data.length === 0) {
        throw new Error(`No data returned`);
      }

      const resultText = response.data[0].statement;
      
      // Clean up the result text by replacing literal \n with actual newlines and unescaping quotes
      const cleanedText = resultText
        .trim()
        .replace(/\\n/g, '\n')           // Replace literal \n with actual newlines
        .replace(/\\'/g, "'")           // Unescape single quotes
        .replace(/\\"/g, '"')           // Unescape double quotes
        .replace(/\\\\/g, '\\');        // Unescape backslashes
      
      return cleanedText;
    } catch (error) {
      console.error(chalk.bold.red(`❌  Failed to get create query for ${type} ${database}.${name}:`), error);
      throw error;
    }
  }

  async optimizeMigrationTable() {
    try {
      const migrationsDatabase = this.context.migrationsDatabase || 'default';
      await this.client.command({
        query: `OPTIMIZE TABLE ${migrationsDatabase}.__clicksuite_migrations FINAL`,
        clickhouse_settings: {
          wait_end_of_query: 1,
        },
      });
    } catch (error) {
      console.error(chalk.bold.red('❌ Failed to optimize migration table:'), error);
      throw error;
    }
  }

  async close() {
    await this.client.close();
  }

  async clearMigrationsTable() {
    try {
      const clusterClause = this.context.cluster ? `ON CLUSTER ${this.context.cluster}` : '';
      const migrationsDatabase = this.context.migrationsDatabase || 'default';
      const query = `TRUNCATE TABLE IF EXISTS ${migrationsDatabase}.__clicksuite_migrations ${clusterClause}`;
      if (this.context.verbose) {
        console.log(chalk.gray('🔍 Clearing migrations table:'), chalk.gray(query));
      }
      await this.client.command({
        query: query,
        clickhouse_settings: {
          wait_end_of_query: 1,
        },
      });
      console.log(chalk.green('✅ Successfully cleared __clicksuite_migrations table.'));
    } catch (error) {
      console.error(chalk.bold.red('❌ Failed to clear __clicksuite_migrations table:'), error);
      throw error;
    }
  }
}