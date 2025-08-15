import { Context, MigrationFile, MigrationRecord, MigrationStatus, MigrationState, RawMigrationFileContent } from './types';
import { Db } from './db';
import * as fs from 'fs/promises';
import * as fsSync from 'fs';
import * as path from 'path';
import chalk from 'chalk';
import yaml from 'js-yaml';
import inquirer from 'inquirer';

const MIGRATION_FILE_REGEX = /^(\d{14})_([\w-]+)\.yml$/;

// Helper function to interpolate environment variables in SQL
function interpolateEnvVars(sql: string): string {
  return sql.replace(/\$\{([^}]+)\}/g, (_, envVarName) => {
    const envValue = process.env[envVarName];
    if (envValue === undefined) {
      console.warn(chalk.yellow(`⚠️  Warning: Environment variable '${envVarName}' is not set. Using empty string.`));
      return '';
    }
    return envValue;
  });
}

// Helper function to format SQL with table and database names and environment variables
function formatSQL(sql?: string, tableName?: string, databaseName?: string): string | undefined {
  if (!sql) {
    return sql;
  }
  let formatted = sql;
  
  // First, replace table and database placeholders
  if (tableName) {
    formatted = formatted.replace(/\{table\}/g, tableName);
  }
  if (databaseName) {
    formatted = formatted.replace(/\{database\}/g, databaseName);
  }
  
  // Then, interpolate environment variables
  formatted = interpolateEnvVars(formatted);
  
  return formatted;
}

export class Runner {
  private context: Context;
  private db: Db;

  constructor(context: Context) {
    this.context = context;
    this.db = new Db(context);
    if (!path.isAbsolute(this.context.migrationsDir)) {
      this.context.migrationsDir = path.resolve(process.cwd(), this.context.migrationsDir);
      console.warn(chalk.yellow(`⚠️  Runner: migrationsDir was not absolute, resolved to ${this.context.migrationsDir}. This should be resolved in index.ts.`));
    }

    // If a "migrations" subdirectory exists inside the provided directory,
    // prefer it to mirror the CLI behavior.
    try {
      const migrationsSubdir = path.join(this.context.migrationsDir, 'migrations');
      if (fsSync.existsSync(migrationsSubdir) && fsSync.statSync(migrationsSubdir).isDirectory()) {
        this.context.migrationsDir = migrationsSubdir;
      }
    } catch (_) {
      // noop: fall back to provided migrationsDir
    }
  }

  private async _getLocalMigrations(): Promise<MigrationFile[]> {
    const migrationFiles: MigrationFile[] = [];
    try {
      const files = await fs.readdir(this.context.migrationsDir);
      for (const file of files) {
        const match = file.match(MIGRATION_FILE_REGEX);
        if (match) {
          const version = match[1];
          const name = match[2].replace(/-/g, ' ');
          const filePath = path.join(this.context.migrationsDir, file);

          try {
            const fileContent = await fs.readFile(filePath, 'utf-8');
            // js-yaml handles YAML anchors and aliases automatically during parsing.
            const rawContent = yaml.load(fileContent) as RawMigrationFileContent;

            const currentEnvConfig = rawContent[this.context.environment] || {};
            const defaultEnvConfig = rawContent['development'] || {}; // Fallback or for alias base

            // Resolve settings: current env specific, then development (if aliased), then empty
            // js-yaml handles alias merging, so currentEnvConfig should already be merged if it used an alias.
            const querySettings = currentEnvConfig.settings || defaultEnvConfig.settings || {};
            
            // Resolve SQL: current env specific, then development (if aliased)
            let upSQL = currentEnvConfig.up || defaultEnvConfig.up;
            let downSQL = currentEnvConfig.down || defaultEnvConfig.down;

            // Format SQL with table and database names if provided
            const tableName = rawContent.table;
            const databaseName = rawContent.database;
            upSQL = formatSQL(upSQL, tableName, databaseName);
            downSQL = formatSQL(downSQL, tableName, databaseName);
            
            migrationFiles.push({
              version,
              name,
              filePath,
              table: tableName,
              database: databaseName,
              upSQL: upSQL,
              downSQL: downSQL,
              querySettings: querySettings,
            });
          } catch (e: any) {
            console.error(chalk.bold.red(`❌  Error reading or parsing migration file ${filePath}:`), e.message);
            if (e.mark) { // js-yaml provides error location
              console.error(chalk.bold.red(`  at line ${e.mark.line + 1}, column ${e.mark.column + 1}`));
            }
          }
        }
      }
    } catch (e: any) {
      if (e.code === 'ENOENT') {
        console.log(chalk.yellow(`⚠️  Migrations directory ${this.context.migrationsDir} not found. Run 'clicksuite init' to create it.`));
      } else {
        console.error(chalk.bold.red('❌  Error reading migrations directory:'), e.message);
      }
      return [];
    }
    return migrationFiles.sort((a, b) => a.version.localeCompare(b.version));
  }

  /**
   * Initialize the project by creating the migrations directory and the migrations table
   */
  async init() {
    console.log(chalk.blue('⏳  Runner: Initializing Clicksuite environment...'));
    try {
      await this.db.initMigrationsTable();
      const pingResult = await this.db.ping();
      if (pingResult.success) {
        console.log(chalk.green('✅  Successfully connected to ClickHouse.'));
      } else {
        console.error(chalk.bold.red('❌  Failed to connect to ClickHouse.'), pingResult.error);
      }
      console.log(chalk.green('✅  Clicksuite initialized successfully. Migration table is ready.'));
    } catch (error) {
      console.error(chalk.bold.red('❌  Runner init failed:'), error);
      throw error;
    }
  }

  /**
   * Generate a new migration file
   * @param name - The name of the migration
   */
  async generate(migrationNameInput: string) {
    const timestamp = new Date().toISOString().replace(/[-:T.]/g, '').slice(0, 14);
    // Sanitize the migration name for the file name part
    const safeFileNamePart = migrationNameInput.replace(/\s+/g, '-').replace(/[^a-zA-Z0-9_-]/g, '').toLowerCase();
    const filename = `${timestamp}_${safeFileNamePart}.yml`;
    const filePath = path.join(this.context.migrationsDir, filename);

    const migrationContent = {
      version: timestamp,
      name: migrationNameInput, // Keep original name for display purposes inside YAML
      table: "your_table_name", // Placeholder for the user
      database: "your_database_name", // Placeholder for the user

      development: {
        // YAML anchor. Note: js-yaml stringifies with quotes, which is fine.
        // For true anchors in output, manual string construction or more complex YAML lib might be needed.
        // However, for parsing, js-yaml handles `<<: *anchor` correctly if typed manually by user.
        // The goal here is a good starting template.
        up: "CREATE TABLE {database}.{table}",
        down: "DROP TABLE IF EXISTS {database}.{table}",
        settings: {},
      },
      test: {
        // Example of aliasing if user wants to type it, though js-yaml.dump won't create anchors/aliases by default.
        // '<<': '*development_defaults', // This is how user would type it. Dump will expand it.
        // For a new file, we can just pre-fill it as if expanded or keep it minimal.
        up: "-- SQL to apply migration for test (defaults to development if not specified or using aliases)",
        down: "-- SQL to roll back migration for test (defaults to development if not specified or using aliases)",
        settings: {},
      },
      production: {
        up: "-- SQL to apply migration for production",
        down: "-- SQL to roll back migration for production",
        settings: {},
      }
    };

    // Constructing a YAML string that demonstrates aliases for user to understand the pattern:
    const yamlString = `version: "${migrationContent.version}"
name: "${migrationContent.name}"
table: "${migrationContent.table}"
database: "${migrationContent.database}"

development: &development_defaults
  up: |
    ${migrationContent.development.up.split('\n').join('\n    ')}
  down: |
    ${migrationContent.development.down.split('\n').join('\n    ')}
  settings: ${JSON.stringify(migrationContent.development.settings)}

test:
  <<: *development_defaults
  # up: |
  #   -- SQL for test up (override)
  # down: |
  #   -- SQL for test down (override)

production:
  <<: *development_defaults
  # up: |
  #   -- SQL for production up (override)
  # down: |
  #   -- SQL for production down (override)
`;

    try {
      await fs.mkdir(this.context.migrationsDir, { recursive: true });
      await fs.writeFile(filePath, yamlString);
      console.log(chalk.green(`✅  Generated new migration file: ${filePath}`));
      console.log(chalk.yellow('ℹ️  Please edit this file to add your environment-specific migration SQL and update the `table` field.'));
    } catch (e: any) {
      console.error(chalk.bold.red(`❌  Error generating migration file ${filePath}:`), e.message);
    }
  }

  async status() {
    console.log(chalk.blue('🔍  Fetching migration status...'));
    const localMigrations = await this._getLocalMigrations();
    const dbRecords = await this.db.getAllMigrationRecords();

    const statusList: MigrationStatus[] = [];

    const dbMap = new Map<string, MigrationRecord>();
    dbRecords.forEach(rec => dbMap.set(rec.version, rec));

    for (const local of localMigrations) {
      const dbRec = dbMap.get(local.version);
      let state: MigrationState;
      let appliedAt: string | undefined;

      if (dbRec) {
        state = dbRec.active === 1 ? 'APPLIED' : 'INACTIVE';
        appliedAt = dbRec.created_at;
        dbMap.delete(local.version);
      } else {
        state = 'PENDING';
      }
      statusList.push({ ...local, state, appliedAt });
    }

    dbMap.forEach(dbRec => {
      statusList.push({
        version: dbRec.version,
        name: 'N/A (DB only. Likely a legacy migration file)',
        filePath: 'N/A',
        state: dbRec.active === 1 ? 'APPLIED' : 'INACTIVE',
        appliedAt: dbRec.created_at,
      });
    });

    statusList.sort((a, b) => a.version.localeCompare(b.version));

    if (statusList.length === 0) {
      console.log(chalk.yellow('ℹ️  No migrations found locally or in the database.'));
      return;
    }

    console.log(chalk.bold(`\nMigration Status (Env: ${this.context.environment}, Migrations DB: ${this.context.migrationsDatabase || 'default'}):`));
    console.log(chalk.gray('-------------------------------------------------------------------------------------'));
    statusList.forEach(s => {
      let stateChalk = chalk.yellow;
      if (s.state === 'APPLIED') stateChalk = chalk.green;
      if (s.state === 'INACTIVE') stateChalk = chalk.gray;
      
      const nameDisplay = s.name === 'N/A (DB only)' ? chalk.italic(s.name) : s.name;
      const dateDisplay = s.appliedAt ? chalk.dim(`(Applied: ${new Date(s.appliedAt).toLocaleString()})`) : '';

      console.log(
        `${stateChalk.bold(s.state.padEnd(10))}` +
        `${chalk.cyan(s.version)} - ${nameDisplay} ${dateDisplay}`
      );
    });
    console.log(chalk.gray('-------------------------------------------------------------------------------------'));
  }

  async migrate() {
    console.log(chalk.blue('⏳  Running pending migrations (migrate:up)...'));
    await this.up();
  }

  async up(targetVersion?: string) {
    const actionWord = this.context.dryRun ? 'Previewing' : 'Executing';
    console.log(chalk.blue(`⏳ ${actionWord} UP migrations for environment '${this.context.environment}'... ${targetVersion ? 'Target: ' + targetVersion : 'All pending'}`));
    const localMigrations = await this._getLocalMigrations();
    const dbAppliedMigrations = await this.db.getAppliedMigrations();
    const appliedVersions = new Set(dbAppliedMigrations.map(m => m.version));

    const pendingMigrations = localMigrations
      .filter(lm => !appliedVersions.has(lm.version))
      .sort((a, b) => a.version.localeCompare(b.version));

    if (pendingMigrations.length === 0) {
      const message = this.context.dryRun 
        ? 'No pending migrations to preview. Database is up-to-date.'
        : 'No pending migrations to apply. Database is up-to-date.';
      console.log(chalk.green(`ℹ️  ${message}`));
      return;
    }

    let migrationsToRun = pendingMigrations;
    if (targetVersion) {
      const targetIdx = migrationsToRun.findIndex(m => m.version === targetVersion);
      if (targetIdx === -1) {
        console.error(chalk.bold.red(`❌  Target version ${targetVersion} not found among pending or already applied (but not active).`));
        return;
      }
      migrationsToRun = migrationsToRun.slice(0, targetIdx + 1);
      if (migrationsToRun.length === 0) {
        console.log(chalk.yellow(`ℹ️  Target version ${targetVersion} seems to be already applied or no prior pending migrations.`));
        return;
      }
    }

    if (this.context.dryRun) {
      console.log(chalk.cyan(`🔍  DRY RUN: The following ${migrationsToRun.length} migration(s) would be applied:`));
      migrationsToRun.forEach(m => {
        console.log(chalk.cyan(`  ✅  ${m.version} - ${m.name}`));
      });
    } else {
      console.log(chalk.yellow(`🔍  Found ${migrationsToRun.length} migration(s) to apply.`));
    }

    for (const migration of migrationsToRun) {
      const migrationTitle = this.context.dryRun 
        ? `DRY RUN: Migration ${migration.version} - ${migration.name}` 
        : `⏳  Applying migration: ${migration.version} - ${migration.name}`;
      
      console.log(chalk.magenta(`\n${migrationTitle}`));
      
      if (!migration.upSQL) {
        const skipMessage = this.context.dryRun
          ? `Would skip ${migration.version}: No 'up' SQL found for environment '${this.context.environment}'.`
          : `⏭️  Skipping ${migration.version}: No 'up' SQL found for environment '${this.context.environment}'.`;
        console.warn(chalk.yellow(skipMessage));
        continue;
      }

      try {
        if (this.context.dryRun) {
          console.log(chalk.cyan('┌─') + chalk.cyan(`─ DRY RUN: Migration ${migration.version} - ${migration.name} `).padEnd(70, '─') + chalk.cyan('─'));
          console.log(chalk.cyan('│') + ` Environment: ${this.context.environment}`);
          if (migration.database) console.log(chalk.cyan('│') + ` Database: ${migration.database}`);
          if (migration.table) console.log(chalk.cyan('│') + ` Table: ${migration.table}`);
          console.log(chalk.cyan('│') + ' ');
          
          // Count queries for display
          const queryCount = migration.upSQL.split(';').filter(q => q.trim().length > 0).length;
          const queryLabel = queryCount === 1 ? 'query' : 'queries';
          console.log(chalk.cyan('│') + ` SQL to execute (${queryCount} ${queryLabel}):`);
          
          // Show each query indented
          const queries = migration.upSQL.split(';').filter(q => q.trim().length > 0);
          queries.forEach(query => {
            console.log(chalk.cyan('│') + `   ${query.trim()};`);
            if (queries.indexOf(query) < queries.length - 1) {
              console.log(chalk.cyan('│') + '   ');
            }
          });
          
          console.log(chalk.cyan('└') + chalk.cyan('─'.repeat(70)));
        } else {
          if (this.context.verbose) {
            console.log(chalk.gray('--- UP SQL (Env: ') + chalk.cyan(this.context.environment) + chalk.gray(') ---'));
            console.log(chalk.gray(migration.upSQL.trim()));
            console.log(chalk.gray('--------------'));
            if (migration.table || migration.database) {
                const details = [];
                if (migration.database) details.push(`database: ${migration.database}`);
                if (migration.table) details.push(`table: ${migration.table}`);
                console.log(chalk.dim(`(Using ${details.join(', ')})`));
            }
          }
          await this.db.executeMigration(migration.upSQL, migration.querySettings);
          await this.db.markMigrationApplied(migration.version);
          console.log(chalk.green(`✅  Successfully applied ${migration.version} - ${migration.name}`));
        }
      } catch (error: any) {
        if (!this.context.dryRun) {
          console.error(chalk.bold.red(`❌  Error applying migration ${migration.version} - ${migration.name}:`), error.message);
          console.error(chalk.bold.red('❌  Migration process halted due to error.'));
          throw error;
        }
      }
    }
    
    if (this.context.dryRun) {
      console.log(chalk.cyan(`\n🔍  DRY RUN COMPLETE: ${migrationsToRun.length} migration(s) would be applied (no changes made)`));
    } else {
      console.log(chalk.greenBright('\n✅  All selected UP migrations applied successfully!'));
      if (!this.context.skipSchemaUpdate) {
        await this._updateSchemaFile();
      }
    }
  }

  async down(targetVersionToBecomeLatest?: string) {
    const localMigrations = await this._getLocalMigrations();
    const localMigrationsMap = new Map(localMigrations.map(m => [m.version, m]));
    // Get all active migrations, sorted by version ascending (oldest first)
    const appliedDbMigrations = (await this.db.getAppliedMigrations())
      .sort((a, b) => a.version.localeCompare(b.version));

    if (appliedDbMigrations.length === 0) {
      console.log(chalk.yellow('ℹ️  No active migrations in the database to roll back.'));
      return;
    }

    let migrationsToEffectivelyRollback: { version: string, name: string, downSQL?: string, querySettings?: Record<string,any>, table?: string, database?: string }[] = [];

    if (!targetVersionToBecomeLatest) {
      // Case 1: No target version specified - roll back the single last applied migration
      const lastAppliedDbRecord = appliedDbMigrations[appliedDbMigrations.length - 1];
      const actionWord = this.context.dryRun ? 'Previewing rollback of' : 'Attempting to roll back';
      console.log(chalk.blue(`🔍  No specific version provided. ${actionWord} the last applied migration: ${lastAppliedDbRecord.version}`));
      const correspondingLocalFile = localMigrationsMap.get(lastAppliedDbRecord.version);
      if (correspondingLocalFile) {
        migrationsToEffectivelyRollback.push(correspondingLocalFile);
      } else {
        console.error(chalk.bold.red(`❌  Local migration file for version ${lastAppliedDbRecord.version} not found. Cannot roll back.`));
        return;
      }
    } else {
      // Case 2: Target version specified - roll back all migrations *after* this version
      const actionWord = this.context.dryRun ? 'Previewing rollback of migrations' : 'Attempting to roll back migrations';
      console.log(chalk.blue(`🔍  ${actionWord} until version ${targetVersionToBecomeLatest} is the latest applied (or only one if it's the target)...`));

      const targetIndexInApplied = appliedDbMigrations.findIndex(m => m.version === targetVersionToBecomeLatest);

      if (targetIndexInApplied === -1) {
        console.error(chalk.bold.red(`❌ Target version ${targetVersionToBecomeLatest} is not currently applied. Cannot roll back to this state.`));
        // Further check: does this version even exist locally?
        if (!localMigrationsMap.has(targetVersionToBecomeLatest)){
            console.error(chalk.bold.red(`❌ Additionally, version ${targetVersionToBecomeLatest} does not exist in local migration files.`));
        }
        return;
      }

      // Migrations to roll back are those applied *after* the targetVersionToBecomeLatest
      // These are from targetIndexInApplied + 1 to the end of the appliedDbMigrations array.
      // We need to roll them back in reverse order of application (latest first).
      const dbRecordsToRollback = appliedDbMigrations.slice(targetIndexInApplied + 1).reverse();
      
      if (dbRecordsToRollback.length === 0) {
        console.log(chalk.green(`✅ Version ${targetVersionToBecomeLatest} is already the latest applied migration or no migrations were applied after it. No rollback needed.`));
        return;
      }

      for (const dbRec of dbRecordsToRollback) {
          const localFile = localMigrationsMap.get(dbRec.version);
          if (localFile) {
              migrationsToEffectivelyRollback.push(localFile);
          } else {
              console.warn(chalk.yellow(`⚠️ Local migration file for version ${dbRec.version} (which is applied in DB) not found. Cannot automatically roll it back.`));
          }
      }
    }

    if (migrationsToEffectivelyRollback.length === 0) {
      console.log(chalk.yellow('ℹ️ No migrations selected for rollback operation.'));
      return;
    }

    if (this.context.dryRun) {
      console.log(chalk.cyan(`🔍 DRY RUN: The following ${migrationsToEffectivelyRollback.length} migration(s) would be rolled back (in order):`));
      migrationsToEffectivelyRollback.forEach(m => console.log(chalk.cyan(`  ✅ ${m.version} - ${m.name}`)));
    } else {
      console.log(chalk.magenta(`⏳ The following ${migrationsToEffectivelyRollback.length} migration(s) will be rolled back (in order):`));
      migrationsToEffectivelyRollback.forEach(m => console.log(chalk.magenta(`  ⏳ ${m.version} - ${m.name}`)));

      if (!this.context.nonInteractive) {
        const answers = await inquirer.prompt([
          {
            type: 'confirm',
            name: 'confirmation',
            message: `Are you sure you want to roll back these ${migrationsToEffectivelyRollback.length} migration(s)?`,
            default: false,
          },
        ]);
        if (!answers.confirmation) {
          console.log(chalk.gray('ℹ️ Rollback cancelled by user.'));
          return;
        }
      }
    }

    for (const migration of migrationsToEffectivelyRollback) {
      const migrationTitle = this.context.dryRun 
        ? `DRY RUN: Rolling back ${migration.version} - ${migration.name}` 
        : `⏳ Rolling back migration: ${migration.version} - ${migration.name}`;
      
      console.log(chalk.magenta(`\n${migrationTitle}`));
      
      if (!migration.downSQL) {
        const skipMessage = this.context.dryRun
          ? `Would skip ${migration.version}: No 'down' SQL found for environment '${this.context.environment}'.`
          : `⏭️ Skipping ${migration.version}: No 'down' SQL found for environment '${this.context.environment}'.`;
        console.warn(chalk.yellow(skipMessage));
        continue; // Or halt, depending on desired strictness
      }

      try {
        if (this.context.dryRun) {
          console.log(chalk.cyan('┌─') + chalk.cyan(`─ DRY RUN: Rollback ${migration.version} - ${migration.name} `).padEnd(70, '─') + chalk.cyan('─'));
          console.log(chalk.cyan('│') + ` Environment: ${this.context.environment}`);
          if (migration.database) console.log(chalk.cyan('│') + ` Database: ${migration.database}`);
          if (migration.table) console.log(chalk.cyan('│') + ` Table: ${migration.table}`);
          console.log(chalk.cyan('│') + ' ');
          
          // Count queries for display
          const queryCount = migration.downSQL.split(';').filter(q => q.trim().length > 0).length;
          const queryLabel = queryCount === 1 ? 'query' : 'queries';
          console.log(chalk.cyan('│') + ` SQL to execute (${queryCount} ${queryLabel}):`);
          
          // Show each query indented
          const queries = migration.downSQL.split(';').filter(q => q.trim().length > 0);
          queries.forEach(query => {
            console.log(chalk.cyan('│') + `   ${query.trim()};`);
            if (queries.indexOf(query) < queries.length - 1) {
              console.log(chalk.cyan('│') + '   ');
            }
          });
          
          console.log(chalk.cyan('└') + chalk.cyan('─'.repeat(70)));
        } else {
          if (this.context.verbose) {
            console.log(chalk.gray('--- DOWN SQL (Env: ') + chalk.cyan(this.context.environment) + chalk.gray(') ---'));
            console.log(chalk.gray(migration.downSQL.trim()));
            console.log(chalk.gray('----------------'));
            if (migration.table || migration.database) {
              const details = [];
              if (migration.database) details.push(`database: ${migration.database}`);
              if (migration.table) details.push(`table: ${migration.table}`);
              console.log(chalk.dim(`(Using ${details.join(', ')})`));
            }
          }
          await this.db.executeMigration(migration.downSQL, migration.querySettings);
          await this.db.markMigrationRolledBack(migration.version);
          console.log(chalk.green(`✅ Successfully rolled back ${migration.version} - ${migration.name}`));
        }
      } catch (error: any) {
        if (!this.context.dryRun) {
          console.error(chalk.bold.red(`❌ Error rolling back migration ${migration.version} - ${migration.name}:`), error.message);
          console.error(chalk.bold.red('Rollback process halted due to error.'));
          throw error; // Re-throw to stop further rollbacks on error
        }
      }
    }
    
    if (this.context.dryRun) {
      console.log(chalk.cyan(`\n🔍 DRY RUN COMPLETE: ${migrationsToEffectivelyRollback.length} migration(s) would be rolled back (no changes made)`));
    } else {
      console.log(chalk.greenBright('\n✅ Selected DOWN migrations completed successfully!'));
      if (!this.context.skipSchemaUpdate) {
        await this._updateSchemaFile();
      }
    }
  }

  async reset() {
    console.warn(chalk.yellow.bold('⚠️ WARNING: This will roll back all applied migrations and clear the migrations table.'));
    let proceed = this.context.nonInteractive;
    if (!proceed) {
      const answers = await inquirer.prompt([
        {
          type: 'confirm',
          name: 'confirmation',
          message: 'Are you sure you want to reset all migrations? This action cannot be undone.',
          default: false,
        },
      ]);
      proceed = answers.confirmation;
    }

    if (!proceed) {
      console.log(chalk.gray('ℹ️ Migration reset cancelled by user.'));
      return;
    }

    console.log(chalk.blue('⏳ Starting migration reset...'));
    const appliedDbMigrations = (await this.db.getAppliedMigrations()).sort((a,b) => b.version.localeCompare(a.version));
    const localMigrations = await this._getLocalMigrations();
    const localMigrationsMap = new Map(localMigrations.map(m => [m.version, m]));

    if (appliedDbMigrations.length === 0) {
      console.log(chalk.yellow('ℹ️ No applied migrations found in the database to roll back.'));
    } else {
      console.log(chalk.magenta(`🔍 Found ${appliedDbMigrations.length} applied migration(s) to roll back.`));
      for (const dbMigration of appliedDbMigrations) {
        console.log(chalk.blue(`\n⏳ Rolling back: ${dbMigration.version}`));
        const localFile = localMigrationsMap.get(dbMigration.version);
        if (!localFile || !localFile.downSQL) {
          console.warn(chalk.yellow(` ⏭️ Skipping rollback of ${dbMigration.version}: No local file or downSQL found for env '${this.context.environment}'.`));
          continue;
        }
        try {
          if (this.context.verbose) {
            console.log(chalk.gray('  --- DOWN SQL (Env: ') + chalk.cyan(this.context.environment) + chalk.gray(') ---'));
            console.log(chalk.gray(`  ${localFile.downSQL.trim().split('\n').join('\n  ')}`));
            console.log(chalk.gray('  ----------------'));
             if (localFile.table || localFile.database) {
              const details = [];
              if (localFile.database) details.push(`database: ${localFile.database}`);
              if (localFile.table) details.push(`table: ${localFile.table}`);
              console.log(chalk.dim(`  (Using ${details.join(', ')})`));
            }
          }
          await this.db.executeMigration(localFile.downSQL, localFile.querySettings);
        } catch (error: any) {
          console.error(chalk.bold.red(`  ❌ Error executing downSQL for migration ${dbMigration.version}:`), error.message);
          console.error(chalk.bold.red('  Reset process halted due to error. Some migrations may remain in the database. Manual cleanup might be required.'));
          throw error;
        }
      }
    }

    try {
      console.log(chalk.blue('\n⏳ Clearing the __clicksuite_migrations table...'));
      await this.db.clearMigrationsTable();
      await this.db.optimizeMigrationTable();
      console.log(chalk.greenBright('\n✅ Database migrations have been reset successfully!'));
      if (!this.context.skipSchemaUpdate) {
        await this._updateSchemaFile();
      }
    } catch (error: any) {
      console.error(chalk.bold.red('❌ Error clearing or optimizing migrations table during reset:'), error.message);
      throw error;
    }
  }

  private async _updateSchemaFile() {
    const schemaPath = path.join(this.context.migrationsDir, 'schema.sql');
    
    try {
      if (this.context.verbose) {
        console.log(chalk.dim(`🔍 Updating schema file for all databases (excluding system databases)`));
      }
      
      // Use the existing getDatabaseSchema method which already handles all the logic
      const schema = await this.db.getDatabaseSchema();
      
      // Get all database objects to count them for verbose output
      if (this.context.verbose) {
        const allTables = await this.db.getDatabaseTables();
        const allViews = await this.db.getDatabaseMaterializedViews();
        const allDictionaries = await this.db.getDatabaseDictionaries();
        console.log(chalk.dim(`🔍 Found ${allTables.length} tables, ${allViews.length} views, ${allDictionaries.length} dictionaries across all databases`));
      }
      
      // Get unique database names from schema keys
      const uniqueDatabases = new Set<string>();
      Object.keys(schema).forEach(key => {
        const match = key.match(/^(table|view|dictionary)\/(.+)\.(.+)$/);
        if (match) {
          uniqueDatabases.add(match[2]); // Extract database name
        }
      });
      
      let schemaContent = `-- Auto-generated schema file
-- This file contains table definitions, materialized view definitions, and dictionary definitions
-- Generated on: ${new Date().toISOString()}
-- Environment: ${this.context.environment}
-- Databases: ${Array.from(uniqueDatabases).sort().join(', ')}

`;

      // Group schema entries by type
      const tables = Object.entries(schema).filter(([key]) => key.startsWith('table/'));
      const views = Object.entries(schema).filter(([key]) => key.startsWith('view/'));
      const dictionaries = Object.entries(schema).filter(([key]) => key.startsWith('dictionary/'));

      // Add table definitions
      if (tables.length > 0) {
        schemaContent += '\n-- =====================================================\n';
        schemaContent += '-- TABLES\n';
        schemaContent += '-- =====================================================\n';
        for (const [key, createStatement] of tables) {
          const objectName = key.replace('table/', '');
          schemaContent += `\n-- Table: ${objectName}\n`;
          schemaContent += createStatement;
          if (!createStatement.endsWith(';')) {
            schemaContent += ';';
          }
          schemaContent += '\n\n';
        }
      }

      // Add materialized view definitions
      if (views.length > 0) {
        schemaContent += '\n-- =====================================================\n';
        schemaContent += '-- MATERIALIZED VIEWS\n';
        schemaContent += '-- =====================================================\n';
        for (const [key, createStatement] of views) {
          const objectName = key.replace('view/', '');
          schemaContent += `\n-- Materialized View: ${objectName}\n`;
          schemaContent += createStatement;
          if (!createStatement.endsWith(';')) {
            schemaContent += ';';
          }
          schemaContent += '\n\n';
        }
      }

      // Add dictionary definitions
      if (dictionaries.length > 0) {
        schemaContent += '\n-- =====================================================\n';
        schemaContent += '-- DICTIONARIES\n';
        schemaContent += '-- =====================================================\n';
        for (const [key, createStatement] of dictionaries) {
          const objectName = key.replace('dictionary/', '');
          schemaContent += `\n-- Dictionary: ${objectName}\n`;
          schemaContent += createStatement;
          if (!createStatement.endsWith(';')) {
            schemaContent += ';';
          }
          schemaContent += '\n\n';
        }
      }

      await fs.writeFile(schemaPath, schemaContent);
      if (this.context.verbose) {
        console.log(chalk.dim(`✅ Schema file updated: ${schemaPath}`));
      } else {
        console.log(chalk.green('✅ Schema file updated'));
      }
    } catch (error: any) {
      console.warn(chalk.yellow(`⚠️ Warning: Could not update schema file: ${error.message}`));
    }
  }

  async schemaLoad() {
    console.log(chalk.blue('⏳ Loading schema from local migration files into the database (marking as applied without running SQL)...'));
    const localMigrations = await this._getLocalMigrations();
    const dbRecords = await this.db.getAllMigrationRecords();
    const dbMap = new Map<string, MigrationRecord>();
    dbRecords.forEach(rec => dbMap.set(rec.version, rec));

    if (localMigrations.length === 0) {
      console.log(chalk.yellow('ℹ️ No local migration files found to load.'));
      return;
    }

    let loadedCount = 0;
    let skippedCount = 0;

    for (const migration of localMigrations) {
      const existingRecord = dbMap.get(migration.version);
      if (existingRecord && existingRecord.active === 1) {
        console.log(chalk.gray(`⏭️ Skipping ${migration.version} - ${migration.name}: Already marked as active in DB.`));
        skippedCount++;
        continue;
      }
      
      try {
        await this.db.markMigrationApplied(migration.version);
        console.log(chalk.green(`✅ Loaded ${migration.version} - ${migration.name} into migrations table as APPLIED.`));
        loadedCount++;
      } catch (error: any) {
        console.error(chalk.bold.red(`❌ Error loading migration ${migration.version} - ${migration.name} into DB:`), error.message);
      }
    }

    console.log(chalk.greenBright('\n✅ Schema loading process complete.'));
    console.log(chalk.cyan(`  ℹ️ ${loadedCount} migration(s) newly marked as APPLIED.`));
    console.log(chalk.gray(`  ℹ️ ${skippedCount} migration(s) were already APPLIED and skipped.`));
    
    if (loadedCount > 0) {
        try {
            await this.db.optimizeMigrationTable();
            if (!this.context.skipSchemaUpdate) {
                await this._updateSchemaFile();
            }
        } catch (e) { /* error already logged by optimizeMigrationTable */ }
    }
  }
}