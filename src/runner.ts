import { Context, MigrationFile, MigrationRecord, MigrationStatus, MigrationState, RawMigrationFileContent } from './types';
import { Db } from './db';
import * as fs from 'fs/promises';
import * as path from 'path';
import chalk from 'chalk';
import yaml from 'js-yaml';
import inquirer from 'inquirer';

const MIGRATION_FILE_REGEX = /^(\d{14})_([\w-]+)\.yml$/;

// Helper function to format SQL with table name
function formatSQL(sql?: string, tableName?: string): string | undefined {
  if (!sql || !tableName) {
    return sql;
  }
  return sql.replace(/\{table\}/g, tableName);
}

export class Runner {
  private context: Context;
  private db: Db;

  constructor(context: Context) {
    this.context = context;
    this.db = new Db(context);
    if (!path.isAbsolute(this.context.migrationsDir)) {
      this.context.migrationsDir = path.resolve(process.cwd(), this.context.migrationsDir);
      console.warn(chalk.yellow(`Runner: migrationsDir was not absolute, resolved to ${this.context.migrationsDir}. This should be resolved in index.ts.`));
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

            // Format SQL with table name if provided
            const tableName = rawContent.table;
            upSQL = formatSQL(upSQL, tableName);
            downSQL = formatSQL(downSQL, tableName);
            
            migrationFiles.push({
              version,
              name,
              filePath,
              table: tableName,
              upSQL: upSQL,
              downSQL: downSQL,
              querySettings: querySettings,
            });
          } catch (e: any) {
            console.error(chalk.red(`Error reading or parsing migration file ${filePath}:`), e.message);
            if (e.mark) { // js-yaml provides error location
              console.error(chalk.red(`  at line ${e.mark.line + 1}, column ${e.mark.column + 1}`));
            }
          }
        }
      }
    } catch (e: any) {
      if (e.code === 'ENOENT') {
        console.log(chalk.yellow(`Migrations directory ${this.context.migrationsDir} not found. Run 'clicksuite init' to create it.`));
      } else {
        console.error(chalk.red('Error reading migrations directory:'), e.message);
      }
      return [];
    }
    return migrationFiles.sort((a, b) => a.version.localeCompare(b.version));
  }

  /**
   * Initialize the project by creating the migrations directory and the migrations table
   */
  async init() {
    console.log(chalk.blue('Runner: Initializing Clicksuite environment...'));
    try {
      await this.db.initMigrationsTable();
      const pingResult = await this.db.ping();
      if (pingResult.success) {
        console.log(chalk.green('Successfully connected to ClickHouse.'));
      } else {
        console.error(chalk.red('Failed to connect to ClickHouse.'), pingResult.error);
      }
      console.log(chalk.green('Clicksuite initialized successfully. Migration table is ready.'));
    } catch (error) {
      console.error(chalk.bold.red('⚠️ Runner init failed:'), error);
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

      development: {
        // YAML anchor. Note: js-yaml stringifies with quotes, which is fine.
        // For true anchors in output, manual string construction or more complex YAML lib might be needed.
        // However, for parsing, js-yaml handles `<<: *anchor` correctly if typed manually by user.
        // The goal here is a good starting template.
        up: "CREATE TABLE {table}",
        down: "DROP TABLE IF EXISTS {table}",
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
      console.log(chalk.green(`Generated new migration file: ${filePath}`));
      console.log(chalk.yellow('Please edit this file to add your environment-specific migration SQL and update the `table` field.'));
    } catch (e: any) {
      console.error(chalk.red(`Error generating migration file ${filePath}:`), e.message);
    }
  }

  async status() {
    console.log(chalk.blue('Fetching migration status...'));
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
      console.log(chalk.yellow('No migrations found locally or in the database.'));
      return;
    }

    console.log(chalk.bold(`\nMigration Status (Env: ${this.context.environment}, DB: ${this.context.database} on ${this.context.host}):`));
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
    console.log(chalk.blue('Running pending migrations (migrate:up)...'));
    await this.up();
  }

  async up(targetVersion?: string) {
    console.log(chalk.blue(`Executing UP migrations for environment '${this.context.environment}'... ${targetVersion ? 'Target: ' + targetVersion : 'All pending'}`));
    const localMigrations = await this._getLocalMigrations();
    const dbAppliedMigrations = await this.db.getAppliedMigrations();
    const appliedVersions = new Set(dbAppliedMigrations.map(m => m.version));

    const pendingMigrations = localMigrations
      .filter(lm => !appliedVersions.has(lm.version))
      .sort((a, b) => a.version.localeCompare(b.version));

    if (pendingMigrations.length === 0) {
      console.log(chalk.green('No pending migrations to apply. Database is up-to-date.'));
      return;
    }

    let migrationsToRun = pendingMigrations;
    if (targetVersion) {
      const targetIdx = migrationsToRun.findIndex(m => m.version === targetVersion);
      if (targetIdx === -1) {
        console.error(chalk.red(`Target version ${targetVersion} not found among pending or already applied (but not active).`));
        return;
      }
      migrationsToRun = migrationsToRun.slice(0, targetIdx + 1);
      if (migrationsToRun.length === 0) {
        console.log(chalk.yellow(`Target version ${targetVersion} seems to be already applied or no prior pending migrations.`));
        return;
      }
    }

    console.log(chalk.yellow(`Found ${migrationsToRun.length} migration(s) to apply.`));

    for (const migration of migrationsToRun) {
      console.log(chalk.magenta(`\nApplying migration: ${migration.version} - ${migration.name}`));
      if (!migration.upSQL) {
        console.warn(chalk.yellow(`Skipping ${migration.version}: No 'up' SQL found for environment '${this.context.environment}'.`));
        continue;
      }
      try {
        console.log(chalk.gray('--- UP SQL (Env: ') + chalk.cyan(this.context.environment) + chalk.gray(') ---'));
        console.log(chalk.gray(migration.upSQL.trim()));
        console.log(chalk.gray('--------------'));
        if (migration.table) {
            console.log(chalk.dim(`(Using table: ${migration.table})`));
        }
        await this.db.executeMigration(migration.upSQL, migration.querySettings);
        await this.db.markMigrationApplied(migration.version);
        console.log(chalk.green(`Successfully applied ${migration.version} - ${migration.name}`));
      } catch (error: any) {
        console.error(chalk.bold.red(`⚠️ Error applying migration ${migration.version} - ${migration.name}:`), error.message);
        console.error(chalk.bold.red('Migration process halted due to error.'));
        throw error;
      }
    }
    console.log(chalk.greenBright('\nAll selected UP migrations applied successfully!'));
  }

  async down(targetVersionInput?: string) {
    let targetVersion = targetVersionInput;
    const localMigrations = await this._getLocalMigrations();
    const appliedDbMigrations = await this.db.getAppliedMigrations();

    if (appliedDbMigrations.length === 0) {
      console.log(chalk.yellow('No active migrations in the database to roll back.'));
      return;
    }

    if (!targetVersion) {
      appliedDbMigrations.sort((a, b) => b.version.localeCompare(a.version));
      targetVersion = appliedDbMigrations[0].version;
      console.log(chalk.blue(`No specific version provided. Attempting to roll back the last applied migration: ${targetVersion}`));
    } else {
      console.log(chalk.blue(`Executing DOWN migration for version: ${targetVersion} in environment '${this.context.environment}'`));
    }
    
    const migrationToRollback = localMigrations.find(m => m.version === targetVersion);

    if (!migrationToRollback) {
      console.error(chalk.red(`Local migration file for version ${targetVersion} not found. Cannot determine down SQL.`));
      return;
    }

    const isAppliedInDb = appliedDbMigrations.some(m => m.version === targetVersion);
    if (!isAppliedInDb) {
      console.warn(chalk.yellow(`Migration ${targetVersion} is not currently active in the database. Cannot roll back.`));
      return;
    }

    if (!migrationToRollback.downSQL) {
      console.error(chalk.red(`No 'down' SQL found for migration ${targetVersion} in environment '${this.context.environment}'. Cannot roll back.`));
      return;
    }

    console.log(chalk.magenta(`\nRolling back migration: ${migrationToRollback.version} - ${migrationToRollback.name}`));
    try {
      console.log(chalk.gray('--- DOWN SQL (Env: ') + chalk.cyan(this.context.environment) + chalk.gray(') ---'));
      console.log(chalk.gray(migrationToRollback.downSQL.trim()));
      console.log(chalk.gray('----------------'));
      if (migrationToRollback.table) {
        console.log(chalk.dim(`(Using table: ${migrationToRollback.table})`));
      }
      await this.db.executeMigration(migrationToRollback.downSQL, migrationToRollback.querySettings);
      await this.db.markMigrationRolledBack(migrationToRollback.version);
      console.log(chalk.green(`Successfully rolled back ${migrationToRollback.version} - ${migrationToRollback.name}`));
    } catch (error: any) {
      console.error(chalk.bold.red(`⚠️ Error rolling back migration ${migrationToRollback.version} - ${migrationToRollback.name}:`), error.message);
      console.error(chalk.bold.red('Rollback process halted due to error.'));
      throw error;
    }
     console.log(chalk.greenBright('\nDOWN migration completed successfully!'));
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
      console.log(chalk.gray('Migration reset cancelled by user.'));
      return;
    }

    console.log(chalk.blue('Starting migration reset...'));
    const appliedDbMigrations = (await this.db.getAppliedMigrations()).sort((a,b) => b.version.localeCompare(a.version));
    const localMigrations = await this._getLocalMigrations();
    const localMigrationsMap = new Map(localMigrations.map(m => [m.version, m]));

    if (appliedDbMigrations.length === 0) {
      console.log(chalk.yellow('No applied migrations found in the database to roll back.'));
    } else {
      console.log(chalk.magenta(`Found ${appliedDbMigrations.length} applied migration(s) to roll back.`));
      for (const dbMigration of appliedDbMigrations) {
        console.log(chalk.blue(`\nRolling back: ${dbMigration.version}`));
        const localFile = localMigrationsMap.get(dbMigration.version);
        if (!localFile || !localFile.downSQL) {
          console.warn(chalk.yellow(`  Skipping rollback of ${dbMigration.version}: No local file or downSQL found for env '${this.context.environment}'.`));
          continue;
        }
        try {
          console.log(chalk.gray('  --- DOWN SQL (Env: ') + chalk.cyan(this.context.environment) + chalk.gray(') ---'));
          console.log(chalk.gray(`  ${localFile.downSQL.trim().split('\n').join('\n  ')}`));
          console.log(chalk.gray('  ----------------'));
           if (localFile.table) {
            console.log(chalk.dim(`  (Using table: ${localFile.table})`));
          }
          await this.db.executeMigration(localFile.downSQL, localFile.querySettings);
        } catch (error: any) {
          console.error(chalk.bold.red(`  ⚠️ Error executing downSQL for migration ${dbMigration.version}:`), error.message);
          console.error(chalk.bold.red('  Reset process halted due to error. Some migrations may remain in the database. Manual cleanup might be required.'));
          throw error;
        }
      }
    }

    try {
      console.log(chalk.blue('\nClearing the __clicksuite_migrations table...'));
      await this.db.clearMigrationsTable();
      await this.db.optimizeMigrationTable();
      console.log(chalk.greenBright('\nDatabase migrations have been reset successfully!'));
    } catch (error: any) {
      console.error(chalk.bold.red('⚠️ Error clearing or optimizing migrations table during reset:'), error.message);
      throw error;
    }
  }

  async schemaLoad() {
    console.log(chalk.blue('Loading schema from local migration files into the database (marking as applied without running SQL)...'));
    const localMigrations = await this._getLocalMigrations();
    const dbRecords = await this.db.getAllMigrationRecords();
    const dbMap = new Map<string, MigrationRecord>();
    dbRecords.forEach(rec => dbMap.set(rec.version, rec));

    if (localMigrations.length === 0) {
      console.log(chalk.yellow('No local migration files found to load.'));
      return;
    }

    let loadedCount = 0;
    let skippedCount = 0;

    for (const migration of localMigrations) {
      const existingRecord = dbMap.get(migration.version);
      if (existingRecord && existingRecord.active === 1) {
        console.log(chalk.gray(`Skipping ${migration.version} - ${migration.name}: Already marked as active in DB.`));
        skippedCount++;
        continue;
      }
      
      try {
        await this.db.markMigrationApplied(migration.version);
        console.log(chalk.green(`Loaded ${migration.version} - ${migration.name} into migrations table as APPLIED.`));
        loadedCount++;
      } catch (error: any) {
        console.error(chalk.bold.red(`⚠️ Error loading migration ${migration.version} - ${migration.name} into DB:`), error.message);
      }
    }

    console.log(chalk.greenBright('\nSchema loading process complete.'));
    console.log(chalk.cyan(`  ${loadedCount} migration(s) newly marked as APPLIED.`));
    console.log(chalk.gray(`  ${skippedCount} migration(s) were already APPLIED and skipped.`));
    
    if (loadedCount > 0) {
        try {
            await this.db.optimizeMigrationTable();
        } catch (e) { /* error already logged by optimizeMigrationTable */ }
    }
  }
}