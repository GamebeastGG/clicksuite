#!/usr/bin/env node

import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import dotenv from 'dotenv';
import * as fs from 'fs/promises';
import * as path from 'path';
import chalk from 'chalk';
import { Runner } from './runner';
import { Context } from './types';

// Export public API for programmatic usage
export { Runner } from './runner';
export { Db } from './db';
export * from './types';

// Load environment variables from .env file
dotenv.config();

export function getContext(argv: { [key: string]: any }): Context {
  const baseUserConfigDir = process.env.CLICKSUITE_MIGRATIONS_DIR || '.';
  const actualMigrationsDir = path.resolve(baseUserConfigDir, 'migrations');
  const environment = process.env.CLICKSUITE_ENVIRONMENT || 'development';

  // Require CLICKHOUSE_URL
  if (!process.env.CLICKHOUSE_URL) {
    throw new Error('CLICKHOUSE_URL environment variable is required. Expected format: http://username:password@host:port/database');
  }

  // Parse database from URL for convenience
  let database: string;
  try {
    const urlObj = new URL(process.env.CLICKHOUSE_URL);
    database = urlObj.pathname.replace('/', '') || 'default';
    
    // Basic URL validation
    if (!urlObj.protocol || !urlObj.hostname) {
      throw new Error('Invalid URL format');
    }
  } catch (error) {
    throw new Error('Invalid CLICKHOUSE_URL format. Expected: http://username:password@host:port/database');
  }

  // Handle cluster configuration
  const cluster = process.env.CLICKHOUSE_CLUSTER;
  const clusterValue = cluster && cluster.trim() !== '' ? cluster : undefined;

  return {
    url: process.env.CLICKHOUSE_URL,
    database,
    cluster: clusterValue,
    migrationsDir: actualMigrationsDir,
    nonInteractive: argv.nonInteractive !== undefined ? argv.nonInteractive as boolean : !!process.env.CI,
    environment: environment,
    dryRun: argv.dryRun !== undefined ? argv.dryRun as boolean : false,
    verbose: argv.verbose !== undefined ? argv.verbose as boolean : false,
  };
}

yargs(hideBin(process.argv))
  .option('non-interactive', {
    alias: 'y',
    type: 'boolean',
    description: 'Run in non-interactive mode (confirming actions automatically)',
    default: false,
  })
  .option('verbose', {
    type: 'boolean',
    description: 'Show detailed SQL logs and verbose output',
    default: false,
  })
  .command(
    'init',
    'Initialize Clicksuite for the current project',
    async (argv) => {
      const context = getContext(argv);
      try {
        console.log(chalk.blue(`ℹ️ Clicksuite base configuration directory: ${path.resolve(process.env.CLICKSUITE_MIGRATIONS_DIR || '.')}`));
        console.log(chalk.blue(`⏳ Ensuring actual migrations (.yml files) directory exists at: ${context.migrationsDir}`));
        await fs.mkdir(context.migrationsDir, { recursive: true });
        console.log(chalk.green(`✅ Migrations directory for .yml files is ready at: ${context.migrationsDir}`));
        
        const runner = new Runner(context);
        await runner.init(); // Runner's init handles DB table creation and connection test
      } catch (error: any) {
        console.error(chalk.bold.red('❌ Initialization failed:'), error.message);
        if (error.stack && !context.nonInteractive) console.error(chalk.gray(error.stack));
        process.exit(1);
      }
    }
  )
  .command(
    'generate <name>',
    'Generate a new migration file',
    (yargsInstance) => {
      return yargsInstance.positional('name', {
        describe: 'Name of the migration (e.g., \'create_users_table\')',
        type: 'string',
        demandOption: true,
      });
    },
    async (argv) => {
      const context = getContext(argv);
      const runner = new Runner(context);
      try {
        await runner.generate(argv.name as string);
      } catch (error: any) {
        console.error(chalk.bold.red('❌ Migration generation failed:'), error.message);
        if (error.stack && !context.nonInteractive) console.error(chalk.gray(error.stack));
        process.exit(1);
      }
    }
  )
  .command(
    'migrate:status',
    'Show the status of all migrations',
    async (argv) => {
      const context = getContext(argv);
      const runner = new Runner(context);
      try {
        await runner.status();
      } catch (error: any) {
        console.error(chalk.bold.red('❌ Failed to get migration status:'), error.message);
        if (error.stack && !context.nonInteractive) console.error(chalk.gray(error.stack));
        process.exit(1);
      }
    }
  )
  .command(
    'migrate',
    'Run all pending migrations (equivalent to migrate:up)',
    async (argv) => {
      const context = getContext(argv);
      const runner = new Runner(context);
      try {
        await runner.migrate();
      } catch (error: any) {
        console.error(chalk.bold.red('❌ Migration run failed:'), error.message);
        if (error.stack && !context.nonInteractive) console.error(chalk.gray(error.stack));
        process.exit(1);
      }
    }
  )
  .command(
    'migrate:up [migrationVersion]',
    'Run pending migrations. If no version is specified, runs all pending.',
    (yargsInstance) => {
      return yargsInstance
        .positional('migrationVersion', {
          describe: 'Optional: Target migration version to run up to (inclusive).',
          type: 'string',
        })
        .option('dry-run', {
          describe: 'Preview migrations without executing them',
          type: 'boolean',
          default: false,
        });
    },
    async (argv) => {
      const context = getContext(argv);
      const runner = new Runner(context);
      try {
        await runner.up(argv.migrationVersion as string | undefined);
      } catch (error: any) {
        console.error(chalk.bold.red('❌ Migrate UP failed:'), error.message);
        if (error.stack && !context.nonInteractive) console.error(chalk.gray(error.stack));
        process.exit(1);
      }
    }
  )
  .command(
    'migrate:down [migrationVersion]',
    'Roll back migrations. If no version, rolls back the last applied. Otherwise, rolls back the specified version.',
    (yargsInstance) => {
      return yargsInstance
        .positional('migrationVersion', {
          describe: 'Optional: The migration version to roll back (e.g., \'20230101120000\'). If omitted, rolls back the last applied migration.',
          type: 'string',
        })
        .option('dry-run', {
          describe: 'Preview rollbacks without executing them',
          type: 'boolean',
          default: false,
        });
    },
    async (argv) => {
      const context = getContext(argv);
      const runner = new Runner(context);
      try {
        await runner.down(argv.migrationVersion as string | undefined);
      } catch (error: any) {
        console.error(chalk.bold.red('❌ Migrate DOWN failed:'), error.message);
        if (error.stack && !context.nonInteractive) console.error(chalk.gray(error.stack));
        process.exit(1);
      }
    }
  )
  .command(
    'migrate:reset',
    'Roll back all applied migrations and clear the migrations table (requires confirmation)',
    async (argv) => {
      const context = getContext(argv);
      const runner = new Runner(context);
      try {
        await runner.reset();
      } catch (error: any) {
        console.error(chalk.bold.red('❌ Migration reset failed:'), error.message);
        if (error.stack && !context.nonInteractive) console.error(chalk.gray(error.stack));
        process.exit(1);
      }
    }
  )
  .command(
    'schema:load',
    'Load all local migrations into the database as APPLIED without running their SQL scripts',
    async (argv) => {
      const context = getContext(argv);
      const runner = new Runner(context);
      try {
        await runner.schemaLoad();
      } catch (error: any) {
        console.error(chalk.bold.red('❌ Schema loading failed:'), error.message);
        if (error.stack && !context.nonInteractive) console.error(chalk.gray(error.stack));
        process.exit(1);
      }
    }
  )
  .strict()
  .demandCommand(1, chalk.yellow('⚠️ Please specify a command. Use --help for available commands.'))
  .alias('h', 'help')
  .alias('v', 'version')
  .epilogue(chalk.gray('For more information, find the documentation at https://github.com/GamebeastGG/clicksuite'))
  .fail((msg, err, yargsInstance) => {
    if (err && err.message && !err.message.startsWith('⚠️')) {
        console.error(chalk.bold.red('❌ Error:'), err.message);
        if (err.stack && !getContext({}).nonInteractive) console.error(chalk.gray(err.stack)); 
    } else if (msg && !err) {
      console.error(chalk.red(`❌ ${msg}`));
      yargsInstance.showHelp();
    }
    process.exit(1);
  })
  .parse();