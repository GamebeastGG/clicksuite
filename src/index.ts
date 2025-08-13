import * as path from 'path';
import { Context } from './types';

// Export public API for programmatic usage
export { Runner } from './runner';
export { Db } from './db';
export * from './types';

export function getContext(argv: { [key: string]: any }): Context {
  const baseUserConfigDir = process.env.CLICKSUITE_MIGRATIONS_DIR || '.';
  const actualMigrationsDir = path.resolve(baseUserConfigDir, 'migrations');
  const environment = process.env.CLICKSUITE_ENVIRONMENT || 'development';
  const migrationsDatabase = process.env.CLICKSUITE_MIGRATIONS_DATABASE || 'default';

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
    migrationsDatabase: argv.migrationsDatabase !== undefined ? argv.migrationsDatabase as string : migrationsDatabase,
  };
}