// Example of using Clicksuite programmatically with TypeScript
import { Runner, Db, Context, MigrationFile, MigrationStatus } from 'clicksuite';

// Create a context for your ClickHouse configuration
const context: Context = {
  url: 'http://default@localhost:8123/my_database',
  database: 'my_database', // Extracted from URL for convenience
  migrationsDir: '/path/to/migrations',
  environment: 'development',
  nonInteractive: false
};

// Alternative: Use the getContext helper function with environment variables
// Make sure to set CLICKHOUSE_URL environment variable:
// process.env.CLICKHOUSE_URL = 'http://default@localhost:8123/my_database';
// const context = getContext({});

async function runMigrations() {
  // Create a runner instance
  const runner = new Runner(context);
  
  try {
    // Initialize the migration system
    await runner.init();
    
    // Check migration status
    await runner.status();
    
    // Run pending migrations
    await runner.migrate();
    
    // Or run specific migration
    await runner.up('20240101120000');
    
    // Generate new migration
    await runner.generate('create_users_table');
    
  } catch (error) {
    console.error('Migration failed:', error);
  }
}

async function directDatabaseAccess() {
  // Create a database instance for direct access
  const db = new Db(context);
  
  try {
    // Test connection
    const pingResult = await db.ping();
    console.log('Connection:', pingResult.success ? 'OK' : 'Failed');
    
    // Get applied migrations
    const appliedMigrations = await db.getAppliedMigrations();
    console.log('Applied migrations:', appliedMigrations);
    
    // Get database tables
    const tables = await db.getDatabaseTables();
    console.log('Tables:', tables.map(t => t.name));
    
  } catch (error) {
    console.error('Database error:', error);
  } finally {
    await db.close();
  }
}

// Example usage
runMigrations().catch(console.error);
directDatabaseAccess().catch(console.error);