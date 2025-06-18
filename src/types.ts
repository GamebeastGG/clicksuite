export interface Context {
  url: string; // Full ClickHouse URL (e.g., 'http://username:password@localhost:8123/database')
  cluster?: string; // Optional: Name of the ClickHouse cluster
  migrationsDir: string; // Absolute path to the migrations directory (e.g., /path/to/project/migrations)
  nonInteractive?: boolean; // For future use in confirming actions
  environment: string; // e.g., 'development', 'production', 'test'
  dryRun?: boolean; // For previewing migrations without executing them
  readonly database: string; // Extracted from URL for convenience
}

// Represents the raw structure of a parsed YAML migration file
export interface RawMigrationFileContent {
  version: string;
  name: string;
  table?: string; // Optional: table name for SQL formatting
  database?: string; // Optional: database name for SQL targeting
  [env: string]: any; // For development, production, test sections, including aliases
}

// Represents a migration file with SQL resolved for the current context's environment
export interface MigrationFile {
  version: string;    // Timestamp-based version from filename (e.g., "20230101120000")
  name: string;       // Descriptive name from filename (e.g., "create_users_table")
  filePath: string;   // Full path to the .yml migration file
  table?: string;     // Table name from the YAML, if provided
  database?: string;  // Database name from the YAML, if provided
  upSQL?: string;     // SQL for applying the migration in the current environment
  downSQL?: string;   // SQL for rolling back the migration in the current environment
  querySettings?: Record<string, any>; // ClickHouse settings for this migration in the current environment
}

// Represents a row from the __clicksuite_migrations table
export interface MigrationRecord {
  version: string;
  active: number; // 0 for rolled back/inactive, 1 for active
  created_at: string; // ISO date string or ClickHouse DateTime string
}

export type MigrationState = 'APPLIED' | 'PENDING' | 'INACTIVE'; // INACTIVE means present in DB but active=0

export interface MigrationStatus extends MigrationFile {
  state: MigrationState;
  appliedAt?: string; // From MigrationRecord.created_at if applied or inactive
}