import { Context, MigrationFile, MigrationRecord, MigrationStatus, RawMigrationFileContent } from '../src/types';

describe('Types', () => {
  describe('Context', () => {
    it('should have required properties with URL configuration', () => {
      const context: Context = {
        url: 'http://default@localhost:8123/test_db',
        database: 'test_db',
        migrationsDir: '/path/to/migrations',
        environment: 'test'
      };

      expect(context.url).toBe('http://default@localhost:8123/test_db');
      expect(context.database).toBe('test_db');
      expect(context.migrationsDir).toBe('/path/to/migrations');
      expect(context.environment).toBe('test');
    });

    it('should support optional properties', () => {
      const context: Context = {
        url: 'https://testuser:testpass@clickhouse.example.com:8443/prod_db',
        database: 'prod_db',
        cluster: 'test_cluster',
        migrationsDir: '/migrations',
        nonInteractive: true,
        environment: 'production',
        dryRun: true,
        verbose: true
      };

      expect(context.url).toBe('https://testuser:testpass@clickhouse.example.com:8443/prod_db');
      expect(context.cluster).toBe('test_cluster');
      expect(context.nonInteractive).toBe(true);
      expect(context.dryRun).toBe(true);
      expect(context.verbose).toBe(true);
    });

    it('should handle minimal URL configuration', () => {
      const context: Context = {
        url: 'http://localhost:8123/default',
        database: 'default',
        migrationsDir: '/migrations',
        environment: 'development'
      };

      expect(context.url).toBe('http://localhost:8123/default');
      expect(context.database).toBe('default');
    });
  });

  describe('MigrationFile', () => {
    it('should represent a migration file correctly', () => {
      const migration: MigrationFile = {
        version: '20240101120000',
        name: 'create_users_table',
        filePath: '/migrations/20240101120000_create_users_table.yml',
        table: 'users',
        database: 'analytics_db',
        upSQL: 'CREATE TABLE analytics_db.users (id UInt64, name String) ENGINE = MergeTree() ORDER BY id',
        downSQL: 'DROP TABLE IF EXISTS analytics_db.users',
        querySettings: { max_execution_time: 60 }
      };

      expect(migration.version).toBe('20240101120000');
      expect(migration.name).toBe('create_users_table');
      expect(migration.filePath).toContain('.yml');
      expect(migration.table).toBe('users');
      expect(migration.database).toBe('analytics_db');
      expect(migration.upSQL).toContain('CREATE TABLE');
      expect(migration.upSQL).toContain('analytics_db.users');
      expect(migration.downSQL).toContain('DROP TABLE');
      expect(migration.querySettings).toHaveProperty('max_execution_time');
    });

    it('should support migrations without database field', () => {
      const migration: MigrationFile = {
        version: '20240101120000',
        name: 'create_users_table',
        filePath: '/migrations/20240101120000_create_users_table.yml',
        upSQL: 'CREATE TABLE users (id UInt64) ENGINE = MergeTree() ORDER BY id',
        downSQL: 'DROP TABLE IF EXISTS users'
      };

      expect(migration.database).toBeUndefined();
      expect(migration.table).toBeUndefined();
      expect(migration.upSQL).toContain('CREATE TABLE users');
    });
  });

  describe('MigrationRecord', () => {
    it('should represent a database migration record', () => {
      const record: MigrationRecord = {
        version: '20240101120000',
        active: 1,
        created_at: '2024-01-01T12:00:00Z'
      };

      expect(record.version).toBe('20240101120000');
      expect(record.active).toBe(1);
      expect(record.created_at).toBe('2024-01-01T12:00:00Z');
    });
  });

  describe('MigrationStatus', () => {
    it('should extend MigrationFile with status information', () => {
      const status: MigrationStatus = {
        version: '20240101120000',
        name: 'create_users_table',
        filePath: '/migrations/test.yml',
        state: 'APPLIED',
        appliedAt: '2024-01-01T12:00:00Z'
      };

      expect(status.state).toBe('APPLIED');
      expect(status.appliedAt).toBe('2024-01-01T12:00:00Z');
    });

    it('should support all migration states', () => {
      const states: Array<MigrationStatus['state']> = ['APPLIED', 'PENDING', 'INACTIVE'];
      
      states.forEach(state => {
        const status: MigrationStatus = {
          version: '20240101120000',
          name: 'test_migration',
          filePath: '/test.yml',
          state
        };
        expect(['APPLIED', 'PENDING', 'INACTIVE']).toContain(status.state);
      });
    });
  });

  describe('RawMigrationFileContent', () => {
    it('should support environment-specific configurations with database field', () => {
      const rawContent: RawMigrationFileContent = {
        version: '20240101120000',
        name: 'test_migration',
        table: 'test_table',
        database: 'analytics_db',
        development: {
          up: 'CREATE DATABASE IF NOT EXISTS {database}; CREATE TABLE {database}.{table}',
          down: 'DROP TABLE {database}.{table}'
        },
        production: {
          up: 'CREATE DATABASE IF NOT EXISTS {database} ON CLUSTER prod; CREATE TABLE {database}.{table} ON CLUSTER prod',
          down: 'DROP TABLE {database}.{table} ON CLUSTER prod'
        }
      };

      expect(rawContent.version).toBe('20240101120000');
      expect(rawContent.table).toBe('test_table');
      expect(rawContent.database).toBe('analytics_db');
      expect(rawContent.development).toHaveProperty('up');
      expect(rawContent.production).toHaveProperty('up');
      expect(rawContent.production.up).toContain('ON CLUSTER');
      expect(rawContent.development.up).toContain('{database}');
      expect(rawContent.development.up).toContain('{table}');
    });

    it('should support legacy configurations without database field', () => {
      const rawContent: RawMigrationFileContent = {
        version: '20240101120000',
        name: 'legacy_migration',
        table: 'users',
        development: {
          up: 'CREATE TABLE {table}',
          down: 'DROP TABLE {table}'
        }
      };

      expect(rawContent.database).toBeUndefined();
      expect(rawContent.table).toBe('users');
      expect(rawContent.development.up).toContain('{table}');
    });
  });
});