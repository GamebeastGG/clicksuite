import { Context, MigrationFile, MigrationRecord, MigrationStatus, RawMigrationFileContent } from '../src/types';

describe('Types', () => {
  describe('Context', () => {
    it('should have required properties', () => {
      const context: Context = {
        protocol: 'http',
        host: 'localhost',
        port: '8123',
        database: 'test_db',
        migrationsDir: '/path/to/migrations',
        environment: 'test'
      };

      expect(context.protocol).toBe('http');
      expect(context.host).toBe('localhost');
      expect(context.port).toBe('8123');
      expect(context.database).toBe('test_db');
      expect(context.migrationsDir).toBe('/path/to/migrations');
      expect(context.environment).toBe('test');
    });

    it('should support optional properties', () => {
      const context: Context = {
        protocol: 'https',
        host: 'clickhouse.example.com',
        port: '8443',
        username: 'testuser',
        password: 'testpass',
        database: 'prod_db',
        cluster: 'test_cluster',
        migrationsDir: '/migrations',
        nonInteractive: true,
        environment: 'production'
      };

      expect(context.username).toBe('testuser');
      expect(context.password).toBe('testpass');
      expect(context.cluster).toBe('test_cluster');
      expect(context.nonInteractive).toBe(true);
    });
  });

  describe('MigrationFile', () => {
    it('should represent a migration file correctly', () => {
      const migration: MigrationFile = {
        version: '20240101120000',
        name: 'create_users_table',
        filePath: '/migrations/20240101120000_create_users_table.yml',
        table: 'users',
        upSQL: 'CREATE TABLE users (id UInt64, name String) ENGINE = MergeTree() ORDER BY id',
        downSQL: 'DROP TABLE IF EXISTS users',
        querySettings: { max_execution_time: 60 }
      };

      expect(migration.version).toBe('20240101120000');
      expect(migration.name).toBe('create_users_table');
      expect(migration.filePath).toContain('.yml');
      expect(migration.upSQL).toContain('CREATE TABLE');
      expect(migration.downSQL).toContain('DROP TABLE');
      expect(migration.querySettings).toHaveProperty('max_execution_time');
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
    it('should support environment-specific configurations', () => {
      const rawContent: RawMigrationFileContent = {
        version: '20240101120000',
        name: 'test_migration',
        table: 'test_table',
        development: {
          up: 'CREATE TABLE test_table',
          down: 'DROP TABLE test_table'
        },
        production: {
          up: 'CREATE TABLE test_table ON CLUSTER prod',
          down: 'DROP TABLE test_table ON CLUSTER prod'
        }
      };

      expect(rawContent.version).toBe('20240101120000');
      expect(rawContent.development).toHaveProperty('up');
      expect(rawContent.production).toHaveProperty('up');
      expect(rawContent.production.up).toContain('ON CLUSTER');
    });
  });
});