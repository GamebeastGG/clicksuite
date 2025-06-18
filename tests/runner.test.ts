import { Runner } from '../src/runner';
import { Db } from '../src/db';
import { Context } from '../src/types';
import * as fs from 'fs/promises';
import * as path from 'path';
import yaml from 'js-yaml';
import inquirer from 'inquirer';

jest.mock('../src/db');
jest.mock('fs/promises');
jest.mock('js-yaml');
jest.mock('inquirer');

const mockFs = fs as jest.Mocked<typeof fs>;
const mockYaml = yaml as jest.Mocked<typeof yaml>;
const mockInquirer = inquirer as jest.Mocked<typeof inquirer>;

describe('Runner', () => {
  let runner: Runner;
  let mockDb: jest.Mocked<Db>;
  let context: Context;

  beforeEach(() => {
    context = {
      url: 'http://default@localhost:8123/test_db',
      database: 'test_db',
      migrationsDir: '/tmp/migrations',
      environment: 'test',
      nonInteractive: false
    };

    mockDb = {
      ping: jest.fn(),
      initMigrationsTable: jest.fn(),
      getAppliedMigrations: jest.fn(),
      getAllMigrationRecords: jest.fn(),
      executeMigration: jest.fn(),
      markMigrationApplied: jest.fn(),
      markMigrationRolledBack: jest.fn(),
      getLatestMigration: jest.fn(),
      getDatabaseTables: jest.fn(),
      getDatabaseMaterializedViews: jest.fn(),
      getDatabaseDictionaries: jest.fn(),
      getDatabaseTablesForDb: jest.fn(),
      getDatabaseMaterializedViewsForDb: jest.fn(),
      getDatabaseDictionariesForDb: jest.fn(),
      getCreateTableQuery: jest.fn(),
      getCreateTableQueryForDb: jest.fn(),
      getDatabaseSchema: jest.fn(),
      clearMigrationsTable: jest.fn(),
      optimizeMigrationTable: jest.fn(),
      close: jest.fn(),
    } as any;

    (Db as jest.MockedClass<typeof Db>).mockImplementation(() => mockDb);

    runner = new Runner(context);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('constructor', () => {
    it('should create Runner with absolute migrations directory', () => {
      const relativeContext = { ...context, migrationsDir: 'migrations' };
      const relativeRunner = new Runner(relativeContext);
      
      expect(relativeRunner['context'].migrationsDir).toContain(process.cwd());
    });

    it('should keep absolute migrations directory unchanged', () => {
      expect(runner['context'].migrationsDir).toBe('/tmp/migrations');
    });
  });

  describe('init', () => {
    it('should initialize migrations table and test connection', async () => {
      mockDb.initMigrationsTable.mockResolvedValue(undefined);
      mockDb.ping.mockResolvedValue({ success: true });

      await runner.init();

      expect(mockDb.initMigrationsTable).toHaveBeenCalled();
      expect(mockDb.ping).toHaveBeenCalled();
    });

    it('should handle initialization errors', async () => {
      const error = new Error('Init failed');
      mockDb.initMigrationsTable.mockRejectedValue(error);

      await expect(runner.init()).rejects.toThrow('Init failed');
    });

    it('should handle ping failure', async () => {
      mockDb.initMigrationsTable.mockResolvedValue(undefined);
      mockDb.ping.mockResolvedValue({ success: false, error: new Error('Connection failed') });

      await runner.init();

      expect(mockDb.ping).toHaveBeenCalled();
    });
  });

  describe('generate', () => {
    beforeEach(() => {
      jest.spyOn(Date.prototype, 'toISOString').mockReturnValue('2024-01-01T12:00:00.000Z');
    });

    it('should generate migration file with correct format', async () => {
      const migrationName = 'create users table';
      mockFs.mkdir.mockResolvedValue(undefined);
      mockFs.writeFile.mockResolvedValue(undefined);

      await runner.generate(migrationName);

      const expectedFilename = '20240101120000_create-users-table.yml';
      const expectedPath = path.join('/tmp/migrations', expectedFilename);

      expect(mockFs.mkdir).toHaveBeenCalledWith('/tmp/migrations', { recursive: true });
      expect(mockFs.writeFile).toHaveBeenCalledWith(
        expectedPath,
        expect.stringContaining('version: "20240101120000"')
      );
      expect(mockFs.writeFile).toHaveBeenCalledWith(
        expectedPath,
        expect.stringContaining('name: "create users table"')
      );
    });

    it('should sanitize migration name for filename', async () => {
      const migrationName = 'Create User$@ T@ble!';
      mockFs.mkdir.mockResolvedValue(undefined);
      mockFs.writeFile.mockResolvedValue(undefined);

      await runner.generate(migrationName);

      const call = mockFs.writeFile.mock.calls[0];
      const filePath = call[0] as string;
      
      expect(filePath).toContain('create-user-tble');
      expect(filePath).not.toContain('$@!');
    });

    it('should handle file generation errors', async () => {
      const error = new Error('Write failed');
      mockFs.mkdir.mockResolvedValue(undefined);
      mockFs.writeFile.mockRejectedValue(error);

      await runner.generate('test migration');

      expect(mockFs.writeFile).toHaveBeenCalled();
    });
  });

  describe('_getLocalMigrations', () => {
    it('should read and parse migration files', async () => {
      const mockFiles = ['20240101120000_create_users.yml', '20240102120000_add_index.yml'];
      const mockYamlContent = {
        version: '20240101120000',
        name: 'create_users',
        table: 'users',
        test: {
          up: 'CREATE TABLE {table}',
          down: 'DROP TABLE {table}',
          settings: { max_execution_time: 60 }
        }
      };

      mockFs.readdir.mockResolvedValue(mockFiles as any);
      mockFs.readFile.mockResolvedValue('yaml content');
      mockYaml.load.mockReturnValue(mockYamlContent);

      const migrations = await runner['_getLocalMigrations']();

      expect(migrations).toHaveLength(2);
      expect(migrations[0]).toMatchObject({
        version: '20240101120000',
        name: 'create_users',
        upSQL: 'CREATE TABLE users',
        downSQL: 'DROP TABLE users',
        table: 'users'
      });
    });

    it('should handle directory not found', async () => {
      const error = new Error('ENOENT');
      (error as any).code = 'ENOENT';
      mockFs.readdir.mockRejectedValue(error);

      const migrations = await runner['_getLocalMigrations']();

      expect(migrations).toEqual([]);
    });

    it('should skip invalid migration files', async () => {
      const mockFiles = ['invalid_file.txt', '20240101120000_valid.yml'];
      mockFs.readdir.mockResolvedValue(mockFiles as any);
      mockFs.readFile.mockResolvedValue('yaml content');
      mockYaml.load.mockReturnValue({
        version: '20240101120000',
        name: 'valid',
        test: { up: 'CREATE TABLE test', down: 'DROP TABLE test' }
      });

      const migrations = await runner['_getLocalMigrations']();

      expect(migrations).toHaveLength(1);
      expect(migrations[0].version).toBe('20240101120000');
    });

    it('should handle YAML parsing errors', async () => {
      const mockFiles = ['20240101120000_invalid.yml'];
      mockFs.readdir.mockResolvedValue(mockFiles as any);
      mockFs.readFile.mockResolvedValue('invalid yaml');
      const yamlError = new Error('YAML parsing failed');
      (yamlError as any).mark = { line: 5, column: 10 };
      mockYaml.load.mockImplementation(() => { throw yamlError; });

      const migrations = await runner['_getLocalMigrations']();

      expect(migrations).toEqual([]);
    });

    it('should use development as fallback environment', async () => {
      const mockFiles = ['20240101120000_test.yml'];
      const mockYamlContent = {
        version: '20240101120000',
        name: 'test',
        development: {
          up: 'CREATE TABLE test',
          down: 'DROP TABLE test'
        }
      };

      mockFs.readdir.mockResolvedValue(mockFiles as any);
      mockFs.readFile.mockResolvedValue('yaml content');
      mockYaml.load.mockReturnValue(mockYamlContent);

      const migrations = await runner['_getLocalMigrations']();

      expect(migrations[0].upSQL).toBe('CREATE TABLE test');
      expect(migrations[0].downSQL).toBe('DROP TABLE test');
    });
  });

  describe('status', () => {
    it('should display migration status correctly', async () => {
      const mockLocalMigrations = [
        {
          version: '20240101120000',
          name: 'create_users',
          filePath: '/tmp/migrations/20240101120000_create_users.yml',
          upSQL: 'CREATE TABLE users',
          downSQL: 'DROP TABLE users'
        }
      ];

      const mockDbRecords = [
        { version: '20240101120000', active: 1, created_at: '2024-01-01T12:00:00Z' }
      ];

      jest.spyOn(runner as any, '_getLocalMigrations').mockResolvedValue(mockLocalMigrations);
      mockDb.getAllMigrationRecords.mockResolvedValue(mockDbRecords);

      await runner.status();

      expect(runner['_getLocalMigrations']).toHaveBeenCalled();
      expect(mockDb.getAllMigrationRecords).toHaveBeenCalled();
    });

    it('should handle empty migrations', async () => {
      jest.spyOn(runner as any, '_getLocalMigrations').mockResolvedValue([]);
      mockDb.getAllMigrationRecords.mockResolvedValue([]);

      await runner.status();

      expect(runner['_getLocalMigrations']).toHaveBeenCalled();
    });
  });

  describe('up', () => {
    it('should apply pending migrations', async () => {
      const mockLocalMigrations = [
        {
          version: '20240101120000',
          name: 'create_users',
          filePath: '/tmp/migrations/test.yml',
          upSQL: 'CREATE TABLE users',
          downSQL: 'DROP TABLE users'
        }
      ];

      const mockAppliedMigrations: any[] = [];

      jest.spyOn(runner as any, '_getLocalMigrations').mockResolvedValue(mockLocalMigrations);
      mockDb.getAppliedMigrations.mockResolvedValue(mockAppliedMigrations);
      mockDb.executeMigration.mockResolvedValue(undefined);
      mockDb.markMigrationApplied.mockResolvedValue(undefined);

      await runner.up();

      expect(mockDb.executeMigration).toHaveBeenCalledWith('CREATE TABLE users', undefined);
      expect(mockDb.markMigrationApplied).toHaveBeenCalledWith('20240101120000');
    });

    it('should skip already applied migrations', async () => {
      const mockLocalMigrations = [
        {
          version: '20240101120000',
          name: 'create_users',
          filePath: '/tmp/migrations/test.yml',
          upSQL: 'CREATE TABLE users'
        }
      ];

      const mockAppliedMigrations = [
        { version: '20240101120000', active: 1, created_at: '2024-01-01T12:00:00Z' }
      ];

      jest.spyOn(runner as any, '_getLocalMigrations').mockResolvedValue(mockLocalMigrations);
      mockDb.getAppliedMigrations.mockResolvedValue(mockAppliedMigrations);

      await runner.up();

      expect(mockDb.executeMigration).not.toHaveBeenCalled();
      expect(mockDb.markMigrationApplied).not.toHaveBeenCalled();
    });

    it('should apply migrations up to target version', async () => {
      const mockLocalMigrations = [
        {
          version: '20240101120000',
          name: 'first',
          filePath: '/tmp/migrations/first.yml',
          upSQL: 'CREATE TABLE first'
        },
        {
          version: '20240102120000',
          name: 'second',
          filePath: '/tmp/migrations/second.yml',
          upSQL: 'CREATE TABLE second'
        }
      ];

      jest.spyOn(runner as any, '_getLocalMigrations').mockResolvedValue(mockLocalMigrations);
      mockDb.getAppliedMigrations.mockResolvedValue([]);
      mockDb.executeMigration.mockResolvedValue(undefined);
      mockDb.markMigrationApplied.mockResolvedValue(undefined);

      await runner.up('20240101120000');

      expect(mockDb.executeMigration).toHaveBeenCalledTimes(1);
      expect(mockDb.executeMigration).toHaveBeenCalledWith('CREATE TABLE first', undefined);
    });

    it('should handle migration execution errors', async () => {
      const mockLocalMigrations = [
        {
          version: '20240101120000',
          name: 'create_users',
          filePath: '/tmp/migrations/test.yml',
          upSQL: 'INVALID SQL'
        }
      ];

      jest.spyOn(runner as any, '_getLocalMigrations').mockResolvedValue(mockLocalMigrations);
      mockDb.getAppliedMigrations.mockResolvedValue([]);
      const executionError = new Error('SQL syntax error');
      mockDb.executeMigration.mockRejectedValue(executionError);

      await expect(runner.up()).rejects.toThrow('SQL syntax error');
    });

    it('should skip migrations without upSQL', async () => {
      const mockLocalMigrations = [
        {
          version: '20240101120000',
          name: 'create_users',
          filePath: '/tmp/migrations/test.yml',
          upSQL: undefined
        }
      ];

      jest.spyOn(runner as any, '_getLocalMigrations').mockResolvedValue(mockLocalMigrations);
      mockDb.getAppliedMigrations.mockResolvedValue([]);

      await runner.up();

      expect(mockDb.executeMigration).not.toHaveBeenCalled();
      expect(mockDb.markMigrationApplied).not.toHaveBeenCalled();
    });
  });

  describe('down', () => {
    it('should rollback last migration when no target specified', async () => {
      const mockLocalMigrations = [
        {
          version: '20240101120000',
          name: 'create_users',
          filePath: '/tmp/migrations/test.yml',
          upSQL: 'CREATE TABLE users',
          downSQL: 'DROP TABLE users'
        }
      ];

      const mockAppliedMigrations = [
        { version: '20240101120000', active: 1, created_at: '2024-01-01T12:00:00Z' }
      ];

      jest.spyOn(runner as any, '_getLocalMigrations').mockResolvedValue(mockLocalMigrations);
      mockDb.getAppliedMigrations.mockResolvedValue(mockAppliedMigrations);
      mockDb.executeMigration.mockResolvedValue(undefined);
      mockDb.markMigrationRolledBack.mockResolvedValue(undefined);
      mockInquirer.prompt.mockResolvedValue({ confirmation: true });

      await runner.down();

      expect(mockDb.executeMigration).toHaveBeenCalledWith('DROP TABLE users', undefined);
      expect(mockDb.markMigrationRolledBack).toHaveBeenCalledWith('20240101120000');
    });

    it('should rollback to target version', async () => {
      const mockLocalMigrations = [
        {
          version: '20240101120000',
          name: 'first',
          filePath: '/tmp/migrations/first.yml',
          downSQL: 'DROP TABLE first'
        },
        {
          version: '20240102120000',
          name: 'second',
          filePath: '/tmp/migrations/second.yml',
          downSQL: 'DROP TABLE second'
        }
      ];

      const mockAppliedMigrations = [
        { version: '20240101120000', active: 1, created_at: '2024-01-01T12:00:00Z' },
        { version: '20240102120000', active: 1, created_at: '2024-01-02T12:00:00Z' }
      ];

      jest.spyOn(runner as any, '_getLocalMigrations').mockResolvedValue(mockLocalMigrations);
      mockDb.getAppliedMigrations.mockResolvedValue(mockAppliedMigrations);
      mockDb.executeMigration.mockResolvedValue(undefined);
      mockDb.markMigrationRolledBack.mockResolvedValue(undefined);
      mockInquirer.prompt.mockResolvedValue({ confirmation: true });

      await runner.down('20240101120000');

      expect(mockDb.executeMigration).toHaveBeenCalledTimes(1);
      expect(mockDb.executeMigration).toHaveBeenCalledWith('DROP TABLE second', undefined);
    });

    it('should handle no applied migrations', async () => {
      jest.spyOn(runner as any, '_getLocalMigrations').mockResolvedValue([]);
      mockDb.getAppliedMigrations.mockResolvedValue([]);

      await runner.down();

      expect(mockDb.executeMigration).not.toHaveBeenCalled();
    });

    it('should skip confirmation in non-interactive mode', async () => {
      const nonInteractiveContext = { ...context, nonInteractive: true };
      const nonInteractiveRunner = new Runner(nonInteractiveContext);

      const mockLocalMigrations = [
        {
          version: '20240101120000',
          name: 'create_users',
          filePath: '/tmp/migrations/test.yml',
          downSQL: 'DROP TABLE users'
        }
      ];

      const mockAppliedMigrations = [
        { version: '20240101120000', active: 1, created_at: '2024-01-01T12:00:00Z' }
      ];

      jest.spyOn(nonInteractiveRunner as any, '_getLocalMigrations').mockResolvedValue(mockLocalMigrations);
      mockDb.getAppliedMigrations.mockResolvedValue(mockAppliedMigrations);
      mockDb.executeMigration.mockResolvedValue(undefined);
      mockDb.markMigrationRolledBack.mockResolvedValue(undefined);

      await nonInteractiveRunner.down();

      expect(mockInquirer.prompt).not.toHaveBeenCalled();
      expect(mockDb.executeMigration).toHaveBeenCalled();
    });

    it('should cancel rollback when user declines confirmation', async () => {
      const mockLocalMigrations = [
        {
          version: '20240101120000',
          name: 'create_users',
          filePath: '/tmp/migrations/test.yml',
          downSQL: 'DROP TABLE users'
        }
      ];

      const mockAppliedMigrations = [
        { version: '20240101120000', active: 1, created_at: '2024-01-01T12:00:00Z' }
      ];

      jest.spyOn(runner as any, '_getLocalMigrations').mockResolvedValue(mockLocalMigrations);
      mockDb.getAppliedMigrations.mockResolvedValue(mockAppliedMigrations);
      mockInquirer.prompt.mockResolvedValue({ confirmation: false });

      await runner.down();

      expect(mockDb.executeMigration).not.toHaveBeenCalled();
      expect(mockDb.markMigrationRolledBack).not.toHaveBeenCalled();
    });
  });

  describe('reset', () => {
    it('should reset all migrations with confirmation', async () => {
      const mockLocalMigrations = [
        {
          version: '20240101120000',
          name: 'create_users',
          filePath: '/tmp/migrations/test.yml',
          downSQL: 'DROP TABLE users'
        }
      ];

      const mockAppliedMigrations = [
        { version: '20240101120000', active: 1, created_at: '2024-01-01T12:00:00Z' }
      ];

      jest.spyOn(runner as any, '_getLocalMigrations').mockResolvedValue(mockLocalMigrations);
      mockDb.getAppliedMigrations.mockResolvedValue(mockAppliedMigrations);
      mockDb.executeMigration.mockResolvedValue(undefined);
      mockDb.clearMigrationsTable.mockResolvedValue(undefined);
      mockDb.optimizeMigrationTable.mockResolvedValue(undefined);
      mockInquirer.prompt.mockResolvedValue({ confirmation: true });

      await runner.reset();

      expect(mockDb.executeMigration).toHaveBeenCalledWith('DROP TABLE users', undefined);
      expect(mockDb.clearMigrationsTable).toHaveBeenCalled();
      expect(mockDb.optimizeMigrationTable).toHaveBeenCalled();
    });

    it('should skip reset when user cancels', async () => {
      mockInquirer.prompt.mockResolvedValue({ confirmation: false });

      await runner.reset();

      expect(mockDb.executeMigration).not.toHaveBeenCalled();
      expect(mockDb.clearMigrationsTable).not.toHaveBeenCalled();
    });

    it('should reset in non-interactive mode', async () => {
      const nonInteractiveContext = { ...context, nonInteractive: true };
      const nonInteractiveRunner = new Runner(nonInteractiveContext);

      jest.spyOn(nonInteractiveRunner as any, '_getLocalMigrations').mockResolvedValue([]);
      mockDb.getAppliedMigrations.mockResolvedValue([]);
      mockDb.clearMigrationsTable.mockResolvedValue(undefined);
      mockDb.optimizeMigrationTable.mockResolvedValue(undefined);

      await nonInteractiveRunner.reset();

      expect(mockInquirer.prompt).not.toHaveBeenCalled();
      expect(mockDb.clearMigrationsTable).toHaveBeenCalled();
    });
  });

  describe('schemaLoad', () => {
    it('should mark local migrations as applied without running SQL', async () => {
      const mockLocalMigrations = [
        {
          version: '20240101120000',
          name: 'create_users',
          filePath: '/tmp/migrations/test.yml'
        },
        {
          version: '20240102120000',
          name: 'add_index',
          filePath: '/tmp/migrations/test2.yml'
        }
      ];

      const mockDbRecords: any[] = [];

      jest.spyOn(runner as any, '_getLocalMigrations').mockResolvedValue(mockLocalMigrations);
      mockDb.getAllMigrationRecords.mockResolvedValue(mockDbRecords);
      mockDb.markMigrationApplied.mockResolvedValue(undefined);
      mockDb.optimizeMigrationTable.mockResolvedValue(undefined);

      await runner.schemaLoad();

      expect(mockDb.markMigrationApplied).toHaveBeenCalledTimes(2);
      expect(mockDb.markMigrationApplied).toHaveBeenCalledWith('20240101120000');
      expect(mockDb.markMigrationApplied).toHaveBeenCalledWith('20240102120000');
      expect(mockDb.optimizeMigrationTable).toHaveBeenCalled();
    });

    it('should skip already active migrations', async () => {
      const mockLocalMigrations = [
        {
          version: '20240101120000',
          name: 'create_users',
          filePath: '/tmp/migrations/test.yml'
        }
      ];

      const mockDbRecords = [
        { version: '20240101120000', active: 1, created_at: '2024-01-01T12:00:00Z' }
      ];

      jest.spyOn(runner as any, '_getLocalMigrations').mockResolvedValue(mockLocalMigrations);
      mockDb.getAllMigrationRecords.mockResolvedValue(mockDbRecords);

      await runner.schemaLoad();

      expect(mockDb.markMigrationApplied).not.toHaveBeenCalled();
    });

    it('should handle empty local migrations', async () => {
      jest.spyOn(runner as any, '_getLocalMigrations').mockResolvedValue([]);
      mockDb.getAllMigrationRecords.mockResolvedValue([]);

      await runner.schemaLoad();

      expect(mockDb.markMigrationApplied).not.toHaveBeenCalled();
    });
  });

  describe('migrate', () => {
    it('should call up method', async () => {
      const upSpy = jest.spyOn(runner, 'up').mockResolvedValue(undefined);

      await runner.migrate();

      expect(upSpy).toHaveBeenCalled();
    });
  });

  describe('_updateSchemaFile', () => {
    beforeEach(() => {
      mockFs.writeFile.mockResolvedValue(undefined);
    });

    it('should generate schema file with tables, views, and dictionaries', async () => {
      const mockTables = [{ name: 'users' }, { name: 'orders' }];
      const mockViews = [{ name: 'user_stats_mv' }];
      const mockDictionaries = [{ name: 'countries_dict' }];

      // Mock migrations to include a migration targeting a different database
      const mockMigrations = [
        { version: '123', name: 'test', database: 'gamebeast', filePath: '/test', upSQL: 'CREATE TABLE test', downSQL: 'DROP TABLE test' }
      ];
      jest.spyOn(runner as any, '_getLocalMigrations').mockResolvedValue(mockMigrations);

      mockDb.getDatabaseTablesForDb.mockResolvedValue(mockTables);
      mockDb.getDatabaseMaterializedViewsForDb.mockResolvedValue(mockViews);
      mockDb.getDatabaseDictionariesForDb.mockResolvedValue(mockDictionaries);
      mockDb.getCreateTableQueryForDb
        .mockResolvedValueOnce('CREATE TABLE test_db.users (id UInt32) ENGINE = MergeTree() ORDER BY id')
        .mockResolvedValueOnce('CREATE TABLE test_db.orders (id UInt32) ENGINE = MergeTree() ORDER BY id')
        .mockResolvedValueOnce('CREATE MATERIALIZED VIEW test_db.user_stats_mv AS SELECT count() FROM users')
        .mockResolvedValueOnce('CREATE DICTIONARY test_db.countries_dict (id UInt32, name String) PRIMARY KEY id');

      await runner['_updateSchemaFile']();

      expect(mockDb.getDatabaseTablesForDb).toHaveBeenCalledWith('test_db');
      expect(mockDb.getDatabaseTablesForDb).toHaveBeenCalledWith('gamebeast');
      expect(mockDb.getDatabaseMaterializedViewsForDb).toHaveBeenCalledWith('test_db');
      expect(mockDb.getDatabaseMaterializedViewsForDb).toHaveBeenCalledWith('gamebeast');
      expect(mockDb.getDatabaseDictionariesForDb).toHaveBeenCalledWith('test_db');
      expect(mockDb.getDatabaseDictionariesForDb).toHaveBeenCalledWith('gamebeast');
      expect(mockDb.getCreateTableQueryForDb).toHaveBeenCalledWith('users', 'test_db', 'TABLE');
      expect(mockDb.getCreateTableQueryForDb).toHaveBeenCalledWith('orders', 'test_db', 'TABLE');
      expect(mockDb.getCreateTableQueryForDb).toHaveBeenCalledWith('user_stats_mv', 'test_db', 'VIEW');
      expect(mockDb.getCreateTableQueryForDb).toHaveBeenCalledWith('countries_dict', 'test_db', 'DICTIONARY');

      const writeCall = mockFs.writeFile.mock.calls[0];
      const schemaPath = writeCall[0] as string;
      const schemaContent = writeCall[1] as string;

      expect(schemaPath).toBe(path.join('/tmp/migrations', 'schema.sql'));
      expect(schemaContent).toContain('-- Auto-generated schema file');
      expect(schemaContent).toContain('-- Environment: test');
      expect(schemaContent).toContain('-- Databases: test_db, gamebeast');
      expect(schemaContent).toContain('-- Tables');
      expect(schemaContent).toContain('-- Table: test_db.users');
      expect(schemaContent).toContain('CREATE TABLE test_db.users (id UInt32) ENGINE = MergeTree() ORDER BY id;');
      expect(schemaContent).toContain('-- Table: test_db.orders');
      expect(schemaContent).toContain('-- Materialized Views');
      expect(schemaContent).toContain('-- Materialized View: test_db.user_stats_mv');
      expect(schemaContent).toContain('CREATE MATERIALIZED VIEW test_db.user_stats_mv AS SELECT count() FROM users;');
      expect(schemaContent).toContain('-- Dictionaries');
      expect(schemaContent).toContain('-- Dictionary: test_db.countries_dict');
      expect(schemaContent).toContain('CREATE DICTIONARY test_db.countries_dict (id UInt32, name String) PRIMARY KEY id;');
    });

    it('should handle empty database', async () => {
      jest.spyOn(runner as any, '_getLocalMigrations').mockResolvedValue([]);
      mockDb.getDatabaseTablesForDb.mockResolvedValue([]);
      mockDb.getDatabaseMaterializedViewsForDb.mockResolvedValue([]);
      mockDb.getDatabaseDictionariesForDb.mockResolvedValue([]);

      await runner['_updateSchemaFile']();

      const writeCall = mockFs.writeFile.mock.calls[0];
      const schemaContent = writeCall[1] as string;

      expect(schemaContent).toContain('-- Auto-generated schema file');
      expect(schemaContent).not.toContain('-- Table:');
      expect(schemaContent).not.toContain('-- Materialized View:');
      expect(schemaContent).not.toContain('-- Dictionary:');
    });

    it('should handle database query errors gracefully', async () => {
      jest.spyOn(runner as any, '_getLocalMigrations').mockResolvedValue([]);
      mockDb.getDatabaseTablesForDb.mockResolvedValue([{ name: 'users' }]);
      mockDb.getDatabaseMaterializedViewsForDb.mockResolvedValue([]);
      mockDb.getDatabaseDictionariesForDb.mockResolvedValue([]);
      mockDb.getCreateTableQueryForDb.mockRejectedValue(new Error('Permission denied'));

      await runner['_updateSchemaFile']();

      expect(mockFs.writeFile).toHaveBeenCalled();
      const writeCall = mockFs.writeFile.mock.calls[0];
      const schemaContent = writeCall[1] as string;
      expect(schemaContent).toContain('-- Auto-generated schema file');
    });

    it('should handle file write errors gracefully', async () => {
      mockDb.getDatabaseTables.mockResolvedValue([]);
      mockDb.getDatabaseMaterializedViews.mockResolvedValue([]);
      mockDb.getDatabaseDictionaries.mockResolvedValue([]);
      mockFs.writeFile.mockRejectedValue(new Error('Permission denied'));

      await expect(runner['_updateSchemaFile']()).resolves.not.toThrow();
    });

    it('should handle database connection errors gracefully', async () => {
      mockDb.getDatabaseTables.mockRejectedValue(new Error('Connection failed'));

      await expect(runner['_updateSchemaFile']()).resolves.not.toThrow();
    });
  });

  describe('schema file integration', () => {
    beforeEach(() => {
      mockFs.writeFile.mockResolvedValue(undefined);
      mockDb.getDatabaseTables.mockResolvedValue([]);
      mockDb.getDatabaseMaterializedViews.mockResolvedValue([]);
      mockDb.getDatabaseDictionaries.mockResolvedValue([]);
      jest.spyOn(runner as any, '_updateSchemaFile').mockResolvedValue(undefined);
    });

    it('should update schema file after successful migration up', async () => {
      const mockLocalMigrations = [
        {
          version: '20240101120000',
          name: 'create_users',
          filePath: '/tmp/migrations/test.yml',
          upSQL: 'CREATE TABLE users'
        }
      ];

      jest.spyOn(runner as any, '_getLocalMigrations').mockResolvedValue(mockLocalMigrations);
      mockDb.getAppliedMigrations.mockResolvedValue([]);
      mockDb.executeMigration.mockResolvedValue(undefined);
      mockDb.markMigrationApplied.mockResolvedValue(undefined);

      await runner.up();

      expect(runner['_updateSchemaFile']).toHaveBeenCalled();
    });

    it('should update schema file after successful migration down', async () => {
      const mockLocalMigrations = [
        {
          version: '20240101120000',
          name: 'create_users',
          filePath: '/tmp/migrations/test.yml',
          downSQL: 'DROP TABLE users'
        }
      ];

      const mockAppliedMigrations = [
        { version: '20240101120000', active: 1, created_at: '2024-01-01T12:00:00Z' }
      ];

      jest.spyOn(runner as any, '_getLocalMigrations').mockResolvedValue(mockLocalMigrations);
      mockDb.getAppliedMigrations.mockResolvedValue(mockAppliedMigrations);
      mockDb.executeMigration.mockResolvedValue(undefined);
      mockDb.markMigrationRolledBack.mockResolvedValue(undefined);
      mockInquirer.prompt.mockResolvedValue({ confirmation: true });

      await runner.down();

      expect(runner['_updateSchemaFile']).toHaveBeenCalled();
    });

    it('should update schema file after successful reset', async () => {
      jest.spyOn(runner as any, '_getLocalMigrations').mockResolvedValue([]);
      mockDb.getAppliedMigrations.mockResolvedValue([]);
      mockDb.clearMigrationsTable.mockResolvedValue(undefined);
      mockDb.optimizeMigrationTable.mockResolvedValue(undefined);
      mockInquirer.prompt.mockResolvedValue({ confirmation: true });

      await runner.reset();

      expect(runner['_updateSchemaFile']).toHaveBeenCalled();
    });

    it('should update schema file after successful schema load', async () => {
      const mockLocalMigrations = [
        {
          version: '20240101120000',
          name: 'create_users',
          filePath: '/tmp/migrations/test.yml'
        }
      ];

      jest.spyOn(runner as any, '_getLocalMigrations').mockResolvedValue(mockLocalMigrations);
      mockDb.getAllMigrationRecords.mockResolvedValue([]);
      mockDb.markMigrationApplied.mockResolvedValue(undefined);
      mockDb.optimizeMigrationTable.mockResolvedValue(undefined);

      await runner.schemaLoad();

      expect(runner['_updateSchemaFile']).toHaveBeenCalled();
    });

    it('should not update schema file if migration up fails', async () => {
      const mockLocalMigrations = [
        {
          version: '20240101120000',
          name: 'create_users',
          filePath: '/tmp/migrations/test.yml',
          upSQL: 'INVALID SQL'
        }
      ];

      jest.spyOn(runner as any, '_getLocalMigrations').mockResolvedValue(mockLocalMigrations);
      mockDb.getAppliedMigrations.mockResolvedValue([]);
      mockDb.executeMigration.mockRejectedValue(new Error('SQL error'));

      await expect(runner.up()).rejects.toThrow();
      expect(runner['_updateSchemaFile']).not.toHaveBeenCalled();
    });
  });
});