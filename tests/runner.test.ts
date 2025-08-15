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
      migrationsDir: '/tmp/migrations',
      environment: 'test',
      nonInteractive: false,
      verbose: false
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

  describe('dry run functionality', () => {
    let consoleSpy: jest.SpyInstance;

    beforeEach(() => {
      consoleSpy = jest.spyOn(console, 'log').mockImplementation();
    });

    afterEach(() => {
      consoleSpy.mockRestore();
    });

    it('should not execute migrations in dry run mode for up', async () => {
      const dryRunContext = { ...context, dryRun: true };
      const dryRunRunner = new Runner(dryRunContext);

      const mockLocalMigrations = [
        {
          version: '20240101120000',
          name: 'create_users',
          filePath: '/tmp/migrations/test.yml',
          upSQL: 'CREATE TABLE users',
          downSQL: 'DROP TABLE users'
        }
      ];

      jest.spyOn(dryRunRunner as any, '_getLocalMigrations').mockResolvedValue(mockLocalMigrations);
      mockDb.getAppliedMigrations.mockResolvedValue([]);

      await dryRunRunner.up();

      expect(mockDb.executeMigration).not.toHaveBeenCalled();
      expect(mockDb.markMigrationApplied).not.toHaveBeenCalled();
    });

    it('should not execute migrations in dry run mode for down', async () => {
      const dryRunContext = { ...context, dryRun: true };
      const dryRunRunner = new Runner(dryRunContext);

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

      jest.spyOn(dryRunRunner as any, '_getLocalMigrations').mockResolvedValue(mockLocalMigrations);
      mockDb.getAppliedMigrations.mockResolvedValue(mockAppliedMigrations);

      await dryRunRunner.down();

      expect(mockDb.executeMigration).not.toHaveBeenCalled();
      expect(mockDb.markMigrationRolledBack).not.toHaveBeenCalled();
    });

    it('should not update schema file in dry run mode', async () => {
      const dryRunContext = { ...context, dryRun: true };
      const dryRunRunner = new Runner(dryRunContext);

      const mockLocalMigrations = [
        {
          version: '20240101120000',
          name: 'create_users',
          filePath: '/tmp/migrations/test.yml',
          upSQL: 'CREATE TABLE users'
        }
      ];

      jest.spyOn(dryRunRunner as any, '_getLocalMigrations').mockResolvedValue(mockLocalMigrations);
      jest.spyOn(dryRunRunner as any, '_updateSchemaFile').mockResolvedValue(undefined);
      mockDb.getAppliedMigrations.mockResolvedValue([]);

      await dryRunRunner.up();

      expect(dryRunRunner['_updateSchemaFile']).not.toHaveBeenCalled();
    });

    it('should show formatted dry run output for up migrations', async () => {
      const dryRunContext = { ...context, dryRun: true };
      const dryRunRunner = new Runner(dryRunContext);

      const mockLocalMigrations = [
        {
          version: '20240101120000',
          name: 'create_users',
          filePath: '/tmp/migrations/test.yml',
          upSQL: 'CREATE TABLE users; INSERT INTO users VALUES (1)',
          table: 'users',
          database: 'test_db'
        }
      ];

      jest.spyOn(dryRunRunner as any, '_getLocalMigrations').mockResolvedValue(mockLocalMigrations);
      mockDb.getAppliedMigrations.mockResolvedValue([]);

      await dryRunRunner.up();

      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('DRY RUN: The following 1 migration(s) would be applied:')
      );
      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('DRY RUN: Migration 20240101120000 - create_users')
      );
      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('Environment: test')
      );
      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('Database: test_db')
      );
      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('Table: users')
      );
      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('SQL to execute (2 queries):')
      );
      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('CREATE TABLE users;')
      );
      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('INSERT INTO users VALUES (1);')
      );
      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('DRY RUN COMPLETE: 1 migration(s) would be applied (no changes made)')
      );
    });

    it('should show formatted dry run output for down migrations', async () => {
      const dryRunContext = { ...context, dryRun: true };
      const dryRunRunner = new Runner(dryRunContext);

      const mockLocalMigrations = [
        {
          version: '20240101120000',
          name: 'create_users',
          filePath: '/tmp/migrations/test.yml',
          downSQL: 'DROP TABLE users',
          table: 'users',
          database: 'test_db'
        }
      ];

      const mockAppliedMigrations = [
        { version: '20240101120000', active: 1, created_at: '2024-01-01T12:00:00Z' }
      ];

      jest.spyOn(dryRunRunner as any, '_getLocalMigrations').mockResolvedValue(mockLocalMigrations);
      mockDb.getAppliedMigrations.mockResolvedValue(mockAppliedMigrations);

      await dryRunRunner.down();

      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('DRY RUN: The following 1 migration(s) would be rolled back')
      );
      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('DRY RUN: Rolling back 20240101120000 - create_users')
      );
      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('Environment: test')
      );
      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('Database: test_db')
      );
      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('Table: users')
      );
      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('SQL to execute (1 query):')
      );
      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('DROP TABLE users;')
      );
      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('DRY RUN COMPLETE: 1 migration(s) would be rolled back (no changes made)')
      );
    });

    it('should show dry run skip message for migrations without upSQL', async () => {
      const dryRunContext = { ...context, dryRun: true };
      const dryRunRunner = new Runner(dryRunContext);
      const consoleWarnSpy = jest.spyOn(console, 'warn').mockImplementation();

      const mockLocalMigrations = [
        {
          version: '20240101120000',
          name: 'create_users',
          filePath: '/tmp/migrations/test.yml',
          upSQL: undefined
        }
      ];

      jest.spyOn(dryRunRunner as any, '_getLocalMigrations').mockResolvedValue(mockLocalMigrations);
      mockDb.getAppliedMigrations.mockResolvedValue([]);

      await dryRunRunner.up();

      expect(consoleWarnSpy).toHaveBeenCalledWith(
        expect.stringContaining('Would skip 20240101120000: No \'up\' SQL found for environment \'test\'.')
      );
      
      consoleWarnSpy.mockRestore();
    });

    it('should show dry run skip message for migrations without downSQL', async () => {
      const dryRunContext = { ...context, dryRun: true };
      const dryRunRunner = new Runner(dryRunContext);
      const consoleWarnSpy = jest.spyOn(console, 'warn').mockImplementation();

      const mockLocalMigrations = [
        {
          version: '20240101120000',
          name: 'create_users',
          filePath: '/tmp/migrations/test.yml',
          downSQL: undefined
        }
      ];

      const mockAppliedMigrations = [
        { version: '20240101120000', active: 1, created_at: '2024-01-01T12:00:00Z' }
      ];

      jest.spyOn(dryRunRunner as any, '_getLocalMigrations').mockResolvedValue(mockLocalMigrations);
      mockDb.getAppliedMigrations.mockResolvedValue(mockAppliedMigrations);

      await dryRunRunner.down();

      expect(consoleWarnSpy).toHaveBeenCalledWith(
        expect.stringContaining('Would skip 20240101120000: No \'down\' SQL found for environment \'test\'.')
      );
      
      consoleWarnSpy.mockRestore();
    });

    it('should show dry run message when no pending migrations', async () => {
      const dryRunContext = { ...context, dryRun: true };
      const dryRunRunner = new Runner(dryRunContext);

      jest.spyOn(dryRunRunner as any, '_getLocalMigrations').mockResolvedValue([]);
      mockDb.getAppliedMigrations.mockResolvedValue([]);

      await dryRunRunner.up();

      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('No pending migrations to preview. Database is up-to-date.')
      );
    });
  });

  describe('verbose flag functionality', () => {
    let consoleSpy: jest.SpyInstance;

    beforeEach(() => {
      consoleSpy = jest.spyOn(console, 'log').mockImplementation();
    });

    afterEach(() => {
      consoleSpy.mockRestore();
    });

    it('should show detailed logs when verbose is true for up migrations', async () => {
      const verboseContext = { ...context, verbose: true };
      const verboseRunner = new Runner(verboseContext);

      const mockLocalMigrations = [
        {
          version: '20240101120000',
          name: 'create_users',
          filePath: '/tmp/migrations/test.yml',
          upSQL: 'CREATE TABLE users',
          table: 'users',
          database: 'test_db'
        }
      ];

      jest.spyOn(verboseRunner as any, '_getLocalMigrations').mockResolvedValue(mockLocalMigrations);
      jest.spyOn(verboseRunner as any, '_updateSchemaFile').mockResolvedValue(undefined);
      mockDb.getAppliedMigrations.mockResolvedValue([]);
      mockDb.executeMigration.mockResolvedValue(undefined);
      mockDb.markMigrationApplied.mockResolvedValue(undefined);

      await verboseRunner.up();

      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('--- UP SQL (Env:')
      );
      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('CREATE TABLE users')
      );
    });

    it('should not show detailed logs when verbose is false for up migrations', async () => {
      const nonVerboseContext = { ...context, verbose: false };
      const nonVerboseRunner = new Runner(nonVerboseContext);

      const mockLocalMigrations = [
        {
          version: '20240101120000',
          name: 'create_users',
          filePath: '/tmp/migrations/test.yml',
          upSQL: 'CREATE TABLE users'
        }
      ];

      jest.spyOn(nonVerboseRunner as any, '_getLocalMigrations').mockResolvedValue(mockLocalMigrations);
      jest.spyOn(nonVerboseRunner as any, '_updateSchemaFile').mockResolvedValue(undefined);
      mockDb.getAppliedMigrations.mockResolvedValue([]);
      mockDb.executeMigration.mockResolvedValue(undefined);
      mockDb.markMigrationApplied.mockResolvedValue(undefined);

      await nonVerboseRunner.up();

      expect(consoleSpy).not.toHaveBeenCalledWith(
        expect.stringContaining('--- UP SQL (Env:')
      );
    });

    it('should show detailed logs when verbose is true for down migrations', async () => {
      const verboseContext = { ...context, verbose: true };
      const verboseRunner = new Runner(verboseContext);

      const mockLocalMigrations = [
        {
          version: '20240101120000',
          name: 'create_users',
          filePath: '/tmp/migrations/test.yml',
          downSQL: 'DROP TABLE users',
          table: 'users',
          database: 'test_db'
        }
      ];

      const mockAppliedMigrations = [
        { version: '20240101120000', active: 1, created_at: '2024-01-01T12:00:00Z' }
      ];

      jest.spyOn(verboseRunner as any, '_getLocalMigrations').mockResolvedValue(mockLocalMigrations);
      jest.spyOn(verboseRunner as any, '_updateSchemaFile').mockResolvedValue(undefined);
      mockDb.getAppliedMigrations.mockResolvedValue(mockAppliedMigrations);
      mockDb.executeMigration.mockResolvedValue(undefined);
      mockDb.markMigrationRolledBack.mockResolvedValue(undefined);
      mockInquirer.prompt.mockResolvedValue({ confirmation: true });

      await verboseRunner.down();

      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('--- DOWN SQL (Env:')
      );
      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('DROP TABLE users')
      );
    });

    it('should not show detailed logs when verbose is false for down migrations', async () => {
      const nonVerboseContext = { ...context, verbose: false };
      const nonVerboseRunner = new Runner(nonVerboseContext);

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

      jest.spyOn(nonVerboseRunner as any, '_getLocalMigrations').mockResolvedValue(mockLocalMigrations);
      jest.spyOn(nonVerboseRunner as any, '_updateSchemaFile').mockResolvedValue(undefined);
      mockDb.getAppliedMigrations.mockResolvedValue(mockAppliedMigrations);
      mockDb.executeMigration.mockResolvedValue(undefined);
      mockDb.markMigrationRolledBack.mockResolvedValue(undefined);

      await nonVerboseRunner.down();

      expect(consoleSpy).not.toHaveBeenCalledWith(
        expect.stringContaining('--- DOWN SQL (Env:')
      );
    });

    it('should show verbose schema update logs when verbose is true', async () => {
      const verboseContext = { ...context, verbose: true };
      const verboseRunner = new Runner(verboseContext);

      const mockLocalMigrations = [
        {
          version: '20240101120000',
          name: 'create_users',
          filePath: '/tmp/migrations/test.yml'
        }
      ];

      mockDb.getDatabaseSchema.mockResolvedValue({});
      mockDb.getDatabaseTables.mockResolvedValue([]);
      mockDb.getDatabaseMaterializedViews.mockResolvedValue([]);
      mockDb.getDatabaseDictionaries.mockResolvedValue([]);
      mockFs.writeFile.mockResolvedValue(undefined);

      await verboseRunner['_updateSchemaFile']();

      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('Updating schema file for all databases')
      );
      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('Found 0 tables, 0 views, 0 dictionaries')
      );
      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('Schema file updated:')
      );
    });

    it('should show simple schema update message when verbose is false', async () => {
      const nonVerboseContext = { ...context, verbose: false };
      const nonVerboseRunner = new Runner(nonVerboseContext);

      const mockLocalMigrations = [
        {
          version: '20240101120000',
          name: 'create_users',
          filePath: '/tmp/migrations/test.yml'
        }
      ];

      mockDb.getDatabaseSchema.mockResolvedValue({});
      mockDb.getDatabaseTables.mockResolvedValue([]);
      mockDb.getDatabaseMaterializedViews.mockResolvedValue([]);
      mockDb.getDatabaseDictionaries.mockResolvedValue([]);
      mockFs.writeFile.mockResolvedValue(undefined);

      await nonVerboseRunner['_updateSchemaFile']();

      expect(consoleSpy).not.toHaveBeenCalledWith(
        expect.stringContaining('Updating schema file for all databases')
      );
    });

    it('should show verbose logs for reset when verbose is true', async () => {
      const verboseContext = { ...context, verbose: true };
      const verboseRunner = new Runner(verboseContext);

      const mockLocalMigrations = [
        {
          version: '20240101120000',
          name: 'create_users',
          filePath: '/tmp/migrations/test.yml',
          downSQL: 'DROP TABLE users',
          table: 'users',
          database: 'test_db'
        }
      ];

      const mockAppliedMigrations = [
        { version: '20240101120000', active: 1, created_at: '2024-01-01T12:00:00Z' }
      ];

      jest.spyOn(verboseRunner as any, '_getLocalMigrations').mockResolvedValue(mockLocalMigrations);
      jest.spyOn(verboseRunner as any, '_updateSchemaFile').mockResolvedValue(undefined);
      mockDb.getAppliedMigrations.mockResolvedValue(mockAppliedMigrations);
      mockDb.executeMigration.mockResolvedValue(undefined);
      mockDb.clearMigrationsTable.mockResolvedValue(undefined);
      mockDb.optimizeMigrationTable.mockResolvedValue(undefined);
      mockInquirer.prompt.mockResolvedValue({ confirmation: true });

      await verboseRunner.reset();

      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('  --- DOWN SQL (Env:')
      );
      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('DROP TABLE users')
      );
    });

    it('should not show verbose logs for reset when verbose is false', async () => {
      const nonVerboseContext = { ...context, verbose: false };
      const nonVerboseRunner = new Runner(nonVerboseContext);

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

      jest.spyOn(nonVerboseRunner as any, '_getLocalMigrations').mockResolvedValue(mockLocalMigrations);
      jest.spyOn(nonVerboseRunner as any, '_updateSchemaFile').mockResolvedValue(undefined);
      mockDb.getAppliedMigrations.mockResolvedValue(mockAppliedMigrations);
      mockDb.executeMigration.mockResolvedValue(undefined);
      mockDb.clearMigrationsTable.mockResolvedValue(undefined);
      mockDb.optimizeMigrationTable.mockResolvedValue(undefined);
      mockInquirer.prompt.mockResolvedValue({ confirmation: true });

      await nonVerboseRunner.reset();

      expect(consoleSpy).not.toHaveBeenCalledWith(
        expect.stringContaining('  --- DOWN SQL (Env:')
      );
    });
  });

  describe('_updateSchemaFile', () => {
    beforeEach(() => {
      mockFs.writeFile.mockResolvedValue(undefined);
    });

    it('should generate schema file with tables, views, and dictionaries', async () => {
      const mockSchema = {
        'table/test_db.users': 'CREATE TABLE test_db.users (id UInt32) ENGINE = MergeTree() ORDER BY id',
        'table/test_db.orders': 'CREATE TABLE test_db.orders (id UInt32) ENGINE = MergeTree() ORDER BY id',
        'view/test_db.user_stats_mv': 'CREATE MATERIALIZED VIEW test_db.user_stats_mv AS SELECT count() FROM users',
        'dictionary/test_db.countries_dict': 'CREATE DICTIONARY test_db.countries_dict (id UInt32, name String) PRIMARY KEY id'
      };

      // For verbose mode, we still need to mock these for the count
      const mockTablesWithDb = [{ name: 'users', database: 'test_db' }, { name: 'orders', database: 'test_db' }];
      const mockViewsWithDb = [{ name: 'user_stats_mv', database: 'test_db' }];
      const mockDictionariesWithDb = [{ name: 'countries_dict', database: 'test_db' }];

      mockDb.getDatabaseSchema.mockResolvedValue(mockSchema);
      mockDb.getDatabaseTables.mockResolvedValue(mockTablesWithDb);
      mockDb.getDatabaseMaterializedViews.mockResolvedValue(mockViewsWithDb);
      mockDb.getDatabaseDictionaries.mockResolvedValue(mockDictionariesWithDb);
      mockFs.writeFile.mockResolvedValue(undefined);

      await runner['_updateSchemaFile']();

      expect(mockDb.getDatabaseSchema).toHaveBeenCalled();

      const writeCall = mockFs.writeFile.mock.calls[0];
      const schemaPath = writeCall[0] as string;
      const schemaContent = writeCall[1] as string;

      expect(schemaPath).toBe(path.join('/tmp/migrations', 'schema.sql'));
      expect(schemaContent).toContain('-- Auto-generated schema file');
      expect(schemaContent).toContain('-- Environment: test');
      expect(schemaContent).toContain('-- Databases: test_db');
      expect(schemaContent).toContain('-- TABLES');
      expect(schemaContent).toContain('-- Table: test_db.users');
      expect(schemaContent).toContain('CREATE TABLE test_db.users (id UInt32) ENGINE = MergeTree() ORDER BY id;');
      expect(schemaContent).toContain('-- Table: test_db.orders');
      expect(schemaContent).toContain('-- MATERIALIZED VIEWS');
      expect(schemaContent).toContain('-- Materialized View: test_db.user_stats_mv');
      expect(schemaContent).toContain('CREATE MATERIALIZED VIEW test_db.user_stats_mv AS SELECT count() FROM users;');
      expect(schemaContent).toContain('-- DICTIONARIES');
      expect(schemaContent).toContain('-- Dictionary: test_db.countries_dict');
      expect(schemaContent).toContain('CREATE DICTIONARY test_db.countries_dict (id UInt32, name String) PRIMARY KEY id;');
    });

    it('should handle empty database', async () => {
      mockDb.getDatabaseSchema.mockResolvedValue({});
      mockDb.getDatabaseTables.mockResolvedValue([]);
      mockDb.getDatabaseMaterializedViews.mockResolvedValue([]);
      mockDb.getDatabaseDictionaries.mockResolvedValue([]);
      mockFs.writeFile.mockResolvedValue(undefined);

      await runner['_updateSchemaFile']();

      const writeCall = mockFs.writeFile.mock.calls[0];
      const schemaContent = writeCall[1] as string;

      expect(schemaContent).toContain('-- Auto-generated schema file');
      expect(schemaContent).not.toContain('-- Table:');
      expect(schemaContent).not.toContain('-- Materialized View:');
      expect(schemaContent).not.toContain('-- Dictionary:');
    });

    it('should handle database query errors gracefully', async () => {
      mockDb.getDatabaseSchema.mockResolvedValue({});
      mockDb.getDatabaseTables.mockResolvedValue([{ name: 'users', database: 'test_db' }]);
      mockDb.getDatabaseMaterializedViews.mockResolvedValue([]);
      mockDb.getDatabaseDictionaries.mockResolvedValue([]);
      mockFs.writeFile.mockResolvedValue(undefined);

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

  describe('skipSchemaUpdate functionality', () => {
    beforeEach(() => {
      mockFs.writeFile.mockResolvedValue(undefined);
      mockDb.getDatabaseTables.mockResolvedValue([]);
      mockDb.getDatabaseMaterializedViews.mockResolvedValue([]);
      mockDb.getDatabaseDictionaries.mockResolvedValue([]);
    });

    it('should skip schema file update when skipSchemaUpdate is true for up migrations', async () => {
      const skipRunner = new Runner({
        ...context,
        skipSchemaUpdate: true
      });
      (skipRunner as any).db = mockDb;

      const updateSchemaFileSpy = jest.spyOn(skipRunner as any, '_updateSchemaFile').mockResolvedValue(undefined);

      const mockLocalMigrations = [
        {
          version: '20240101120000',
          name: 'create_users',
          filePath: '/tmp/migrations/test.yml',
          upSQL: 'CREATE TABLE users'
        }
      ];

      jest.spyOn(skipRunner as any, '_getLocalMigrations').mockResolvedValue(mockLocalMigrations);
      mockDb.getAppliedMigrations.mockResolvedValue([]);
      mockDb.executeMigration.mockResolvedValue(undefined);
      mockDb.markMigrationApplied.mockResolvedValue(undefined);

      await skipRunner.up();

      expect(updateSchemaFileSpy).not.toHaveBeenCalled();
    });

    it('should skip schema file update when skipSchemaUpdate is true for down migrations', async () => {
      const skipRunner = new Runner({
        ...context,
        skipSchemaUpdate: true
      });
      (skipRunner as any).db = mockDb;

      const updateSchemaFileSpy = jest.spyOn(skipRunner as any, '_updateSchemaFile').mockResolvedValue(undefined);

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

      jest.spyOn(skipRunner as any, '_getLocalMigrations').mockResolvedValue(mockLocalMigrations);
      mockDb.getAppliedMigrations.mockResolvedValue(mockAppliedMigrations);
      mockDb.executeMigration.mockResolvedValue(undefined);
      mockDb.markMigrationRolledBack.mockResolvedValue(undefined);
      mockInquirer.prompt.mockResolvedValue({ confirmation: true });

      await skipRunner.down();

      expect(updateSchemaFileSpy).not.toHaveBeenCalled();
    });

    it('should skip schema file update when skipSchemaUpdate is true for reset', async () => {
      const skipRunner = new Runner({
        ...context,
        skipSchemaUpdate: true,
        nonInteractive: true
      });
      (skipRunner as any).db = mockDb;

      const updateSchemaFileSpy = jest.spyOn(skipRunner as any, '_updateSchemaFile').mockResolvedValue(undefined);

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

      jest.spyOn(skipRunner as any, '_getLocalMigrations').mockResolvedValue(mockLocalMigrations);
      mockDb.getAppliedMigrations.mockResolvedValue(mockAppliedMigrations);
      mockDb.executeMigration.mockResolvedValue(undefined);
      mockDb.markMigrationRolledBack.mockResolvedValue(undefined);
      mockDb.clearMigrationsTable.mockResolvedValue(undefined);
      mockDb.optimizeMigrationTable.mockResolvedValue(undefined);

      await skipRunner.reset();

      expect(updateSchemaFileSpy).not.toHaveBeenCalled();
    });

    it('should skip schema file update when skipSchemaUpdate is true for schemaLoad', async () => {
      const skipRunner = new Runner({
        ...context,
        skipSchemaUpdate: true
      });
      (skipRunner as any).db = mockDb;

      const updateSchemaFileSpy = jest.spyOn(skipRunner as any, '_updateSchemaFile').mockResolvedValue(undefined);

      const mockLocalMigrations = [
        {
          version: '20240101120000',
          name: 'create_users',
          filePath: '/tmp/migrations/test.yml'
        }
      ];

      jest.spyOn(skipRunner as any, '_getLocalMigrations').mockResolvedValue(mockLocalMigrations);
      mockDb.getAllMigrationRecords.mockResolvedValue([]);
      mockDb.markMigrationApplied.mockResolvedValue(undefined);
      mockDb.optimizeMigrationTable.mockResolvedValue(undefined);

      await skipRunner.schemaLoad();

      expect(updateSchemaFileSpy).not.toHaveBeenCalled();
    });

    it('should update schema file when skipSchemaUpdate is false (default behavior)', async () => {
      const defaultRunner = new Runner({
        ...context,
        skipSchemaUpdate: false
      });
      (defaultRunner as any).db = mockDb;

      const updateSchemaFileSpy = jest.spyOn(defaultRunner as any, '_updateSchemaFile').mockResolvedValue(undefined);

      const mockLocalMigrations = [
        {
          version: '20240101120000',
          name: 'create_users',
          filePath: '/tmp/migrations/test.yml',
          upSQL: 'CREATE TABLE users'
        }
      ];

      jest.spyOn(defaultRunner as any, '_getLocalMigrations').mockResolvedValue(mockLocalMigrations);
      mockDb.getAppliedMigrations.mockResolvedValue([]);
      mockDb.executeMigration.mockResolvedValue(undefined);
      mockDb.markMigrationApplied.mockResolvedValue(undefined);

      await defaultRunner.up();

      expect(updateSchemaFileSpy).toHaveBeenCalled();
    });
  });

  describe('environment variable interpolation', () => {
    let originalEnv: NodeJS.ProcessEnv;
    let consoleWarnSpy: jest.SpyInstance;

    beforeEach(() => {
      originalEnv = { ...process.env };
      consoleWarnSpy = jest.spyOn(console, 'warn').mockImplementation();
    });

    afterEach(() => {
      process.env = originalEnv;
      consoleWarnSpy.mockRestore();
    });

    it('should warn and return empty string for undefined environment variables', async () => {
      delete process.env.UNDEFINED_VAR;

      // Mock file system and YAML parsing to simulate real file loading
      mockFs.readdir.mockResolvedValue(['20240101120000_create_users.yml'] as any);
      mockFs.readFile.mockResolvedValue('test:\n  up: "CREATE TABLE ${UNDEFINED_VAR}_users"');
      mockYaml.load.mockReturnValue({
        test: {
          up: 'CREATE TABLE ${UNDEFINED_VAR}_users'
        }
      });

      mockDb.getAppliedMigrations.mockResolvedValue([]);
      mockDb.executeMigration.mockResolvedValue(undefined);
      mockDb.markMigrationApplied.mockResolvedValue(undefined);
      jest.spyOn(runner as any, '_updateSchemaFile').mockResolvedValue(undefined);

      await runner.up();

      expect(consoleWarnSpy).toHaveBeenCalledWith(
        expect.stringContaining("⚠️  Warning: Environment variable 'UNDEFINED_VAR' is not set. Using empty string.")
      );
      expect(mockDb.executeMigration).toHaveBeenCalledWith('CREATE TABLE _users', {});
    });

    it('should use environment variable value when defined', async () => {
      process.env.TEST_PREFIX = 'prod';

      // Mock file system and YAML parsing to simulate real file loading
      mockFs.readdir.mockResolvedValue(['20240101120000_create_users.yml'] as any);
      mockFs.readFile.mockResolvedValue('test:\n  up: "CREATE TABLE ${TEST_PREFIX}_users"');
      mockYaml.load.mockReturnValue({
        test: {
          up: 'CREATE TABLE ${TEST_PREFIX}_users'
        }
      });

      mockDb.getAppliedMigrations.mockResolvedValue([]);
      mockDb.executeMigration.mockResolvedValue(undefined);
      mockDb.markMigrationApplied.mockResolvedValue(undefined);
      jest.spyOn(runner as any, '_updateSchemaFile').mockResolvedValue(undefined);

      await runner.up();

      expect(mockDb.executeMigration).toHaveBeenCalledWith('CREATE TABLE prod_users', {});
    });
  });

  describe('formatSQL edge cases', () => {
    it('should handle migrations with empty or undefined SQL', async () => {
      const mockLocalMigrations = [
        {
          version: '20240101120000',
          name: 'empty_migration',
          filePath: '/tmp/migrations/test.yml',
          upSQL: undefined,
          table: 'users',
          database: 'test_db'
        },
        {
          version: '20240102120000',
          name: 'null_migration',
          filePath: '/tmp/migrations/test2.yml',
          upSQL: null,
          table: 'posts',
          database: 'test_db'
        }
      ];

      jest.spyOn(runner as any, '_getLocalMigrations').mockResolvedValue(mockLocalMigrations);
      mockDb.getAppliedMigrations.mockResolvedValue([]);
      jest.spyOn(runner as any, '_updateSchemaFile').mockResolvedValue(undefined);

      await runner.up();

      // Should skip both migrations due to no upSQL
      expect(mockDb.executeMigration).not.toHaveBeenCalled();
      expect(mockDb.markMigrationApplied).not.toHaveBeenCalled();
    });
  });

  describe('rollback error scenarios', () => {
    let consoleErrorSpy: jest.SpyInstance;

    beforeEach(() => {
      consoleErrorSpy = jest.spyOn(console, 'error').mockImplementation();
    });

    afterEach(() => {
      consoleErrorSpy.mockRestore();
    });

    it('should handle target version not currently applied', async () => {
      const mockLocalMigrations = [
        {
          version: '20240101120000',
          name: 'create_users',
          filePath: '/tmp/migrations/test.yml',
          downSQL: 'DROP TABLE users'
        },
        {
          version: '20240102120000',
          name: 'add_index',
          filePath: '/tmp/migrations/test2.yml',
          downSQL: 'DROP INDEX idx_users'
        }
      ];

      const mockAppliedMigrations = [
        { version: '20240101120000', active: 1, created_at: '2024-01-01T12:00:00Z' }
      ];

      jest.spyOn(runner as any, '_getLocalMigrations').mockResolvedValue(mockLocalMigrations);
      mockDb.getAppliedMigrations.mockResolvedValue(mockAppliedMigrations);

      await runner.down('20240102120000');

      expect(consoleErrorSpy).toHaveBeenCalledWith(
        expect.stringContaining('❌ Target version 20240102120000 is not currently applied')
      );
    });

    it('should handle target version not existing locally', async () => {
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

      await runner.down('20240999999999');

      expect(consoleErrorSpy).toHaveBeenCalledWith(
        expect.stringContaining('❌ Target version 20240999999999 is not currently applied')
      );
      expect(consoleErrorSpy).toHaveBeenCalledWith(
        expect.stringContaining('❌ Additionally, version 20240999999999 does not exist in local migration files')
      );
    });
  });
});