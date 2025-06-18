import { Db } from '../src/db';
import { Context } from '../src/types';
import { createClient } from '@clickhouse/client';

jest.mock('@clickhouse/client');

const mockCreateClient = createClient as jest.MockedFunction<typeof createClient>;

describe('Db', () => {
  let db: Db;
  let mockClient: any;
  let context: Context;

  beforeEach(() => {
    mockClient = {
      ping: jest.fn(),
      command: jest.fn(),
      query: jest.fn(),
      insert: jest.fn(),
      close: jest.fn(),
    };

    mockCreateClient.mockReturnValue(mockClient);

    context = {
      url: 'http://default@localhost:8123/test_db',
      database: 'test_db',
      migrationsDir: '/tmp/migrations',
      environment: 'test'
    };

    db = new Db(context);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('constructor', () => {
    it('should create ClickHouse client with URL configuration', () => {
      expect(mockCreateClient).toHaveBeenCalledWith({
        url: 'http://default@localhost:8123/test_db',
      });
    });

    it('should handle HTTPS URLs', () => {
      const httpsContext: Context = {
        url: 'https://user:pass@clickhouse.example.com:8443/prod_db',
        database: 'prod_db',
        migrationsDir: '/tmp/migrations',
        environment: 'test'
      };
      new Db(httpsContext);

      expect(mockCreateClient).toHaveBeenCalledWith({
        url: 'https://user:pass@clickhouse.example.com:8443/prod_db',
      });
    });

    it('should handle URLs with cluster configuration', () => {
      const clusterContext: Context = {
        url: 'http://default@localhost:8123/test_db',
        database: 'test_db',
        cluster: 'test_cluster',
        migrationsDir: '/tmp/migrations',
        environment: 'test'
      };
      new Db(clusterContext);

      expect(mockCreateClient).toHaveBeenCalledWith({
        url: 'http://default@localhost:8123/test_db',
      });
    });
  });

  describe('ping', () => {
    it('should call client ping method', async () => {
      const pingResult = { success: true };
      mockClient.ping.mockResolvedValue(pingResult);

      const result = await db.ping();

      expect(mockClient.ping).toHaveBeenCalled();
      expect(result).toEqual(pingResult);
    });
  });

  describe('initMigrationsTable', () => {
    it('should create migrations table without cluster', async () => {
      mockClient.command.mockResolvedValue(undefined);

      await db.initMigrationsTable();

      expect(mockClient.command).toHaveBeenCalledWith({
        query: expect.stringContaining('CREATE TABLE IF NOT EXISTS default.__clicksuite_migrations'),
        clickhouse_settings: { wait_end_of_query: 1 }
      });

      const query = mockClient.command.mock.calls[0][0].query;
      expect(query).toContain('ReplacingMergeTree()');
      expect(query).not.toContain('ON CLUSTER');
    });

    it('should create migrations table with cluster', async () => {
      const clusterContext: Context = {
        url: 'http://default@localhost:8123/test_db',
        database: 'test_db',
        cluster: 'test_cluster',
        migrationsDir: '/tmp/migrations',
        environment: 'test'
      };
      const clusterDb = new Db(clusterContext);
      mockClient.command.mockResolvedValue(undefined);

      await clusterDb.initMigrationsTable();

      const query = mockClient.command.mock.calls[0][0].query;
      expect(query).toContain('ON CLUSTER test_cluster');
      expect(query).toContain('ReplicatedReplacingMergeTree');
      expect(query).toContain('default.__clicksuite_migrations');
    });

    it('should handle errors during table creation', async () => {
      const error = new Error('Database connection failed');
      mockClient.command.mockRejectedValue(error);

      await expect(db.initMigrationsTable()).rejects.toThrow('Database connection failed');
    });
  });

  describe('getAppliedMigrations', () => {
    it('should return applied migrations', async () => {
      const mockMigrations = [
        { version: '20240101120000', active: 1, created_at: '2024-01-01T12:00:00Z' },
        { version: '20240102120000', active: 1, created_at: '2024-01-02T12:00:00Z' }
      ];

      const mockResultSet = {
        json: jest.fn().mockResolvedValue(mockMigrations)
      };
      mockClient.query.mockResolvedValue(mockResultSet);

      const result = await db.getAppliedMigrations();

      expect(mockClient.query).toHaveBeenCalledWith({
        query: 'SELECT version, active, created_at FROM default.__clicksuite_migrations WHERE active = 1 ORDER BY version ASC',
        format: 'JSONEachRow'
      });
      expect(result).toEqual(mockMigrations);
    });

    it('should return empty array on error', async () => {
      mockClient.query.mockRejectedValue(new Error('Query failed'));

      const result = await db.getAppliedMigrations();

      expect(result).toEqual([]);
    });
  });

  describe('getAllMigrationRecords', () => {
    it('should return all migration records', async () => {
      const mockRecords = [
        { version: '20240101120000', active: 1, created_at: '2024-01-01T12:00:00Z' },
        { version: '20240102120000', active: 0, created_at: '2024-01-02T12:00:00Z' }
      ];

      const mockResultSet = {
        json: jest.fn().mockResolvedValue(mockRecords)
      };
      mockClient.query.mockResolvedValue(mockResultSet);

      const result = await db.getAllMigrationRecords();

      expect(mockClient.query).toHaveBeenCalledWith({
        query: 'SELECT version, active, created_at FROM default.__clicksuite_migrations ORDER BY version ASC',
        format: 'JSONEachRow'
      });
      expect(result).toEqual(mockRecords);
    });
  });

  describe('executeMigration', () => {
    it('should execute migration query with settings', async () => {
      const query = 'CREATE TABLE test_table';
      const settings = { max_execution_time: 60 };
      mockClient.command.mockResolvedValue(undefined);

      await db.executeMigration(query, settings);

      expect(mockClient.command).toHaveBeenCalledWith({
        query,
        clickhouse_settings: {
          ...settings,
          wait_end_of_query: 1
        }
      });
    });

    it('should execute migration query without settings', async () => {
      const query = 'CREATE TABLE test_table';
      mockClient.command.mockResolvedValue(undefined);

      await db.executeMigration(query);

      expect(mockClient.command).toHaveBeenCalledWith({
        query,
        clickhouse_settings: {
          wait_end_of_query: 1
        }
      });
    });

    it('should handle execution errors', async () => {
      const query = 'INVALID SQL';
      const error = new Error('Syntax error');
      mockClient.command.mockRejectedValue(error);

      await expect(db.executeMigration(query)).rejects.toThrow('Syntax error');
    });

    it('should execute multiple queries separated by semicolons', async () => {
      const query = 'CREATE TABLE test_table1; CREATE TABLE test_table2; INSERT INTO test_table1 VALUES (1)';
      mockClient.command.mockResolvedValue(undefined);

      await db.executeMigration(query);

      expect(mockClient.command).toHaveBeenCalledTimes(3);
      expect(mockClient.command).toHaveBeenNthCalledWith(1, {
        query: 'CREATE TABLE test_table1',
        clickhouse_settings: {
          wait_end_of_query: 1
        }
      });
      expect(mockClient.command).toHaveBeenNthCalledWith(2, {
        query: 'CREATE TABLE test_table2',
        clickhouse_settings: {
          wait_end_of_query: 1
        }
      });
      expect(mockClient.command).toHaveBeenNthCalledWith(3, {
        query: 'INSERT INTO test_table1 VALUES (1)',
        clickhouse_settings: {
          wait_end_of_query: 1
        }
      });
    });

    it('should execute multiple queries with settings', async () => {
      const query = 'CREATE TABLE test_table1; CREATE TABLE test_table2';
      const settings = { max_execution_time: 60 };
      mockClient.command.mockResolvedValue(undefined);

      await db.executeMigration(query, settings);

      expect(mockClient.command).toHaveBeenCalledTimes(2);
      expect(mockClient.command).toHaveBeenNthCalledWith(1, {
        query: 'CREATE TABLE test_table1',
        clickhouse_settings: {
          ...settings,
          wait_end_of_query: 1
        }
      });
      expect(mockClient.command).toHaveBeenNthCalledWith(2, {
        query: 'CREATE TABLE test_table2',
        clickhouse_settings: {
          ...settings,
          wait_end_of_query: 1
        }
      });
    });

    it('should handle empty queries in multiple query string', async () => {
      const query = 'CREATE TABLE test_table1;; ; CREATE TABLE test_table2;';
      mockClient.command.mockResolvedValue(undefined);

      await db.executeMigration(query);

      expect(mockClient.command).toHaveBeenCalledTimes(2);
      expect(mockClient.command).toHaveBeenNthCalledWith(1, {
        query: 'CREATE TABLE test_table1',
        clickhouse_settings: {
          wait_end_of_query: 1
        }
      });
      expect(mockClient.command).toHaveBeenNthCalledWith(2, {
        query: 'CREATE TABLE test_table2',
        clickhouse_settings: {
          wait_end_of_query: 1
        }
      });
    });

    it('should handle queries with whitespace and newlines', async () => {
      const query = `
        CREATE TABLE test_table1 (
          id UInt32
        );
        
        CREATE TABLE test_table2 (
          name String
        );
      `;
      mockClient.command.mockResolvedValue(undefined);

      await db.executeMigration(query);

      expect(mockClient.command).toHaveBeenCalledTimes(2);
      expect(mockClient.command).toHaveBeenNthCalledWith(1, {
        query: 'CREATE TABLE test_table1 (\n          id UInt32\n        )',
        clickhouse_settings: {
          wait_end_of_query: 1
        }
      });
      expect(mockClient.command).toHaveBeenNthCalledWith(2, {
        query: 'CREATE TABLE test_table2 (\n          name String\n        )',
        clickhouse_settings: {
          wait_end_of_query: 1
        }
      });
    });

    it('should do nothing when query is empty or only semicolons', async () => {
      const query = ';;; ; ';
      mockClient.command.mockResolvedValue(undefined);

      await db.executeMigration(query);

      expect(mockClient.command).not.toHaveBeenCalled();
    });
  });

  describe('markMigrationApplied', () => {
    it('should mark migration as applied', async () => {
      const version = '20240101120000';
      mockClient.insert.mockResolvedValue(undefined);
      mockClient.command.mockResolvedValue(undefined); // for optimize call

      await db.markMigrationApplied(version);

      expect(mockClient.insert).toHaveBeenCalledWith({
        table: 'default.__clicksuite_migrations',
        values: [{
          version,
          active: 1,
          created_at: expect.any(String)
        }],
        format: 'JSONEachRow',
        clickhouse_settings: {
          date_time_input_format: 'best_effort'
        }
      });
    });

    it('should handle marking errors', async () => {
      const error = new Error('Insert failed');
      mockClient.insert.mockRejectedValue(error);

      await expect(db.markMigrationApplied('20240101120000')).rejects.toThrow('Insert failed');
    });
  });

  describe('markMigrationRolledBack', () => {
    it('should mark migration as rolled back', async () => {
      const version = '20240101120000';
      mockClient.insert.mockResolvedValue(undefined);
      mockClient.command.mockResolvedValue(undefined); // for optimize call

      await db.markMigrationRolledBack(version);

      expect(mockClient.insert).toHaveBeenCalledWith({
        table: 'default.__clicksuite_migrations',
        values: [{
          version,
          active: 0,
          created_at: expect.any(String)
        }],
        format: 'JSONEachRow',
        clickhouse_settings: {
          date_time_input_format: 'best_effort'
        }
      });
    });
  });

  describe('getLatestMigration', () => {
    it('should return latest migration version', async () => {
      const mockMigrations = [
        { version: '20240101120000', active: 1, created_at: '2024-01-01T12:00:00Z' },
        { version: '20240102120000', active: 1, created_at: '2024-01-02T12:00:00Z' }
      ];

      const mockResultSet = {
        json: jest.fn().mockResolvedValue(mockMigrations)
      };
      mockClient.query.mockResolvedValue(mockResultSet);

      const result = await db.getLatestMigration();

      expect(result).toBe('20240102120000');
    });

    it('should return undefined when no migrations exist', async () => {
      const mockResultSet = {
        json: jest.fn().mockResolvedValue([])
      };
      mockClient.query.mockResolvedValue(mockResultSet);

      const result = await db.getLatestMigration();

      expect(result).toBeUndefined();
    });
  });

  describe('getDatabaseTables', () => {
    it('should return list of tables', async () => {
      const mockTables = [{ name: 'users' }, { name: 'orders' }];
      const mockResultSet = {
        json: jest.fn().mockResolvedValue(mockTables)
      };
      mockClient.query.mockResolvedValue(mockResultSet);

      const result = await db.getDatabaseTables();

      expect(mockClient.query).toHaveBeenCalledWith({
        query: "SELECT name FROM system.tables WHERE database = 'test_db' AND engine NOT LIKE '%View' AND engine != 'MaterializedView'",
        format: 'JSONEachRow'
      });
      expect(result).toEqual(mockTables);
    });

    it('should return empty array on error', async () => {
      mockClient.query.mockRejectedValue(new Error('Query failed'));

      const result = await db.getDatabaseTables();

      expect(result).toEqual([]);
    });
  });

  describe('clearMigrationsTable', () => {
    it('should clear migrations table without cluster', async () => {
      mockClient.command.mockResolvedValue(undefined);

      await db.clearMigrationsTable();

      expect(mockClient.command).toHaveBeenCalledWith({
        query: 'TRUNCATE TABLE IF EXISTS default.__clicksuite_migrations ',
        clickhouse_settings: { wait_end_of_query: 1 }
      });
    });

    it('should clear migrations table with cluster', async () => {
      const clusterContext: Context = {
        url: 'http://default@localhost:8123/test_db',
        database: 'test_db',
        cluster: 'test_cluster',
        migrationsDir: '/tmp/migrations',
        environment: 'test'
      };
      const clusterDb = new Db(clusterContext);
      mockClient.command.mockResolvedValue(undefined);

      await clusterDb.clearMigrationsTable();

      const query = mockClient.command.mock.calls[0][0].query;
      expect(query).toContain('ON CLUSTER test_cluster');
      expect(query).toContain('default.__clicksuite_migrations');
    });

    it('should handle clear errors', async () => {
      const error = new Error('Clear failed');
      mockClient.command.mockRejectedValue(error);

      await expect(db.clearMigrationsTable()).rejects.toThrow('Clear failed');
    });
  });

  describe('close', () => {
    it('should close client connection', async () => {
      mockClient.close.mockResolvedValue(undefined);

      await db.close();

      expect(mockClient.close).toHaveBeenCalled();
    });
  });

  describe('optimizeMigrationTable', () => {
    it('should optimize migrations table', async () => {
      mockClient.command.mockResolvedValue(undefined);

      await db.optimizeMigrationTable();

      expect(mockClient.command).toHaveBeenCalledWith({
        query: 'OPTIMIZE TABLE default.__clicksuite_migrations FINAL',
        clickhouse_settings: { wait_end_of_query: 1 }
      });
    });

    it('should handle optimize errors gracefully', async () => {
      const error = new Error('Optimize failed');
      mockClient.command.mockRejectedValue(error);

      await expect(db.optimizeMigrationTable()).rejects.toThrow('Optimize failed');
    });
  });

  describe('verbose logging', () => {
    let consoleSpy: jest.SpyInstance;

    beforeEach(() => {
      consoleSpy = jest.spyOn(console, 'log').mockImplementation();
    });

    afterEach(() => {
      consoleSpy.mockRestore();
    });

    it('should log initMigrationsTable query when verbose is true', async () => {
      const verboseContext = { ...context, verbose: true };
      const verboseDb = new Db(verboseContext);
      mockClient.command.mockResolvedValue(undefined);

      await verboseDb.initMigrationsTable();

      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('Executing initMigrationsTable query:'),
        expect.any(String)
      );
    });

    it('should not log initMigrationsTable query when verbose is false', async () => {
      const nonVerboseContext = { ...context, verbose: false };
      const nonVerboseDb = new Db(nonVerboseContext);
      mockClient.command.mockResolvedValue(undefined);

      await nonVerboseDb.initMigrationsTable();

      expect(consoleSpy).not.toHaveBeenCalledWith(
        expect.stringContaining('Executing initMigrationsTable query:'),
        expect.any(String)
      );
    });

    it('should log single migration query when verbose is true', async () => {
      const verboseContext = { ...context, verbose: true };
      const verboseDb = new Db(verboseContext);
      mockClient.command.mockResolvedValue(undefined);

      await verboseDb.executeMigration('CREATE TABLE test');

      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('Executing migration query:'),
        expect.any(String)
      );
    });

    it('should not log single migration query when verbose is false', async () => {
      const nonVerboseContext = { ...context, verbose: false };
      const nonVerboseDb = new Db(nonVerboseContext);
      mockClient.command.mockResolvedValue(undefined);

      await nonVerboseDb.executeMigration('CREATE TABLE test');

      expect(consoleSpy).not.toHaveBeenCalledWith(
        expect.stringContaining('Executing migration query:'),
        expect.any(String)
      );
    });

    it('should log multiple migration queries when verbose is true', async () => {
      const verboseContext = { ...context, verbose: true };
      const verboseDb = new Db(verboseContext);
      mockClient.command.mockResolvedValue(undefined);

      await verboseDb.executeMigration('CREATE TABLE test1; CREATE TABLE test2');

      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('Executing 2 migration queries:')
      );
      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('Query 1/2:'),
        expect.any(String)
      );
      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('Query 2/2:'),
        expect.any(String)
      );
    });

    it('should not log multiple migration queries when verbose is false', async () => {
      const nonVerboseContext = { ...context, verbose: false };
      const nonVerboseDb = new Db(nonVerboseContext);
      mockClient.command.mockResolvedValue(undefined);

      await nonVerboseDb.executeMigration('CREATE TABLE test1; CREATE TABLE test2');

      expect(consoleSpy).not.toHaveBeenCalledWith(
        expect.stringContaining('Executing 2 migration queries:')
      );
    });

    it('should log markMigrationApplied when verbose is true', async () => {
      const verboseContext = { ...context, verbose: true };
      const verboseDb = new Db(verboseContext);
      mockClient.insert.mockResolvedValue(undefined);
      mockClient.command.mockResolvedValue(undefined);

      await verboseDb.markMigrationApplied('20240101120000');

      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('Marking migration applied with version:'),
        expect.stringContaining('20240101120000')
      );
    });

    it('should not log markMigrationApplied when verbose is false', async () => {
      const nonVerboseContext = { ...context, verbose: false };
      const nonVerboseDb = new Db(nonVerboseContext);
      mockClient.insert.mockResolvedValue(undefined);
      mockClient.command.mockResolvedValue(undefined);

      await nonVerboseDb.markMigrationApplied('20240101120000');

      expect(consoleSpy).not.toHaveBeenCalledWith(
        expect.stringContaining('Marking migration applied with version:'),
        expect.any(String)
      );
    });

    it('should log markMigrationRolledBack when verbose is true', async () => {
      const verboseContext = { ...context, verbose: true };
      const verboseDb = new Db(verboseContext);
      mockClient.insert.mockResolvedValue(undefined);
      mockClient.command.mockResolvedValue(undefined);

      await verboseDb.markMigrationRolledBack('20240101120000');

      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('Marking migration rolled back for version:'),
        expect.stringContaining('20240101120000')
      );
    });

    it('should not log markMigrationRolledBack when verbose is false', async () => {
      const nonVerboseContext = { ...context, verbose: false };
      const nonVerboseDb = new Db(nonVerboseContext);
      mockClient.insert.mockResolvedValue(undefined);
      mockClient.command.mockResolvedValue(undefined);

      await nonVerboseDb.markMigrationRolledBack('20240101120000');

      expect(consoleSpy).not.toHaveBeenCalledWith(
        expect.stringContaining('Marking migration rolled back for version:'),
        expect.any(String)
      );
    });

    it('should log clearMigrationsTable when verbose is true', async () => {
      const verboseContext = { ...context, verbose: true };
      const verboseDb = new Db(verboseContext);
      mockClient.command.mockResolvedValue(undefined);

      await verboseDb.clearMigrationsTable();

      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('Clearing migrations table:'),
        expect.any(String)
      );
    });

    it('should not log clearMigrationsTable when verbose is false', async () => {
      const nonVerboseContext = { ...context, verbose: false };
      const nonVerboseDb = new Db(nonVerboseContext);
      mockClient.command.mockResolvedValue(undefined);

      await nonVerboseDb.clearMigrationsTable();

      expect(consoleSpy).not.toHaveBeenCalledWith(
        expect.stringContaining('Clearing migrations table:'),
        expect.any(String)
      );
    });
  });
});