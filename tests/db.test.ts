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
        json: jest.fn().mockResolvedValue({ data: mockMigrations })
      };
      mockClient.query.mockResolvedValue(mockResultSet);

      const result = await db.getAppliedMigrations();

      expect(mockClient.query).toHaveBeenCalledWith({
        query: 'SELECT version, active, created_at FROM default.__clicksuite_migrations WHERE active = 1 ORDER BY version ASC'
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
        json: jest.fn().mockResolvedValue({ data: mockRecords })
      };
      mockClient.query.mockResolvedValue(mockResultSet);

      const result = await db.getAllMigrationRecords();

      expect(mockClient.query).toHaveBeenCalledWith({
        query: 'SELECT version, active, created_at FROM default.__clicksuite_migrations ORDER BY version ASC'
      });
      expect(result).toEqual(mockRecords);
    });

    it('should return empty array and log error on failure', async () => {
      const consoleSpy = jest.spyOn(console, 'error').mockImplementation();
      const error = new Error('Database connection failed');
      mockClient.query.mockRejectedValue(error);

      const result = await db.getAllMigrationRecords();

      expect(result).toEqual([]);
      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('❌ Failed to get all migration records:'),
        error
      );
      
      consoleSpy.mockRestore();
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

    it('should handle rollback marking errors', async () => {
      const error = new Error('Rollback failed');
      mockClient.insert.mockRejectedValue(error);

      await expect(db.markMigrationRolledBack('20240101120000')).rejects.toThrow('Rollback failed');
    });
  });

  describe('getLatestMigration', () => {
    it('should return latest migration version', async () => {
      const mockMigrations = [
        { version: '20240101120000', active: 1, created_at: '2024-01-01T12:00:00Z' },
        { version: '20240102120000', active: 1, created_at: '2024-01-02T12:00:00Z' }
      ];

      const mockResultSet = {
        json: jest.fn().mockResolvedValue({ data: mockMigrations })
      };
      mockClient.query.mockResolvedValue(mockResultSet);

      const result = await db.getLatestMigration();

      expect(result).toBe('20240102120000');
    });

    it('should return undefined when no migrations exist', async () => {
      const mockResultSet = {
        json: jest.fn().mockResolvedValue({ data: [] })
      };
      mockClient.query.mockResolvedValue(mockResultSet);

      const result = await db.getLatestMigration();

      expect(result).toBeUndefined();
    });
  });

  describe('getDatabaseTables', () => {
    it('should return list of tables', async () => {
      const mockTables = [{ name: 'users', database: 'test_db' }, { name: 'orders', database: 'prod_db' }];
      const mockResultSet = {
        json: jest.fn().mockResolvedValue({ data: mockTables })
      };
      mockClient.query.mockResolvedValue(mockResultSet);

      const result = await db.getDatabaseTables();

      expect(mockClient.query).toHaveBeenCalledWith({
        query: "SELECT name, database FROM system.tables WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA') AND engine NOT LIKE '%View' AND engine != 'MaterializedView'"
      });
      expect(result).toEqual(mockTables);
    });

    it('should return empty array on error', async () => {
      mockClient.query.mockRejectedValue(new Error('Query failed'));

      const result = await db.getDatabaseTables();

      expect(result).toEqual([]);
    });
  });

  describe('getDatabaseMaterializedViews', () => {
    it('should return list of materialized views', async () => {
      const mockViews = [{ name: 'user_stats_mv', database: 'test_db' }, { name: 'order_stats_mv', database: 'prod_db' }];
      const mockResultSet = {
        json: jest.fn().mockResolvedValue({ data: mockViews })
      };
      mockClient.query.mockResolvedValue(mockResultSet);

      const result = await db.getDatabaseMaterializedViews();

      expect(mockClient.query).toHaveBeenCalledWith({
        query: "SELECT name, database FROM system.tables WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA') AND engine = 'MaterializedView'"
      });
      expect(result).toEqual(mockViews);
    });

    it('should return empty array on error', async () => {
      const consoleSpy = jest.spyOn(console, 'error').mockImplementation();
      const error = new Error('Materialized views query failed');
      mockClient.query.mockRejectedValue(error);

      const result = await db.getDatabaseMaterializedViews();

      expect(result).toEqual([]);
      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('❌ Failed to get materialized views:'),
        error
      );
      
      consoleSpy.mockRestore();
    });
  });

  describe('getDatabaseDictionaries', () => {
    it('should return list of dictionaries', async () => {
      const mockDictionaries = [{ name: 'countries_dict', database: 'test_db' }, { name: 'languages_dict', database: 'prod_db' }];
      const mockResultSet = {
        json: jest.fn().mockResolvedValue({ data: mockDictionaries })
      };
      mockClient.query.mockResolvedValue(mockResultSet);

      const result = await db.getDatabaseDictionaries();

      expect(mockClient.query).toHaveBeenCalledWith({
        query: "SELECT name, database FROM system.dictionaries WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')"
      });
      expect(result).toEqual(mockDictionaries);
    });

    it('should return empty array on error', async () => {
      const consoleSpy = jest.spyOn(console, 'error').mockImplementation();
      const error = new Error('Dictionaries query failed');
      mockClient.query.mockRejectedValue(error);

      const result = await db.getDatabaseDictionaries();

      expect(result).toEqual([]);
      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('❌ Failed to get dictionaries:'),
        error
      );
      
      consoleSpy.mockRestore();
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

  describe('getDatabaseSchema', () => {

    it('should handle errors when getting CREATE statements for tables', async () => {
      const consoleSpy = jest.spyOn(console, 'warn').mockImplementation();
      
      // Mock with one table that will fail
      const mockTables = [{ name: 'bad_table', database: 'test_db' }];
      const mockTablesResultSet = { json: jest.fn().mockResolvedValue({ data: mockTables }) };
      
      mockClient.query
        .mockResolvedValueOnce(mockTablesResultSet)     // getDatabaseTables succeeds
        .mockResolvedValueOnce({ json: jest.fn().mockResolvedValue({ data: [] }) })  // getDatabaseMaterializedViews
        .mockResolvedValueOnce({ json: jest.fn().mockResolvedValue({ data: [] }) })  // getDatabaseDictionaries
        .mockRejectedValueOnce(new Error('CREATE TABLE failed'));  // getCreateTableQuery fails

      const result = await db.getDatabaseSchema();

      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('⚠️ Could not get CREATE TABLE for test_db.bad_table'),
        expect.any(Error)
      );
      
      expect(result).toEqual({});
      
      consoleSpy.mockRestore();
    });

    it('should handle errors when getting CREATE statements for views', async () => {
      const consoleSpy = jest.spyOn(console, 'warn').mockImplementation();
      
      // Mock with one view that will fail
      const mockViews = [{ name: 'bad_view', database: 'test_db' }];
      const mockViewsResultSet = { json: jest.fn().mockResolvedValue({ data: mockViews }) };
      
      mockClient.query
        .mockResolvedValueOnce({ json: jest.fn().mockResolvedValue({ data: [] }) })  // getDatabaseTables
        .mockResolvedValueOnce(mockViewsResultSet)     // getDatabaseMaterializedViews succeeds
        .mockResolvedValueOnce({ json: jest.fn().mockResolvedValue({ data: [] }) })  // getDatabaseDictionaries
        .mockRejectedValueOnce(new Error('CREATE VIEW failed'));  // getCreateTableQuery fails

      const result = await db.getDatabaseSchema();

      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('⚠️ Could not get CREATE VIEW for test_db.bad_view'),
        expect.any(Error)
      );
      
      expect(result).toEqual({});
      
      consoleSpy.mockRestore();
    });

    it('should handle errors when getting CREATE statements for dictionaries', async () => {
      const consoleSpy = jest.spyOn(console, 'warn').mockImplementation();
      
      // Mock with one dictionary that will fail
      const mockDictionaries = [{ name: 'bad_dict', database: 'test_db' }];
      const mockDictionariesResultSet = { json: jest.fn().mockResolvedValue({ data: mockDictionaries }) };
      
      mockClient.query
        .mockResolvedValueOnce({ json: jest.fn().mockResolvedValue({ data: [] }) })  // getDatabaseTables
        .mockResolvedValueOnce({ json: jest.fn().mockResolvedValue({ data: [] }) })  // getDatabaseMaterializedViews
        .mockResolvedValueOnce(mockDictionariesResultSet)     // getDatabaseDictionaries succeeds
        .mockRejectedValueOnce(new Error('CREATE DICTIONARY failed'));  // getCreateTableQuery fails

      const result = await db.getDatabaseSchema();

      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('⚠️ Could not get CREATE DICTIONARY for test_db.bad_dict'),
        expect.any(Error)
      );
      
      expect(result).toEqual({});
      
      consoleSpy.mockRestore();
    });
  });

  describe('getDatabaseTablesForDb', () => {
    it('should return list of tables for specific database', async () => {
      const mockTables = [{ name: 'users' }, { name: 'orders' }];
      const mockResultSet = {
        json: jest.fn().mockResolvedValue({ data: mockTables })
      };
      mockClient.query.mockResolvedValue(mockResultSet);

      const result = await db.getDatabaseTablesForDb('custom_db');

      expect(mockClient.query).toHaveBeenCalledWith({
        query: "SELECT name FROM system.tables WHERE database = 'custom_db' AND engine NOT LIKE '%View' AND engine != 'MaterializedView'"
      });
      expect(result).toEqual(mockTables);
    });

    it('should return empty array on error', async () => {
      mockClient.query.mockRejectedValue(new Error('Query failed'));

      const result = await db.getDatabaseTablesForDb('custom_db');

      expect(result).toEqual([]);
    });
  });

  describe('getDatabaseMaterializedViewsForDb', () => {
    it('should return list of materialized views for specific database', async () => {
      const mockViews = [{ name: 'user_stats_mv' }, { name: 'order_stats_mv' }];
      const mockResultSet = {
        json: jest.fn().mockResolvedValue({ data: mockViews })
      };
      mockClient.query.mockResolvedValue(mockResultSet);

      const result = await db.getDatabaseMaterializedViewsForDb('custom_db');

      expect(mockClient.query).toHaveBeenCalledWith({
        query: "SELECT name FROM system.tables WHERE database = 'custom_db' AND engine = 'MaterializedView'"
      });
      expect(result).toEqual(mockViews);
    });

    it('should return empty array on error', async () => {
      const consoleSpy = jest.spyOn(console, 'error').mockImplementation();
      const error = new Error('Materialized views query failed');
      mockClient.query.mockRejectedValue(error);

      const result = await db.getDatabaseMaterializedViewsForDb('custom_db');

      expect(result).toEqual([]);
      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('❌ Failed to get materialized views for database custom_db:'),
        error
      );
      
      consoleSpy.mockRestore();
    });
  });

  describe('getDatabaseDictionariesForDb', () => {
    it('should return list of dictionaries for specific database', async () => {
      const mockDictionaries = [{ name: 'countries_dict' }, { name: 'languages_dict' }];
      const mockResultSet = {
        json: jest.fn().mockResolvedValue({ data: mockDictionaries })
      };
      mockClient.query.mockResolvedValue(mockResultSet);

      const result = await db.getDatabaseDictionariesForDb('custom_db');

      expect(mockClient.query).toHaveBeenCalledWith({
        query: "SELECT name FROM system.dictionaries WHERE database = 'custom_db'"
      });
      expect(result).toEqual(mockDictionaries);
    });

    it('should return empty array on error', async () => {
      const consoleSpy = jest.spyOn(console, 'error').mockImplementation();
      const error = new Error('Dictionaries query failed');
      mockClient.query.mockRejectedValue(error);

      const result = await db.getDatabaseDictionariesForDb('custom_db');

      expect(result).toEqual([]);
      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('❌ Failed to get dictionaries for database custom_db:'),
        error
      );
      
      consoleSpy.mockRestore();
    });
  });

  describe('getCreateTableQueryForDb', () => {
    it('should handle materialized views with TABLE query type', async () => {
      const mockResponse = `CREATE MATERIALIZED VIEW test_view\\n(\\n    \`id\` UInt64\\n)\\nENGINE = MergeTree()\\nORDER BY id`;
      const mockResultSet = {
        json: jest.fn().mockResolvedValue({ data: [{ statement: mockResponse }] })
      };
      mockClient.query.mockResolvedValue(mockResultSet);

      const result = await db.getCreateTableQueryForDb('test_view', 'test_db', 'VIEW');

      expect(mockClient.query).toHaveBeenCalledWith({
        query: 'SHOW CREATE TABLE test_db.test_view'
      });
      // Verify that escape characters are properly converted
      expect(result).toContain('\n');
      expect(result).not.toContain('\\n');
      expect(result).toContain('CREATE MATERIALIZED VIEW test_view');
    });

    it('should handle tables with TABLE query type', async () => {
      const mockResponse = `CREATE TABLE test_table\\n(\\n    \`id\` UInt64\\n)\\nENGINE = MergeTree()\\nORDER BY id`;
      const mockResultSet = {
        json: jest.fn().mockResolvedValue({ data: [{ statement: mockResponse }] })
      };
      mockClient.query.mockResolvedValue(mockResultSet);

      const result = await db.getCreateTableQueryForDb('test_table', 'test_db', 'TABLE');

      expect(mockClient.query).toHaveBeenCalledWith({
        query: 'SHOW CREATE TABLE test_db.test_table'
      });
      expect(result).toContain('\n');
      expect(result).not.toContain('\\n');
    });

    it('should handle dictionaries with DICTIONARY query type', async () => {
      const mockResponse = `CREATE DICTIONARY test_dict\\n(\\n    \`id\` UInt64\\n)\\nPRIMARY KEY id\\nSOURCE(CLICKHOUSE(DB \\'test\\' TABLE \\'source\\'))\\nLIFETIME(600)`;
      const mockResultSet = {
        json: jest.fn().mockResolvedValue({ data: [{ statement: mockResponse }] })
      };
      mockClient.query.mockResolvedValue(mockResultSet);

      const result = await db.getCreateTableQueryForDb('test_dict', 'test_db', 'DICTIONARY');

      expect(mockClient.query).toHaveBeenCalledWith({
        query: 'SHOW CREATE DICTIONARY test_db.test_dict'
      });
      expect(result).toContain('\n');
      expect(result).not.toContain('\\n');
      expect(result).toContain("'test'");
      expect(result).not.toContain("\\'test\\'");
    });

    it('should unescape quotes and backslashes properly', async () => {
      const mockResponse = `CREATE TABLE test\\n(\\n    \`name\` String DEFAULT \\'test\\',\\n    \`path\` String DEFAULT \\'C:\\\\\\\\test\\'\\n)`;
      const mockResultSet = {
        json: jest.fn().mockResolvedValue({ data: [{ statement: mockResponse }] })
      };
      mockClient.query.mockResolvedValue(mockResultSet);

      const result = await db.getCreateTableQueryForDb('test', 'test_db', 'TABLE');

      expect(result).toContain("DEFAULT 'test'");
      expect(result).toContain("DEFAULT 'C:\\\\test'");
      expect(result).not.toContain("\\'");
      expect(result).not.toContain("\\\\\\\\");
    });

    it('should handle verbose logging', async () => {
      const verboseContext = { ...context, verbose: true };
      const verboseDb = new Db(verboseContext);
      const consoleSpy = jest.spyOn(console, 'log').mockImplementation();
      
      const mockResponse = `CREATE TABLE test\\n(\\n    \`id\` UInt64\\n)`;
      const mockResultSet = {
        json: jest.fn().mockResolvedValue({ data: [{ statement: mockResponse }] })
      };
      mockClient.query.mockResolvedValue(mockResultSet);

      await verboseDb.getCreateTableQueryForDb('test', 'test_db', 'TABLE');

      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('🔍  Executing schema query: SHOW CREATE TABLE test_db.test')
      );
      
      consoleSpy.mockRestore();
    });

    it('should handle query execution errors', async () => {
      const error = new Error('Query failed');
      mockClient.query.mockRejectedValue(error);

      await expect(db.getCreateTableQueryForDb('test', 'test_db', 'TABLE')).rejects.toThrow('Query failed');
    });
  });
});