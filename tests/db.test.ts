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
      protocol: 'http',
      host: 'localhost',
      port: '8123',
      username: 'default',
      password: '',
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
    it('should create ClickHouse client with correct configuration', () => {
      expect(mockCreateClient).toHaveBeenCalledWith({
        url: 'http://localhost:8123',
        username: 'default',
        password: '',
        database: 'test_db',
      });
    });

    it('should handle HTTPS protocol', () => {
      const httpsContext = { ...context, protocol: 'https', port: '8443' };
      new Db(httpsContext);

      expect(mockCreateClient).toHaveBeenCalledWith({
        url: 'https://localhost:8443',
        username: 'default',
        password: '',
        database: 'test_db',
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
        query: expect.stringContaining('CREATE TABLE IF NOT EXISTS test_db.__clicksuite_migrations'),
        clickhouse_settings: { wait_end_of_query: 1 }
      });

      const query = mockClient.command.mock.calls[0][0].query;
      expect(query).toContain('ReplacingMergeTree()');
      expect(query).not.toContain('ON CLUSTER');
    });

    it('should create migrations table with cluster', async () => {
      const clusterContext = { ...context, cluster: 'test_cluster' };
      const clusterDb = new Db(clusterContext);
      mockClient.command.mockResolvedValue(undefined);

      await clusterDb.initMigrationsTable();

      const query = mockClient.command.mock.calls[0][0].query;
      expect(query).toContain('ON CLUSTER test_cluster');
      expect(query).toContain('ReplicatedReplacingMergeTree');
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
        query: 'SELECT version, active, created_at FROM test_db.__clicksuite_migrations WHERE active = 1 ORDER BY version ASC',
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
        query: 'SELECT version, active, created_at FROM test_db.__clicksuite_migrations ORDER BY version ASC',
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
  });

  describe('markMigrationApplied', () => {
    it('should mark migration as applied', async () => {
      const version = '20240101120000';
      mockClient.insert.mockResolvedValue(undefined);
      mockClient.command.mockResolvedValue(undefined); // for optimize call

      await db.markMigrationApplied(version);

      expect(mockClient.insert).toHaveBeenCalledWith({
        table: 'test_db.__clicksuite_migrations',
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
        table: 'test_db.__clicksuite_migrations',
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
        query: 'SHOW TABLES FROM test_db',
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
        query: 'TRUNCATE TABLE IF EXISTS test_db.__clicksuite_migrations ',
        clickhouse_settings: { wait_end_of_query: 1 }
      });
    });

    it('should clear migrations table with cluster', async () => {
      const clusterContext = { ...context, cluster: 'test_cluster' };
      const clusterDb = new Db(clusterContext);
      mockClient.command.mockResolvedValue(undefined);

      await clusterDb.clearMigrationsTable();

      const query = mockClient.command.mock.calls[0][0].query;
      expect(query).toContain('ON CLUSTER test_cluster');
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
        query: 'OPTIMIZE TABLE test_db.__clicksuite_migrations FINAL',
        clickhouse_settings: { wait_end_of_query: 1 }
      });
    });

    it('should handle optimize errors gracefully', async () => {
      const error = new Error('Optimize failed');
      mockClient.command.mockRejectedValue(error);

      await expect(db.optimizeMigrationTable()).rejects.toThrow('Optimize failed');
    });
  });
});