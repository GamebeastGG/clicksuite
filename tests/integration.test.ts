import { Runner } from '../src/runner';
import { Db } from '../src/db';
import { Context } from '../src/types';
import * as fs from 'fs/promises';
import * as path from 'path';
import * as os from 'os';

describe('Integration Tests', () => {
  let context: Context;
  let tempDir: string;
  let runner: Runner;

  beforeEach(async () => {
    tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'clicksuite-test-'));
    
    context = {
      url: 'http://default@localhost:8123/test_db',
      migrationsDir: path.join(tempDir, 'migrations'),
      environment: 'test',
      nonInteractive: true
    };

    runner = new Runner(context);
  });

  afterEach(async () => {
    try {
      await fs.rm(tempDir, { recursive: true, force: true });
    } catch (error) {
      // Ignore cleanup errors
    }
  });

  describe('Migration File Generation and Parsing', () => {
    it('should generate and parse migration files correctly', async () => {
      await fs.mkdir(context.migrationsDir, { recursive: true });

      jest.spyOn(Date.prototype, 'toISOString').mockReturnValue('2024-01-01T12:00:00.000Z');

      await runner.generate('create users table');

      const files = await fs.readdir(context.migrationsDir);
      expect(files).toHaveLength(1);
      expect(files[0]).toBe('20240101120000_create-users-table.yml');

      const filePath = path.join(context.migrationsDir, files[0]);
      const content = await fs.readFile(filePath, 'utf-8');

      expect(content).toContain('version: "20240101120000"');
      expect(content).toContain('name: "create users table"');
      expect(content).toContain('table: "your_table_name"');
      expect(content).toContain('development: &development_defaults');
      expect(content).toContain('<<: *development_defaults');

      const migrations = await runner['_getLocalMigrations']();
      expect(migrations).toHaveLength(1);
      expect(migrations[0]).toMatchObject({
        version: '20240101120000',
        name: 'create users table',
        filePath
      });
    });

    it('should handle multiple migration files in correct order', async () => {
      await fs.mkdir(context.migrationsDir, { recursive: true });

      const migrationFiles = [
        '20240101120000_first_migration.yml',
        '20240102120000_second_migration.yml',
        '20240103120000_third_migration.yml'
      ];

      for (const filename of migrationFiles) {
        const content = `
version: "${filename.split('_')[0]}"
name: "${filename.replace('.yml', '').replace(/_/g, ' ')}"
table: "test_table"

development:
  up: "CREATE TABLE test_table"
  down: "DROP TABLE test_table"

test:
  up: "CREATE TABLE test_table"
  down: "DROP TABLE test_table"
`;
        await fs.writeFile(path.join(context.migrationsDir, filename), content);
      }

      const migrations = await runner['_getLocalMigrations']();
      expect(migrations).toHaveLength(3);
      expect(migrations[0].version).toBe('20240101120000');
      expect(migrations[1].version).toBe('20240102120000');
      expect(migrations[2].version).toBe('20240103120000');
    });

    it('should handle environment-specific configurations', async () => {
      await fs.mkdir(context.migrationsDir, { recursive: true });

      const yamlContent = `
version: "20240101120000"
name: "environment test"
table: "users"

development:
  up: "CREATE TABLE {table} (id UInt64) ENGINE = MergeTree() ORDER BY id"
  down: "DROP TABLE {table}"
  settings:
    max_execution_time: 30

test:
  up: "CREATE TABLE {table} (id UInt64, test_flag UInt8) ENGINE = MergeTree() ORDER BY id"
  down: "DROP TABLE {table}"
  settings:
    max_execution_time: 60

production:
  up: "CREATE TABLE {table} ON CLUSTER prod (id UInt64) ENGINE = ReplicatedMergeTree ORDER BY id"
  down: "DROP TABLE {table} ON CLUSTER prod"
  settings:
    max_execution_time: 120
`;

      const filePath = path.join(context.migrationsDir, '20240101120000_env_test.yml');
      await fs.writeFile(filePath, yamlContent);

      const migrations = await runner['_getLocalMigrations']();
      expect(migrations).toHaveLength(1);

      const migration = migrations[0];
      expect(migration.upSQL).toContain('test_flag UInt8');
      expect(migration.upSQL).toContain('CREATE TABLE users');
      expect(migration.downSQL).toBe('DROP TABLE users');
      expect(migration.querySettings).toEqual({ max_execution_time: 60 });
    });

    it('should use development as fallback for missing environment', async () => {
      await fs.mkdir(context.migrationsDir, { recursive: true });

      // Create a migration with only development configuration
      const yamlContent = `
version: "20240101120000"
name: "fallback test"
table: "test_table"

development:
  up: "CREATE TABLE {table}"
  down: "DROP TABLE {table}"
  settings:
    max_execution_time: 30
`;

      const filePath = path.join(context.migrationsDir, '20240101120000_fallback.yml');
      await fs.writeFile(filePath, yamlContent);

      const migrations = await runner['_getLocalMigrations']();
      expect(migrations).toHaveLength(1);

      const migration = migrations[0];
      expect(migration.upSQL).toBe('CREATE TABLE test_table');
      expect(migration.downSQL).toBe('DROP TABLE test_table');
      expect(migration.querySettings).toEqual({ max_execution_time: 30 });
    });
  });

  describe('Migration Status Logic', () => {
    it('should correctly identify migration states', async () => {
      await fs.mkdir(context.migrationsDir, { recursive: true });

      const migrationContent = `
version: "20240101120000"
name: "status test"
table: "test_table"

test:
  up: "CREATE TABLE test_table"
  down: "DROP TABLE test_table"
`;

      await fs.writeFile(
        path.join(context.migrationsDir, '20240101120000_status_test.yml'),
        migrationContent
      );

      const mockDb = {
        getAllMigrationRecords: jest.fn().mockResolvedValue([
          { version: '20240101120000', active: 1, created_at: '2024-01-01T12:00:00Z' }
        ])
      };

      (runner as any).db = mockDb;

      await runner.status();

      expect(mockDb.getAllMigrationRecords).toHaveBeenCalled();
    });
  });

  describe('File System Edge Cases', () => {
    it('should handle missing migrations directory', async () => {
      const migrations = await runner['_getLocalMigrations']();
      expect(migrations).toEqual([]);
    });

    it('should ignore non-migration files', async () => {
      await fs.mkdir(context.migrationsDir, { recursive: true });

      await fs.writeFile(path.join(context.migrationsDir, 'README.md'), '# Migrations');
      await fs.writeFile(path.join(context.migrationsDir, 'config.json'), '{}');
      await fs.writeFile(path.join(context.migrationsDir, 'invalid_name.yml'), 'content');

      const migrationContent = `
version: "20240101120000"
name: "valid migration"
test:
  up: "CREATE TABLE valid"
  down: "DROP TABLE valid"
`;

      await fs.writeFile(
        path.join(context.migrationsDir, '20240101120000_valid.yml'),
        migrationContent
      );

      const migrations = await runner['_getLocalMigrations']();
      expect(migrations).toHaveLength(1);
      expect(migrations[0].version).toBe('20240101120000');
    });

    it('should handle malformed YAML files gracefully', async () => {
      await fs.mkdir(context.migrationsDir, { recursive: true });

      await fs.writeFile(
        path.join(context.migrationsDir, '20240101120000_invalid.yml'),
        'invalid: yaml: content: ['
      );

      const validContent = `
version: "20240102120000"
name: "valid migration"
test:
  up: "CREATE TABLE valid"
  down: "DROP TABLE valid"
`;

      await fs.writeFile(
        path.join(context.migrationsDir, '20240102120000_valid.yml'),
        validContent
      );

      const migrations = await runner['_getLocalMigrations']();
      expect(migrations).toHaveLength(1);
      expect(migrations[0].version).toBe('20240102120000');
    });
  });

  describe('Context Resolution', () => {
    it('should resolve relative migrations directory to absolute', () => {
      const relativeContext = {
        ...context,
        migrationsDir: 'relative/path'
      };

      const relativeRunner = new Runner(relativeContext);
      const resolvedDir = relativeRunner['context'].migrationsDir;

      expect(path.isAbsolute(resolvedDir)).toBe(true);
      expect(resolvedDir).toContain('relative/path');
    });
  });

  describe('Migration Name Sanitization', () => {
    it('should sanitize complex migration names', async () => {
      await fs.mkdir(context.migrationsDir, { recursive: true });

      const complexNames = [
        'Create User$@ T@ble!',
        'add-index_on users.email',
        'Update   multiple   spaces',
        'Special#Characters&More',
        '日本語名前'
      ];

      const expectedSanitized = [
        'create-user-tble',
        'add-index_on-usersemail',
        'update-multiple-spaces',
        'specialcharactersmore',
        ''
      ];

      const mockToISOString = jest.spyOn(Date.prototype, 'toISOString');
      
      for (let i = 0; i < complexNames.length; i++) {
        mockToISOString.mockReturnValueOnce(`2024-01-0${i + 1}T12:00:00.000Z`);
        await runner.generate(complexNames[i]);
      }

      const files = await fs.readdir(context.migrationsDir);
      expect(files).toHaveLength(5);

      for (let i = 0; i < expectedSanitized.length; i++) {
        const expectedFile = `2024010${i + 1}120000_${expectedSanitized[i]}.yml`;
        if (expectedSanitized[i]) {
          expect(files).toContain(expectedFile);
        }
      }
    });
  });
});