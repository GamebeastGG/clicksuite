import * as path from 'path';

describe('Utility Functions', () => {
  describe('Migration File Regex', () => {
    const MIGRATION_FILE_REGEX = /^(\d{14})_([\w-]+)\.yml$/;

    it('should match valid migration filenames', () => {
      const validFilenames = [
        '20240101120000_create_users.yml',
        '20231225235959_add_index.yml',
        '20240301000000_update-schema.yml',
        '20240101120000_migration-with-dashes.yml',
        '20240101120000_migration_with_underscores.yml',
        '20240101120000_MixedCase.yml'
      ];

      validFilenames.forEach(filename => {
        const match = filename.match(MIGRATION_FILE_REGEX);
        expect(match).not.toBeNull();
        expect(match![1]).toMatch(/^\d{14}$/);
        expect(match![2]).toMatch(/^[\w-]+$/);
      });
    });

    it('should not match invalid migration filenames', () => {
      const invalidFilenames = [
        'invalid_format.yml',
        '2024010112000_too_short.yml',
        '202401011200000_too_long.yml',
        '20240101120000_invalid@chars.yml',
        '20240101120000_spaces in name.yml',
        '20240101120000_name.yaml',
        '20240101120000_name.txt',
        '20240101120000_.yml',
        '_20240101120000_name.yml',
        'README.md'
      ];

      invalidFilenames.forEach(filename => {
        const match = filename.match(MIGRATION_FILE_REGEX);
        expect(match).toBeNull();
      });
    });

    it('should extract version and name correctly', () => {
      const filename = '20240101120000_create_users_table.yml';
      const match = filename.match(MIGRATION_FILE_REGEX);

      expect(match).not.toBeNull();
      expect(match![1]).toBe('20240101120000');
      expect(match![2]).toBe('create_users_table');
    });
  });

  describe('formatSQL Function', () => {
    const formatSQL = (sql?: string, tableName?: string): string | undefined => {
      if (!sql || !tableName) {
        return sql;
      }
      return sql.replace(/\{table\}/g, tableName);
    };

    it('should replace table placeholders', () => {
      const sql = 'CREATE TABLE {table} (id UInt64) ENGINE = MergeTree()';
      const tableName = 'users';

      const result = formatSQL(sql, tableName);

      expect(result).toBe('CREATE TABLE users (id UInt64) ENGINE = MergeTree()');
    });

    it('should replace multiple table placeholders', () => {
      const sql = 'CREATE TABLE {table} AS SELECT * FROM {table}_backup';
      const tableName = 'users';

      const result = formatSQL(sql, tableName);

      expect(result).toBe('CREATE TABLE users AS SELECT * FROM users_backup');
    });

    it('should return original SQL when no table name provided', () => {
      const sql = 'CREATE TABLE {table}';

      const result = formatSQL(sql);

      expect(result).toBe(sql);
    });

    it('should return undefined when SQL is undefined', () => {
      const result = formatSQL(undefined, 'users');

      expect(result).toBeUndefined();
    });

    it('should handle empty table name', () => {
      const sql = 'CREATE TABLE {table}';

      const result = formatSQL(sql, '');

      expect(result).toBe(sql);
    });

    it('should handle SQL without placeholders', () => {
      const sql = 'CREATE TABLE users (id UInt64)';
      const tableName = 'products';

      const result = formatSQL(sql, tableName);

      expect(result).toBe('CREATE TABLE users (id UInt64)');
    });
  });

  describe('Path Resolution', () => {
    it('should resolve relative paths correctly', () => {
      const basePath = '/base/path';
      const relativePath = 'migrations';

      const resolved = path.resolve(basePath, relativePath);

      expect(resolved).toBe('/base/path/migrations');
      expect(path.isAbsolute(resolved)).toBe(true);
    });

    it('should keep absolute paths unchanged', () => {
      const absolutePath = '/absolute/path/migrations';

      const resolved = path.resolve(absolutePath);

      expect(resolved).toBe('/absolute/path/migrations');
      expect(path.isAbsolute(resolved)).toBe(true);
    });

    it('should handle current working directory resolution', () => {
      const relativePath = 'migrations';

      const resolved = path.resolve(process.cwd(), relativePath);

      expect(path.isAbsolute(resolved)).toBe(true);
      expect(resolved).toContain('migrations');
    });
  });

  describe('Migration Name Sanitization', () => {
    const sanitizeMigrationName = (name: string): string => {
      return name.replace(/\s+/g, '-').replace(/[^a-zA-Z0-9_-]/g, '').toLowerCase();
    };

    it('should replace spaces with dashes', () => {
      const name = 'create users table';
      const sanitized = sanitizeMigrationName(name);

      expect(sanitized).toBe('create-users-table');
    });

    it('should remove special characters', () => {
      const name = 'create@user$table!';
      const sanitized = sanitizeMigrationName(name);

      expect(sanitized).toBe('createusertable');
    });

    it('should convert to lowercase', () => {
      const name = 'CreateUsersTable';
      const sanitized = sanitizeMigrationName(name);

      expect(sanitized).toBe('createuserstable');
    });

    it('should handle multiple spaces', () => {
      const name = 'create   multiple   spaces';
      const sanitized = sanitizeMigrationName(name);

      expect(sanitized).toBe('create-multiple-spaces');
    });

    it('should preserve underscores and dashes', () => {
      const name = 'create_users-table';
      const sanitized = sanitizeMigrationName(name);

      expect(sanitized).toBe('create_users-table');
    });

    it('should handle empty string', () => {
      const name = '';
      const sanitized = sanitizeMigrationName(name);

      expect(sanitized).toBe('');
    });

    it('should handle string with only special characters', () => {
      const name = '@#$%^&*()';
      const sanitized = sanitizeMigrationName(name);

      expect(sanitized).toBe('');
    });
  });

  describe('Timestamp Generation', () => {
    it('should generate correct timestamp format', () => {
      const testDate = new Date('2024-01-01T12:00:00.000Z');
      const timestamp = testDate.toISOString().replace(/[-:T.]/g, '').slice(0, 14);

      expect(timestamp).toBe('20240101120000');
      expect(timestamp).toMatch(/^\d{14}$/);
    });

    it('should handle different dates correctly', () => {
      const testCases = [
        { date: '2024-12-31T23:59:59.999Z', expected: '20241231235959' },
        { date: '2024-01-01T00:00:00.000Z', expected: '20240101000000' },
        { date: '2024-06-15T14:30:45.123Z', expected: '20240615143045' }
      ];

      testCases.forEach(({ date, expected }) => {
        const testDate = new Date(date);
        const timestamp = testDate.toISOString().replace(/[-:T.]/g, '').slice(0, 14);

        expect(timestamp).toBe(expected);
      });
    });
  });

  describe('Environment Variable Handling', () => {
    const getEnvValue = (key: string, defaultValue: string): string => {
      return process.env[key] || defaultValue;
    };

    const getOptionalEnvValue = (key: string): string | undefined => {
      const value = process.env[key];
      return value === '' ? undefined : value;
    };

    it('should return environment value when set', () => {
      process.env.TEST_VAR = 'test_value';

      const result = getEnvValue('TEST_VAR', 'default');

      expect(result).toBe('test_value');

      delete process.env.TEST_VAR;
    });

    it('should return default value when environment variable not set', () => {
      delete process.env.TEST_VAR;

      const result = getEnvValue('TEST_VAR', 'default');

      expect(result).toBe('default');
    });

    it('should handle optional environment variables', () => {
      process.env.OPTIONAL_VAR = 'value';

      let result = getOptionalEnvValue('OPTIONAL_VAR');
      expect(result).toBe('value');

      process.env.OPTIONAL_VAR = '';
      result = getOptionalEnvValue('OPTIONAL_VAR');
      expect(result).toBeUndefined();

      delete process.env.OPTIONAL_VAR;
      result = getOptionalEnvValue('OPTIONAL_VAR');
      expect(result).toBeUndefined();
    });

    it('should detect CI environment', () => {
      const isCIEnvironment = (): boolean => {
        return !!process.env.CI;
      };

      process.env.CI = 'true';
      expect(isCIEnvironment()).toBe(true);

      process.env.CI = '1';
      expect(isCIEnvironment()).toBe(true);

      delete process.env.CI;
      expect(isCIEnvironment()).toBe(false);

      process.env.CI = '';
      expect(isCIEnvironment()).toBe(false);
    });
  });

  describe('Array Sorting', () => {
    it('should sort migration versions correctly', () => {
      const versions = [
        '20240103120000',
        '20240101120000',
        '20240102120000',
        '20240101110000',
        '20240101130000'
      ];

      const sorted = versions.sort((a, b) => a.localeCompare(b));

      expect(sorted).toEqual([
        '20240101110000',
        '20240101120000',
        '20240101130000',
        '20240102120000',
        '20240103120000'
      ]);
    });

    it('should sort migrations by version in reverse order', () => {
      const migrations = [
        { version: '20240101120000', name: 'first' },
        { version: '20240103120000', name: 'third' },
        { version: '20240102120000', name: 'second' }
      ];

      const sorted = migrations.sort((a, b) => b.version.localeCompare(a.version));

      expect(sorted[0].version).toBe('20240103120000');
      expect(sorted[1].version).toBe('20240102120000');
      expect(sorted[2].version).toBe('20240101120000');
    });
  });
});