import dotenv from 'dotenv';
import { Runner } from '../src/runner';

jest.mock('dotenv', () => ({
  __esModule: true,
  default: { config: jest.fn() },
}));

jest.mock('fs/promises', () => ({
  __esModule: true,
  mkdir: jest.fn().mockResolvedValue(undefined),
}));

jest.mock('../src/runner');

describe('CLI commands (cli.ts)', () => {
  let originalArgv: string[];
  let mockRunnerInstance: jest.Mocked<Runner>;

  beforeEach(() => {
    originalArgv = process.argv;
    process.env.CLICKHOUSE_URL = 'http://default@localhost:8123/test_db';

    mockRunnerInstance = {
      init: jest.fn().mockResolvedValue(undefined),
      generate: jest.fn().mockResolvedValue(undefined),
      status: jest.fn().mockResolvedValue(undefined),
      migrate: jest.fn().mockResolvedValue(undefined),
      up: jest.fn().mockResolvedValue(undefined),
      down: jest.fn().mockResolvedValue(undefined),
      reset: jest.fn().mockResolvedValue(undefined),
      schemaLoad: jest.fn().mockResolvedValue(undefined),
    } as any;

    (Runner as unknown as jest.MockedClass<typeof Runner>).mockImplementation(() => mockRunnerInstance);

    // Silence console during tests
    jest.spyOn(console, 'log').mockImplementation(() => {});
    jest.spyOn(console, 'error').mockImplementation(() => {});
  });

  afterEach(() => {
    process.argv = originalArgv;
    jest.restoreAllMocks();
    jest.clearAllMocks();
  });

  it('runs init command', async () => {
    process.argv = ['node', 'cli', 'init', '--non-interactive'];
    const { createCli } = require('../src/cli');
    await createCli().parseAsync();

    expect(mockRunnerInstance.init).toHaveBeenCalled();
  });

  it('runs generate command', async () => {
    process.argv = ['node', 'cli', 'generate', 'add_users_table', '--non-interactive'];
    const { createCli } = require('../src/cli');
    await createCli().parseAsync();

    expect(mockRunnerInstance.generate).toHaveBeenCalledWith('add_users_table');
  });

  it('runs migrate:status command', async () => {
    process.argv = ['node', 'cli', 'migrate:status', '--non-interactive'];
    const { createCli } = require('../src/cli');
    await createCli().parseAsync();

    expect(mockRunnerInstance.status).toHaveBeenCalled();
  });

  it('runs migrate command', async () => {
    process.argv = ['node', 'cli', 'migrate', '--non-interactive'];
    const { createCli } = require('../src/cli');
    await createCli().parseAsync();

    expect(mockRunnerInstance.migrate).toHaveBeenCalled();
  });

  it('runs migrate:up with version', async () => {
    process.argv = ['node', 'cli', 'migrate:up', '20240101120000', '--non-interactive'];
    const { createCli } = require('../src/cli');
    await createCli().parseAsync();

    expect(mockRunnerInstance.up).toHaveBeenCalledWith('20240101120000');
  });

  it('runs migrate:down with version', async () => {
    process.argv = ['node', 'cli', 'migrate:down', '20240102120000', '--non-interactive'];
    const { createCli } = require('../src/cli');
    await createCli().parseAsync();

    expect(mockRunnerInstance.down).toHaveBeenCalledWith('20240102120000');
  });

  it('runs migrate:reset', async () => {
    process.argv = ['node', 'cli', 'migrate:reset', '--non-interactive'];
    const { createCli } = require('../src/cli');
    await createCli().parseAsync();

    expect(mockRunnerInstance.reset).toHaveBeenCalled();
  });

  it('runs schema:load', async () => {
    process.argv = ['node', 'cli', 'schema:load', '--non-interactive'];
    const { createCli } = require('../src/cli');
    await createCli().parseAsync();

    expect(mockRunnerInstance.schemaLoad).toHaveBeenCalled();
  });

  it('shows help and exits when no command is provided', () => {
    const exitSpy = jest.spyOn(process, 'exit').mockImplementation((() => {
      throw new Error('exit') as any;
    }) as any);
    process.argv = ['node', 'cli'];
    const { createCli } = require('../src/cli');
    expect(() => createCli().parse()).toThrow('exit');
    expect(exitSpy).toHaveBeenCalledWith(1);
    exitSpy.mockRestore();
  });
});


