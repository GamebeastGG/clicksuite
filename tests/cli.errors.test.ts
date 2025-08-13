import dotenv from 'dotenv';
import { Runner } from '../src/runner';

jest.mock('dotenv', () => ({
  __esModule: true,
  default: { config: jest.fn() },
}));

jest.mock('../src/runner');

describe('CLI command error paths (cli.ts)', () => {
  let originalArgv: string[];
  let mockRunnerInstance: jest.Mocked<Runner>;

  beforeEach(() => {
    originalArgv = process.argv;
    process.env.CLICKHOUSE_URL = 'http://default@localhost:8123/test_db';

    mockRunnerInstance = {
      init: jest.fn(),
      generate: jest.fn(),
      status: jest.fn(),
      migrate: jest.fn(),
      up: jest.fn(),
      down: jest.fn(),
      reset: jest.fn(),
      schemaLoad: jest.fn(),
    } as any;

    (Runner as unknown as jest.MockedClass<typeof Runner>).mockImplementation(() => mockRunnerInstance);

    jest.spyOn(console, 'error').mockImplementation(() => {});
  });

  afterEach(() => {
    process.argv = originalArgv;
    jest.restoreAllMocks();
    jest.clearAllMocks();
  });

  function expectExit(testFn: () => Promise<void> | void) {
    const exitSpy = jest.spyOn(process, 'exit').mockImplementation(((() => {
      throw new Error('exit');
    }) as unknown) as any);
    return {
      exitSpy,
      run: async () => {
        await expect(testFn()).rejects.toThrow('exit');
        expect(exitSpy).toHaveBeenCalledWith(1);
        exitSpy.mockRestore();
      },
    };
  }

  it('handles init error', async () => {
    mockRunnerInstance.init.mockRejectedValue(new Error('init failed'));
    process.argv = ['node', 'cli', 'init'];
    const { createCli } = require('../src/cli');
    await expectExit(async () => {
      await createCli().parseAsync();
    }).run();
  });

  it('handles generate error', async () => {
    mockRunnerInstance.generate.mockRejectedValue(new Error('gen failed'));
    process.argv = ['node', 'cli', 'generate', 'x'];
    const { createCli } = require('../src/cli');
    await expectExit(async () => {
      await createCli().parseAsync();
    }).run();
  });

  it('handles status error', async () => {
    mockRunnerInstance.status.mockRejectedValue(new Error('status failed'));
    process.argv = ['node', 'cli', 'migrate:status'];
    const { createCli } = require('../src/cli');
    await expectExit(async () => {
      await createCli().parseAsync();
    }).run();
  });

  it('handles migrate error', async () => {
    mockRunnerInstance.migrate.mockRejectedValue(new Error('migrate failed'));
    process.argv = ['node', 'cli', 'migrate'];
    const { createCli } = require('../src/cli');
    await expectExit(async () => {
      await createCli().parseAsync();
    }).run();
  });

  it('handles up error', async () => {
    mockRunnerInstance.up.mockRejectedValue(new Error('up failed'));
    process.argv = ['node', 'cli', 'migrate:up', '202401'];
    const { createCli } = require('../src/cli');
    await expectExit(async () => {
      await createCli().parseAsync();
    }).run();
  });

  it('handles down error', async () => {
    mockRunnerInstance.down.mockRejectedValue(new Error('down failed'));
    process.argv = ['node', 'cli', 'migrate:down', '202401'];
    const { createCli } = require('../src/cli');
    await expectExit(async () => {
      await createCli().parseAsync();
    }).run();
  });

  it('handles reset error', async () => {
    mockRunnerInstance.reset.mockRejectedValue(new Error('reset failed'));
    process.argv = ['node', 'cli', 'migrate:reset'];
    const { createCli } = require('../src/cli');
    await expectExit(async () => {
      await createCli().parseAsync();
    }).run();
  });

  it('handles schema:load error', async () => {
    mockRunnerInstance.schemaLoad.mockRejectedValue(new Error('schema failed'));
    process.argv = ['node', 'cli', 'schema:load'];
    const { createCli } = require('../src/cli');
    await expectExit(async () => {
      await createCli().parseAsync();
    }).run();
  });
});


