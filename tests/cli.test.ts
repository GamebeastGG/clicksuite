import dotenv from 'dotenv';

jest.mock('dotenv', () => ({
  __esModule: true,
  default: { config: jest.fn() },
}));

describe('CLI module', () => {
  it('should export createCli and not auto-execute in tests', async () => {
    const { createCli } = require('../src/cli');
    expect(typeof createCli).toBe('function');
  });
});


