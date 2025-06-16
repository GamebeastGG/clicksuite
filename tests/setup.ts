import dotenv from 'dotenv';

dotenv.config({ path: '.env.test' });

beforeAll(() => {
  jest.setTimeout(10000);
});

afterAll(() => {
  jest.clearAllTimers();
});

global.console = {
  ...console,
  log: jest.fn(),
  warn: jest.fn(),
  error: jest.fn(),
};