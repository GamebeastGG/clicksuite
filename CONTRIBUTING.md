# Contributing to Clicksuite

Thank you for your interest in contributing to Clicksuite! This document provides guidelines and information for contributors.

## Code of Conduct

By participating in this project, you agree to abide by our code of conduct. Be respectful, inclusive, and constructive in all interactions.

## Getting Started

1. **Fork the repository** on GitHub
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/your-username/clicksuite.git
   cd clicksuite
   ```
3. **Install dependencies**:
   ```bash
   npm install
   ```
4. **Create a branch** for your feature or bug fix:
   ```bash
   git checkout -b feature/your-feature-name
   ```

## Development Workflow

### Building the Project

```bash
npm run build
```

### Running Tests

```bash
# Run all tests
npm test

# Run tests in watch mode
npm run test:watch

# Run tests with coverage
npm run test:coverage
```

### Code Quality

- **TypeScript**: All code must be properly typed
- **Tests**: Write tests for new functionality
- **Coverage**: Maintain or improve test coverage
- **Linting**: Follow existing code style

## Testing Guidelines

### Writing Tests

1. **Unit Tests**: Test individual functions and methods
2. **Integration Tests**: Test interactions between components
3. **Error Handling**: Test error conditions and edge cases
4. **Mocking**: Use Jest mocks for external dependencies

### Test Structure

```typescript
describe('Component/Feature', () => {
  describe('method/function', () => {
    it('should do something specific', () => {
      // Arrange
      const input = 'test';
      
      // Act
      const result = functionUnderTest(input);
      
      // Assert
      expect(result).toBe('expected');
    });
  });
});
```

### Test Coverage

- Aim for high test coverage (>80%)
- Focus on critical paths and edge cases
- Test both success and failure scenarios

## Submitting Changes

### Pull Request Process

1. **Update tests** for your changes
2. **Run the test suite** and ensure all tests pass:
   ```bash
   npm test
   ```
3. **Update documentation** if needed
4. **Commit your changes** with descriptive commit messages:
   ```bash
   git commit -m "feat: add new migration validation feature"
   ```
5. **Push to your fork** and submit a pull request

### Commit Message Guidelines

Follow conventional commit format:

- `feat:` New features
- `fix:` Bug fixes
- `docs:` Documentation changes
- `test:` Adding or fixing tests
- `refactor:` Code refactoring
- `style:` Code style changes
- `ci:` CI/CD changes

Examples:
```
feat: add support for custom migration directories
fix: handle empty migration files gracefully
docs: update installation instructions
test: add integration tests for rollback functionality
```

### Pull Request Checklist

- [ ] Tests pass locally (`npm test`)
- [ ] Code follows existing style and conventions
- [ ] Documentation updated (if applicable)
- [ ] Commit messages follow convention
- [ ] Branch is up-to-date with main branch
- [ ] PR description clearly explains the changes

## Project Structure

```
clicksuite/
├── src/                 # TypeScript source code
│   ├── db.ts           # Database operations
│   ├── runner.ts       # Migration runner logic
│   ├── index.ts        # CLI interface
│   └── types.ts        # Type definitions
├── tests/              # Test files
│   ├── db.test.ts      # Database tests
│   ├── runner.test.ts  # Runner tests
│   └── ...
├── dist/               # Compiled JavaScript (generated)
├── .github/            # GitHub Actions workflows
└── migrations/         # Example migrations
```

## Areas for Contribution

### High Priority

- **Performance improvements**: Optimize database queries and file operations
- **Error handling**: Improve error messages and recovery
- **Documentation**: Examples, tutorials, and API documentation
- **Testing**: Increase test coverage and add integration tests

### New Features

- **Migration validation**: Schema validation before applying migrations
- **Backup/restore**: Automatic backups before major changes
- **Dry run mode**: Preview changes without applying them
- **Migration templates**: Common migration patterns
- **Parallel execution**: Run migrations in parallel when safe

### Bug Fixes

- Check the [issues page](https://github.com/gamebeastgg/clicksuite/issues) for known bugs
- Report new bugs with detailed reproduction steps

## Getting Help

- **Issues**: Open an issue for bugs or feature requests
- **Discussions**: Use GitHub discussions for questions
- **Documentation**: Check the README and code comments

## Release Process

Releases are automated through GitHub Actions:

1. **Version bump**: Update version in `package.json`
2. **Create release**: Create a GitHub release with tag
3. **Automated publishing**: NPM publish happens automatically

## License

By contributing to Clicksuite, you agree that your contributions will be licensed under the ISC License.

Thank you for contributing! 🚀