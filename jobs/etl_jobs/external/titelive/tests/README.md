# Testing Documentation

This document describes the automated testing setup for the Titelive ETL project.

## Test Structure

The test suite is organized into several categories:

```
tests/
├── __init__.py
├── conftest.py                  # Shared fixtures and test configuration
├── data/
│   └── sample_responses.json    # Sample API responses for testing
├── unit/                        # Unit tests for individual components
│   ├── __init__.py
│   ├── test_gcp.py             # Tests for GCP utilities
│   ├── test_requests.py        # Tests for API request functions
│   └── test_parse_products.py    # Tests for data processing functions
└── integration/                 # Integration tests for complete workflows
    ├── __init__.py
    └── test_workflow.py         # End-to-end workflow tests
```

## Test Categories

### Unit Tests

Unit tests focus on testing individual functions and classes in isolation:

- **`test_gcp.py`**: Tests for Google Cloud Secret Manager integration
- **`test_requests.py`**: Tests for Titelive API communication, token management, and data retrieval
- **`test_parse_products.py`**: Tests for data transformation and processing logic

### Integration Tests

Integration tests verify that different components work together correctly:

- **`test_workflow.py`**: End-to-end tests that simulate the complete ETL pipeline

## Running Tests

### Prerequisites

Ensure you have the test dependencies installed:

```bash
make install
```

### Running All Tests

```bash
make test
```

### Running Specific Test Categories

```bash
# Run only unit tests
make test-unit

# Run only integration tests
make test-integration
```

### Running Tests with Coverage

```bash
make test-coverage
```

This generates both terminal output and an HTML coverage report in `htmlcov/`.

### Running Individual Test Files

```bash
# Run specific test file
PYTHONPATH=. uv run pytest tests/unit/test_requests.py -v

# Run specific test class or method
PYTHONPATH=. uv run pytest tests/unit/test_requests.py::TestGetModifiedOffers -v
PYTHONPATH=. uv run pytest tests/unit/test_requests.py::TestGetModifiedOffers::test_get_modified_products_single_page -v
```

## Test Configuration

### pytest.ini

The project uses a pytest configuration file that sets up:

- Test discovery patterns
- Verbose output
- Warning filters
- Test markers for categorization

### Fixtures

Common test fixtures are defined in `conftest.py`:

- **`sample_responses`**: Loads realistic API response data from JSON files
- **`mock_titelive_api_response`**: Provides mock API responses for book data
- **`mock_music_api_response`**: Provides mock API responses for music data
- **`sample_raw_dataframe`**: Pre-configured DataFrame for testing parsing logic
- **`mock_secret_manager`**: Mock Google Cloud Secret Manager client

## Mocking Strategy

The tests use comprehensive mocking to avoid external dependencies:

### API Calls

- All HTTP requests to Titelive API are mocked
- Token authentication is mocked
- Error conditions (401, timeouts, etc.) are simulated

### GCP Services

- Google Cloud Secret Manager calls are mocked
- No actual GCP credentials are required for testing

### File I/O

- Parquet file operations use temporary directories
- No persistent test data is created

## Test Data

### Sample API Responses

The `tests/data/sample_responses.json` file contains realistic API responses that mirror the actual Titelive API structure:

- Book metadata with French titles and authors
- Music album data
- Complete article information including pricing and availability

### Test Scenarios

The test suite covers various scenarios:

#### Happy Path

- Successful API authentication
- Complete data extraction and parsing
- Proper date filtering
- Correct data type enforcement

#### Error Handling

- API authentication failures
- Network timeouts and errors
- Invalid JSON data
- Missing or malformed fields

#### Edge Cases

- Empty API responses
- Large result sets with pagination
- Date filtering edge cases
- Special characters in French text

## Continuous Integration

### GitHub Actions

The project includes a CI/CD pipeline (`.github/workflows/ci.yml`) that:

- Runs on Python 3.12
- Installs dependencies with `uv`
- Performs linting with `ruff`
- Executes all tests
- Generates coverage reports
- Uploads coverage to Codecov (optional)

### Local CI Simulation

You can simulate the CI environment locally:

```bash
# Install dependencies
make install

# Run linting
make lint

# Run all tests
make test

# Check coverage
make test-coverage
```

## Writing New Tests

### Unit Test Guidelines

1. Test one function or method per test
2. Use descriptive test names that explain the scenario
3. Follow the Arrange-Act-Assert pattern
4. Mock all external dependencies
5. Test both success and failure cases

### Integration Test Guidelines

1. Test realistic end-to-end scenarios
2. Use temporary files for I/O operations
3. Verify the complete data flow
4. Test error propagation across components

### Adding Test Data

When adding new test scenarios:

1. Add realistic sample data to `tests/data/sample_responses.json`
2. Create fixtures in `conftest.py` if the data will be reused
3. Ensure test data covers edge cases and error conditions

## Debugging Tests

### Running Tests in Debug Mode

```bash
# Run with more verbose output
PYTHONPATH=. uv run pytest tests/ -vvv

# Run with print statements visible
PYTHONPATH=. uv run pytest tests/ -s

# Stop on first failure
PYTHONPATH=. uv run pytest tests/ -x

# Run specific test with debugging
PYTHONPATH=. uv run pytest tests/unit/test_requests.py::test_function_name -vvv -s
```

### Common Issues

1. **Import Errors**: Ensure `PYTHONPATH=.` is set when running tests
2. **Fixture Not Found**: Check that fixtures are defined in `conftest.py`
3. **Mock Not Working**: Verify the correct module path in `@patch` decorators
4. **File Path Issues**: Use `Path(__file__).parent` for relative paths in tests

## Performance Considerations

- Tests should run quickly (< 1 second per test typically)
- Use mocking to avoid network calls
- Clean up temporary files and resources
- Avoid sleeping or waiting in tests

## Maintenance

### Updating Test Data

When the Titelive API structure changes:

1. Update `tests/data/sample_responses.json` with new field structures
2. Update relevant test assertions
3. Add tests for new fields or behaviors

### Adding New Test Categories

For new functionality:

1. Create new test files following the naming convention `test_*.py`
2. Add appropriate fixtures to `conftest.py`
3. Update this documentation
4. Add new test commands to the Makefile if needed
