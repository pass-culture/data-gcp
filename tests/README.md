# ETL Job Testing Framework

This directory contains the unit testing framework for ETL jobs, designed to run tests in isolated environments using `uv` with job-specific requirements.

## Directory Structure

```
tests/
├── unit/
│   ├── etl_jobs/
│   │   ├── external/
│   │   │   ├── contentful/
│   │   │   │   └── test_contentful_client.py
│   │   │   └── [other job tests]
│   │   └── conftest.py
├── scripts/
│   ├── test_runner.sh
│   └── run_job_tests.py
├── conftest.py
├── pytest.ini
└── README.md
```

## Quick Start

### Prerequisites

- Python 3.10+
- `uv` installed: `curl -LsSf https://astral.sh/uv/install.sh | sh`

### Running Tests

#### For a specific job:
```bash
./tests/scripts/test_runner.sh jobs/etl_jobs/external/contentful
```

#### For all testable jobs:
```bash
python tests/scripts/run_job_tests.py --all
```

#### For changed jobs only:
```bash
python tests/scripts/run_job_tests.py --changed
```

#### For specific jobs:
```bash
python tests/scripts/run_job_tests.py --jobs jobs/etl_jobs/external/contentful jobs/etl_jobs/external/brevo
```

## Features

### 🔒 Isolated Environments
Each job's tests run in a separate `uv` virtual environment with job-specific dependencies from `requirements.txt`.

### 📊 Coverage Reporting
Automatic code coverage reporting with HTML output saved to `htmlcov-{job_name}/index.html`.

### 🚀 Parallel Execution
Run multiple job tests in parallel using the `--parallel` flag.

### 🎯 Smart Test Discovery
Automatically detects which jobs need testing based on file changes.

## Writing Tests

### Test Structure
```python
"""Unit tests for {JobName}Client."""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from jobs.etl_jobs.external.{job_name}.{module} import {ClassName}

class Test{ClassName}:
    """Test suite for {ClassName} class."""

    def test_method_name(self, mock_dependency):
        """Test description."""
        # Arrange
        # Act
        # Assert
```

### Available Fixtures

#### Common Fixtures (from `tests/conftest.py`):
- `mock_gcp_credentials`: Mock GCP authentication
- `mock_bigquery_client`: Mock BigQuery client
- `mock_storage_client`: Mock GCS client
- `mock_secret_manager`: Mock Secret Manager
- `sample_dataframe`: Sample pandas DataFrame
- `mock_requests`: Mock HTTP requests
- `config_env`: Standard configuration environment

#### ETL-Specific Fixtures (from `tests/unit/etl_jobs/conftest.py`):
- `mock_contentful_client`: Mock Contentful API client
- `mock_brevo_client`: Mock Brevo API client
- `sample_etl_data`: Sample ETL data
- `mock_external_api`: Configurable external API mock
- `etl_config`: ETL job configuration
- `transformation_functions`: Common data transformation functions

### Custom Assertions
- `pytest.assert_dataframes_equal(df1, df2)`: Compare DataFrames with detailed error messages
- `pytest.assert_dataframe_not_empty(df)`: Assert DataFrame is not empty
- `pytest.assert_dataframe_has_columns(df, columns)`: Assert DataFrame has expected columns

## Configuration

### pytest.ini
The `pytest.ini` file contains:
- Test discovery settings
- Coverage configuration (minimum 70% coverage)
- Output formatting
- Custom markers for test categorization

### Markers
- `@pytest.mark.unit`: Unit tests
- `@pytest.mark.integration`: Integration tests
- `@pytest.mark.slow`: Slow running tests
- `@pytest.mark.external_api`: Tests requiring external API access

## CI/CD Integration

The testing framework integrates with the existing CI pipeline:

1. **File Change Detection**: Uses `tj-actions/changed-files` to detect changes in `jobs/etl_jobs/external/**`
2. **Test Discovery**: Enhanced `find_tests.sh` discovers jobs with unit tests
3. **Isolated Execution**: Each job runs in its own `uv` environment
4. **Parallel Testing**: Jobs tested in parallel for faster CI runs

## Troubleshooting

### Common Issues

1. **"No unit tests found for {job_name}"**
   - Ensure the test directory exists: `tests/unit/etl_jobs/external/{job_name}/`
   - Verify test files follow the naming convention: `test_*.py`

2. **Import errors**
   - Check that the job's `requirements.txt` includes all necessary dependencies
   - Verify the job structure allows for editable installation (`uv pip install -e`)

3. **Mock not working**
   - Ensure mocks are patched at the correct import path
   - Use `patch.object()` for specific method mocking

### Debugging

Enable verbose output:
```bash
./tests/scripts/test_runner.sh jobs/etl_jobs/external/contentful -v -s
```

Check coverage details:
```bash
# Coverage report is automatically generated in htmlcov-{job_name}/
open htmlcov-contentful/index.html
```

## Best Practices

1. **Test Isolation**: Each test should be independent and not rely on other tests
2. **Mock External Dependencies**: Always mock external APIs, databases, and services
3. **Descriptive Test Names**: Use clear, descriptive test method names
4. **Arrange-Act-Assert**: Structure tests with clear setup, execution, and verification phases
5. **Test Edge Cases**: Include tests for error conditions and edge cases
6. **Maintain Coverage**: Aim for at least 70% code coverage for critical paths

## Adding New Job Tests

1. Create test directory:
   ```bash
   mkdir -p tests/unit/etl_jobs/external/{job_name}
   ```

2. Create test files:
   ```bash
   touch tests/unit/etl_jobs/external/{job_name}/test_{module_name}.py
   ```

3. Write tests following the established patterns

4. Run tests to verify:
   ```bash
   ./tests/scripts/test_runner.sh jobs/etl_jobs/external/{job_name}
   ```

## Contributing

When adding new features to the testing framework:

1. Update this README
2. Add appropriate fixtures to `conftest.py` files
3. Update the CI configuration if needed
4. Test the changes with existing jobs
