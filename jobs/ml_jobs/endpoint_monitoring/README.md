# Reco Integration Test

## Overview

This module runs integration tests for recommendation endpoints. It calls endpoints for both real and mock user/item IDs, analyzes predictions, and saves comparison reports.

## Prerequisites

- Python 3.10+
- Install dependencies:
  ```bash
  uv sync
  ```

## Running `main.py`

You can run the integration test via the Typer CLI. Example usage:

```bash
python main.py \
  --endpoint-name <ENDPOINT_NAME> \
  --experiment-name <EXPERIMENT_NAME> \
  --storage-path <PATH_TO_DATA> \
  --number-of-ids 10 \
  --number-of-mock-ids 10 \
  --number-of-calls-per-user 2
```

**Arguments:**
- `--endpoint-name`: Name of the endpoint to test (required)
- `--experiment-name`: Name of the mlflow experiment containing 'recommendation model (required)
- `--storage-path`: Path to the data directory containing user and item data and output report (required)
- `--number-of-ids`: Number of real IDs to test (default: 10)
- `--number-of-mock-ids`: Number of mock IDs to test (default: 10)
- `--number-of-calls-per-user`: Number of calls per user (default: 2)

## Output

- Reports for each call type: `integration_tests_reports.parquet` in the specified storage path

## Running Tests

To run the tests:

```bash
make test
```
