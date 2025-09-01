# Endpoints Monitoring

## Overview


This module runs monitoring tests for recommendation endpoints. It calls endpoints for both real and mock user/item IDs, analyzes predictions, and saves comparison reports.

### Prediction Analysis Tests
The `analyze_predictions` function computes a variety of metrics to assess endpoint quality and behavior:

- **Recommendation count:** Average number of recommendations returned per call.
- **Response codes:** Success/error rate of endpoint responses.
- **Search type distribution:** Proportion of different search types in the predictions.
- **Prediction overlap:** Overlap between repeated calls for the same user/item.
- **Latency metrics:** Average, min, max, and 95th percentile latency of endpoint responses.
- **Prediction attributes:** Statistics on special attributes like `_distance` and `_user_item_dot_similarity` if present.
- **User-user recommendation overlap:** Jaccard similarity between recommended items for different users.
- **Intra-list similarity:** Average cosine similarity between items in the same recommendation list (using loaded TT model).
- **User-item similarity:** Average cosine similarity between user and recommended item embeddings (using loaded TT model).



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

**in staging**
```bash
python main.py \
  --endpoint-name recommendation_user_retrieval_stg \
  --experiment-name algo_training_two_towers_v1.2_stg \
  --storage-path . \
  --number-of-ids 10 \
  --number-of-mock-ids 10 \
  --number-of-calls-per-user 2
```
## Output

- Reports for each call type: `integration_tests_reports.parquet` in the specified storage path

## Running Tests

To run the tests:

```bash
make test
```
