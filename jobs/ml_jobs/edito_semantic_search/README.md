# Edito Semantic Search Microservice

This microservice provides a semantic search API and related tools for editorial content

## Setup

1. **Install dependencies**

```bash
make install-api
```

## Data Preparation

### Prepare Catalog Data

Run BigQuery queries to extract offer catalog data, export to GCS as parquet, and optionally partition it:

```bash
# Default: dev environment with partitioning
python prepare_catalog_data.py

# Specific environment
python prepare_catalog_data.py --env stg
python prepare_catalog_data.py --env prod

# Skip partitioning
python prepare_catalog_data.py --env dev --no-partition

# Custom partition columns
python prepare_catalog_data.py --env dev -p offer_category_id -p venue_department_code
```

**What it does:**
1. Executes SQL query from `sql/chatbot_catalog_offers.sql`
2. Exports results to GCS as parquet: `gs://mlflow-bucket-{env}/streamlit_data/chatbot_edito/offers_{env}/`
3. Partitions the data using Hive-style partitioning (default: by `offer_subcategory_id` and `venue_department_code`)
4. Outputs partitioned data to: `gs://mlflow-bucket-{env}/streamlit_data/chatbot_edito/offers_{env}_partitioned/`

## Running the API

Start the Flask API server (served via Hypercorn):

```bash
make start_app
```

This will run the API on `0.0.0.0:8085` by default. You can access endpoints

## Running Tests

To run the test suite (using pytest):

```bash
make test
```

This will run all tests in the `tests/` directory. You can also run specific tests with pytest directly.

## Running the Streamlit to visualize API responses

To launch the Streamlit for visualizing API responses:

```bash
make streamlit
```

This will start Streamlit defined in `streamlits/st_visualise_api_response.py`.
