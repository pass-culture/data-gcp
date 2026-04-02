# Edito Semantic Search Microservice

This microservice provides a semantic search API and related tools for editorial content

## Setup

1. **Install dependencies**

```bash
make install
```

This will install the project with uv and initialize the .env file.
(by default it is rooted on dev environment)

## Data Preparation

### Prepare Catalog Data

Run BigQuery queries to extract offer catalog data, export to GCS as parquet
    in partitionned directories.

```bash
# Default: dev environment with partitioning
PYTHONPATH=. python prepare_catalog_data.py

# Specific environment
PYTHONPATH=. python prepare_catalog_data.py --env stg
PYTHONPATH=. python prepare_catalog_data.py --env prod

# Custom partition columns
PYTHONPATH=. python prepare_catalog_data.py --env dev -p offer_category_id -p venue_department_code
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

> * It will use the .env file values to determine in which environment it is run.
> * This will run the API on `0.0.0.0:8085` by default.

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

##  Deployment

### Build the container API

The API is containerized using Docker. To build and push the container

```bash
make build_and_push_docker_image
```

> It will use the .env file values to determine in which environment it is run.

### Deploy the containerized API to VertexAI

The containerized API can be deployed to VertexAI as an online prediction endpoint.

```bash
make deploy_model
```

> It will use the .env file values to determine in which environment it is run.
