# Edito Semantic Search Microservice

This microservice provides a semantic search API and related tools for editorial content

## Setup

1. **Install dependencies**

```bash
make install-api
```

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
