# Retrieval Recommendation API

### Overview
This API is designed to provide recommendations based on user preferences and vector embeddings. The API leverages **LanceDB** for vector search and supports similar offer, recommendation, semantic search, and filtering mechanisms to deliver personalized results.

### Key Features
- **User Recommendation** engine: Suggests similar items based on user preferences calculated through vector embeddings (Two Tower Logic).
- **In the Same Category** engine: Suggests similar items based on a set of provided items calculated through custom vector embeddings or semantics from sentence transformers.
- **Vector Search**: Uses **LanceDB** to store and search vectors for items and users.
- **Filtering**: Applies filtering criteria to narrow down recommendations and get top associated items.
- **Re-ranking**: Supports re-ranking of results based on additional metrics.

### Requirements
- **Python 3.10+**
- **LanceDB** for vector database operations.
- **Flask** for the API.
- **DocArray** for managing documents and embeddings.
- **Pytest** for testing.

You can find all dependencies in the `api-requirements.in` file.

### How to Run

```sh
uv pip install -r api-requirements.in
```

Start the API using:

```sh
hypercorn --bind 0.0.0.0:8080 app.app:app
```

### Testing
To run the tests, including unit tests and integration tests, use:

```sh
pytest --log-cli-level=DEBUG
```

This will run the entire test suite and display logs at the `DEBUG` level for troubleshooting.

### Running Individual Tests
You can also run a specific test or module:

```sh
pytest tests/retrieval/test_similar_offer.py
```
