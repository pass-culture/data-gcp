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

1. **Install Dependencies**:
    The following command will install packages and also downloads the LanceDB vector DataBase for the models used in the API.
    ```sh
    make install-api
    ```
      * by default we use the lancedb model contained inside the following Docker Image  `europe-west1-docker.pkg.dev/passculture-infra-prod/pass-culture-artifact-registry/data-gcp/retrieval-vector/prod/retrieval_recommendation_v1_2_prod:two_towers_user_recommendation_prod_v20250429`
      * if you want to use a different model, you can run
        ```sh
        make install-api DOCKER_IMAGE_TAG=<docker_image_tag>
        ```
        where `<docker_image_tag>` is the tag of the docker image you want to use. You can find those in [Artifact Registry](https://console.cloud.google.com/artifacts/docker/passculture-infra-prod/europe-west1/pass-culture-artifact-registry?authuser=2&project=passculture-infra-prod).
        * For instance :
            ```sh
            DOCKER_IMAGE_TAG=europe-west1-docker.pkg.dev/passculture-infra-prod/pass-culture-artifact-registry/data-gcp/retrieval-vector/prod/retrieval_recommendation_v1_2_prod:two_towers_user_recommendation_prod_v20250428 make install-api
            ```

2. Start the API using:
    ```sh
    make start
    ```
    => It will run the API on `0.0.0.0:8080`
3. Make a prediction
    ```sh
    curl -X POST localhost:8080/predict -H 'Content-Type: application/json' -H 'Accept: application/json' -d '{
    "instances": [
        {
        "model_type": "recommendation",
        "user_id": "3734607",
        "size": 50,
        "params": {},
        "call_id": "1234567890",
        "debug": 1,
        "prefilter": 1,
        "similarity_metric": "dot"
        }
    ]
    }'
    ```

### Testing
To run the tests, including unit tests and integration tests, use:

```sh
pytest --log-cli-level=DEBUG
```

or

```sh
PYTHONPATH=./ pytest --cov
```

This will run the entire test suite and display logs at the `DEBUG` level for troubleshooting.

### Running Individual Tests
You can also run a specific test or module:

```sh
pytest tests/retrieval/test_similar_offer.py
```
