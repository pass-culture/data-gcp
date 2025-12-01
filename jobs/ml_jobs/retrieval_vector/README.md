# Retrieval Recommendation API

## Overview

This API is designed to provide recommendations based on user preferences and vector embeddings. The API leverages **LanceDB** for vector search and supports similar offer, recommendation, semantic search, and filtering mechanisms to deliver personalized results.

## Key Features

- **User Recommendation** engine: Suggests similar items based on user preferences calculated through vector embeddings (Two Tower Logic).
- **In the Same Category** engine: Suggests similar items based on a set of provided items calculated through custom vector embeddings or semantics from sentence transformers.
- **Vector Search**: Uses **LanceDB** to store and search vectors for items and users.
- **Filtering**: Applies filtering criteria to narrow down recommendations and get top associated items.
- **Re-ranking**: Supports re-ranking of results based on additional metrics.

##  About vector search metrics

The vector search is implemented using **LanceDB**, a high-performance vector database.

The default similarity metric used for vector search is the **dot product**.

**However, if an index has been built for a specific vector search, the metric used to build the index will override the default similarity metric.**

> [LanceDB Doc](https://lancedb.com/docs/search/vector-search/#configure-distance-metric): You can configure the distance metric during search only if there’s no vector index. If a vector index exists, the distance metric will always be the one you specified when creating the index.

=> Check create_items_table function in `create_vector_database.py` to see which metric is used.

## Requirements

- **Python 3.10+**
- **LanceDB** for vector database operations.
- **Flask** for the API.
- **DocArray** for managing documents and embeddings.
- **Pytest** for testing.

You can find all dependencies in the `api-requirements.in` file.

## How to Run the API locally

1. **Install Dependencies**:
   The following command will install packages.

   ```sh
   make install-api
   ```

2. **Build the lancedb vector database**:
   You can build the LanceDB vector database using the following commands:
   - For a dummy model:

      ```sh
      python create_vector_database.py dummy-database
      ```

   - For a production model:

      ```sh
      python create_vector_database.py default-database --source-artifact-uri <source_artifact_uri>
      ```

      where `<source_artifact_uri>` is the GS URI of the source artifact of the Two Tower training you want to use (don't forget the `/model` suffix). You can find it on [MLFlow](https://mlflow.passculture.team/#/experiments/35).
      Example:

      ```sh
      python create_vector_database.py default-database --source-artifact-uri gs://mlflow-bucket-prod/artifacts/35/e894fb5e2b5248feb4114bb2473571ff/artifacts/model
      ```

3. **Start the API using**:

   ```sh
   make start
   ```

   => It will run the API on `0.0.0.0:8080`
   => If you want to change the port, edit the start target in the `Makefile`.

4. **Make a prediction**:

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
       }
   ]
   }'
   ```

   There are several use cases for which you can use the API:
   - `for user recommendation :

      ```json
      {
         "model_type": "recommendation",
         "user_id": "3734607",
         "size": 10,
         "params": {},
         "call_id": "1234567890",
         "debug": 1,
         "prefilter": 1,
      }
      ```

   - for similar offer recommendation :

      ```json

      {
         "model_type": "similar_offer",
         "items": ["product-6344516"],
         "size": 10,
         "params": {},
         "call_id": "1234567890",
         "debug": 1,
         "prefilter": 1,
      }
      ```

   - for playlist recommendation constructions (with multiple items) :

      ```json
      {
         "model_type": "similar_offer",
         "items": ["product-6344516", "product-6344517"],
         "size": 10,
         "params": {},
         "call_id": "1234567890",
         "debug": 1,
         "prefilter": 1,
      }
      ```

   - for top recommendations

      ```json
      {
         "model_type": "tops",
         "size": 10,
         "params": {},
         "call_id": "1234567890",
         "debug": 1,
         "prefilter": 1,
         "vector_column_name": "booking_number_desc"
      }
      ```

   - If you built a semantic retrieval textclient :

      ```json
      {
         "model_type": "semantic",
         "size": 10,
         "params": {},
         "call_id": "1234567890",
         "debug": 1,
         "prefilter": 1,
         "text": "YOUR TEXT HERE"
      }
      ```

   In the above examples, you will get a dictionnary with a key "predictions" containing the list of recommended items.
   > Note that the `"_distance"` field is to improved :
   - for top recommendations, it will return a number > 1, the lowest being the best recommendation.
   - for vector search, it will return the distance between the input vector and the closest vector in the database for the `dot` distance (i.e. `1 - dot_product`).
      - ⚠️ If an index has been built for this vector search as it is recommended, then the search metric is overriden by the one used to build the index (see above section "About vector search metrics").
      - ⚠️ If you do a vector search but find no results, then the fallback will be the top recommendations, giving you a `"_distance"` > 1

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

### Troubleshooting

1. Retrieve lancedb database from the docker image: If you want to use the same vector database as the one which was already build and deploy (for debug purposes), you can retrieve the LanceDB vector database from the docker image.

   To do this, run:

   ```sh
   DOCKER_IMAGE_TAG=<docker_image_tag> make install-api
   ```

   where `<docker_image_tag>` is the tag of the docker image you want to use. You can find those in [Artifact Registry](https://console.cloud.google.com/artifacts/docker/passculture-infra-prod/europe-west1/pass-culture-artifact-registry?authuser=2&project=passculture-infra-prod).

   - For instance :

     ```sh
     DOCKER_IMAGE_TAG=europe-west1-docker.pkg.dev/passculture-infra-prod/pass-culture-artifact-registry/data-gcp/retrieval-vector/prod/retrieval_recommendation_v1_2_prod:two_towers_user_recommendation_prod_v20250428 make install-api
     ```

   - ⚠️ If you use a production model, please delete the Docker image locally after use. ⚠️
