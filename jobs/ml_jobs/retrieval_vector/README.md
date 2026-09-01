# Retrieval Recommendation API

## Overview

This API is designed to provide recommendations based on two tower embeddings hosted on a vector database. The API leverages **LanceDB** for vector search and supports different request modes including : user recommendation, similar offer retrieval, and trend-based filtering.

## Key Features

- **User Recommendation** engine: Suggests similar items based on user preferences. It computes similarity score between user two-tower embeddings and items two tower embeddings, and so returns most similar items to the user.
- **Similar Offer** engine: Suggests items similar to a given set of items using **only** Two-Tower item vector embeddings.
- **Vector Search**: Uses **LanceDB** to store and search vectors for items and users.
- **Filtering**: Applies filtering criteria to narrow down recommendations and get top associated items.
- **Re-ranking**: Supports re-ranking of results based on additional metrics.

## Data model

### Storage layout

There is **one** LanceDB table (`metadata/vector/items.lance`) and **two** flat document stores:

| Store | Path | Contents |
|-------|------|----------|
| LanceDB table | `metadata/vector/` | All item vectors + metadata, searched at query time |
| Item document store | `metadata/item.docs` | Item id → embedding mapping, used to look up input item vectors in-process |
| User document store | `metadata/user.docs` | User id → embedding mapping, used to look up the requesting user's vector in-process |

Users are **not** stored in LanceDB. The user vector is retrieved from `user.docs` and then used as the query vector against the LanceDB `items` table.

### `items` table schema

The table has the following column types:

| Column | Type | Description |
|--------|------|-------------|
| `vector` | `float32[N]` | Two Tower item embedding. **Has a vector index** (metric set at build time in `cli/create_vector_database.py`). |
| `raw_embeddings` | `float32[N]` | Identical copy of the Two Tower embedding. **No vector index** — always uses default `dot` product, never overridden by an index metric. |
| `item_id` | `string` | Item identifier |
| `booking_number_desc` | `float32[1]` | Pre-computed booking rank (1D vector, used for tops search) |
| `booking_trend_desc` | `float32[1]` | Booking trend rank (1D vector) |
| `booking_creation_trend_desc` | `float32[1]` | Creation trend rank (1D vector) |
| `booking_release_trend_desc` | `float32[1]` | Release trend rank (1D vector) |
| `category`, `subcategory_id`, `search_group_name` | `string` | **Scalar-indexed** — used for `params` filtering |
| `stock_price` | `float32` | **Scalar-indexed** (BTREE) |
| Other metadata | various | `topic_id`, `cluster_id`, `gtl_*`, `is_geolocated`, `booking_number*`, `stock_*`, `offer_*`, `example_*` |

The 1D trend columns (`booking_number_desc`, etc.) are vector columns so they can be searched with the same LanceDB vector search interface as the embeddings — this is how `tops` ranking works.

### About `vector` vs `raw_embeddings`

Both columns store the same Two Tower item embedding. The distinction is:

- **`vector`**: has a vector index built on it (ANN index, metric set at build time). Searching it is fast but the metric is fixed to whatever was used at index creation.
- **`raw_embeddings`**: no index. Searching it always uses exact `dot` product. Use this if you want to force exact dot product regardless of the index.

> **⚠️ Warning about lancedb distances**: [LanceDB Doc](https://lancedb.github.io/lancedb/search/): If a vector index exists, the distance metric will always be the one you specified when creating the index — the metric parameter in the search call is ignored.

## Semantic retrieval flavor (`semantic`)

This codebase serves several **flavors** (each built into its own container and
deployed to its own Vertex AI endpoint): `two_tower` recommendation, `metadata_graph`
retrieval, and **`semantic`**. The semantic flavor serves the item semantic
embeddings produced by the `item_embedding` microservice (`google/embeddinggemma-300m`).

It is configured by `metadata/model_type.json`:

```json
{ "type": "semantic", "vector_search_metric": "cosine" }
```

which loads a `SemanticClient`. **No embedding model is bundled** in the container.

### `items` table schema (semantic)

| Column | Type | Index |
|--------|------|-------|
| `vector` | `float32[768]` | Vector (IVF_PQ, **cosine**) — used for `semantic_search` |
| `item_id` | `string` | Scalar (BTREE) — output id + fast query-vector lookup |
| `item_name`, `item_description` | `string` | Returned metadata |
| `search_text` (`item_name` + `item_description`) | `string` | **FTS** — used for `text_search` |
| `category`, `subcategory_id` | `string` | Scalar (BITMAP) — `params` filtering |

No `item.docs` / `user.docs` are baked: the query item's vector is read straight
from the table (a 768-dim id→vector dump would be far too large to bake).

### Search modes

**`semantic_search`** ⭐ — item-to-item vector search. Looks the input item's vector up
in the table and returns nearest neighbors (cosine). Input items are excluded. No
`tops` fallback (the semantic table has no booking columns).

```sh
curl -X POST localhost:8080/predict -H 'Content-Type: application/json' \
  -d '{"instances": [{"model_type": "semantic_search", "items": ["product-123"], "size": 10}]}'
```

**`text_search`** — keyword full-text search over `search_text`.

```sh
curl -X POST localhost:8080/predict -H 'Content-Type: application/json' \
  -d '{"instances": [{"model_type": "text_search", "text": "roman policier", "size": 10}]}'
```

Both modes accept `params` (filtering on `category` / `subcategory_id`) and `debug`
(returns the metadata columns plus `_distance` / `_score`). Output is `item_id` + metadata.

### Build & deploy

- Build the DB: `python cli/create_vector_database.py semantic-database --item-embedding-gs-path <…> --item-metadata-gs-path <…>`
  from BigQuery exports of `item_embedding_refactor` + `item_metadata`.
- The `build_and_push_semantic_retrieval_api` Airflow DAG builds & pushes the container
  (MLFlow experiment `semantic_item_retrieval_v1.0_<env>`); `algo_default_deployment`
  deploys it to the `semantic_item_retrieval_<env>` endpoint.

## Requirements

- **Python 3.11**
- **LanceDB** for vector database operations.
- **Flask** for the API.
- **Pytest** for testing.

All dependencies are managed via `pyproject.toml` and `uv`.

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
      python cli/create_vector_database.py dummy-database
      ```

   - For a production model:

      ```sh
      python cli/create_vector_database.py default-database --source-artifact-uri <source_artifact_uri>
      ```

      where `<source_artifact_uri>` is the GS URI of the source artifact of the Two Tower training you want to use (don't forget the `/model` suffix). You can find it on [MLFlow](https://mlflow.passculture.team/#/experiments/35).
      Example:

      ```sh
      python cli/create_vector_database.py default-database --source-artifact-uri gs://mlflow-bucket-prod/artifacts/35/e894fb5e2b5248feb4114bb2473571ff/artifacts/model
      ```

3. **Start the API using**:

   ```sh
   make start
   ```

   => It will run the API on `0.0.0.0:8080`
   => If you want to change the port, edit the start target in the `Makefile`.

4. **Make a prediction**:

   ```sh
   curl -X POST localhost:8080/predict \
     -H 'Content-Type: application/json' \
     -d '{"instances": [{"model_type": "recommendation", "user_id": "3734607", "size": 10}]}'
   ```

   See [API reference](#api-reference) for the full request schema and per-mode payloads.

## API reference
This API is built with FastAPI.
### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/predict` | Run a prediction |
| `GET` | `/isalive` | Health check — returns `200` if the service is up |

### Request envelope

All prediction requests use the same envelope. Only the first element of `instances` is processed.

```json
{
  "instances": [
    { ...request fields... }
  ]
}
```

### Request fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `model_type` | string | **required** | One of `recommendation`, `similar_offer`, `tops`, `filter` |
| `user_id` | string | `null` | Required for `recommendation` |
| `items` | string[] | `[]` | Required for `similar_offer`. List of item IDs to use as input vectors |
| `size` | int ≥ 1 | `500` | Number of results to return |
| `params` | object | `{}` | Filter expressions (see [Filtering](#filtering-params)). Setting this automatically enables `prefilter` |
| `vector_column_name` | string | `"vector"` | Which vector column to search. Must be valid for the given `model_type` (see table below) |
| `debug` | bool | `false` | Include full metadata and metric fields in the response |
| `prefilter` | bool | `false` | Apply `params` filter before the vector search (post-filter if `false`). Automatically `true` when `params` is non-empty |
| `re_rank` | bool | `false` | Apply secondary re-ranking after the vector search |
| `excluded_items` | string[] | `[]` | Item IDs to exclude from results |
| `call_id` | string | auto UUID | Request identifier for tracing |

> `offer_id` (string) is a **deprecated** alias for `items`. If `items` is empty and `offer_id` is provided, it is automatically promoted to `items: [offer_id]`. Always use `items`.

### Valid `vector_column_name` per `model_type`

| `model_type`   | Valid `vector_column_name` values |
|----------------|-----------------------------------|
| `recommendation` | `vector` *(default, ANN index)*, `raw_embeddings` *(exact dot, no index)* |
| `similar_offer`  | `vector` *(default, ANN index)*, `raw_embeddings` *(exact dot, no index)* |
| `tops`           | `booking_number_desc` *(default)*, `booking_trend_desc`, `booking_creation_trend_desc`, `booking_release_trend_desc` |
| `filter`         | same as `tops` |

### Response

```json
{
  "predictions": [
    { "idx": 0, "item_id": "product-123" },
    ...
  ]
}
```

When `debug=true`, each prediction also includes all metadata columns from the `items` table plus:

| Field | Description |
|-------|-------------|
| `_distance` | Distance to the query vector. Lower is better. Values > 1 indicate a `tops` fallback result |
| `_search_type` | `vector`, `tops`, or `aggregated_vectors` |
| `_user_item_dot_similarity` | Dot product between the user vector and the result item vector (present when `user_id` is provided) |
| `_item_item_dot_similarity` | Map of `{input_item_id → dot_product}` between each input item and the result item (present when `items` is provided) |

### Error responses

| HTTP | Cause |
|------|-------|
| `400` | Missing or malformed JSON body, or missing `instances` key |
| `403` | Field validation error (e.g. invalid `model_type`, wrong `vector_column_name`) or logic error (e.g. missing `user_id` for `recommendation`) |
| `500` | Unexpected server error |

---

## `model_type` modes

### `recommendation`

Retrieves items similar to the requesting user by searching the `vector` column using the user's Two Tower embedding from `metadata/user.docs`.

**Minimal payload:**
```json
{
  "model_type": "recommendation",
  "user_id": "3734607"
}
```

**Logic:**
1. Looks up the user vector in `metadata/user.docs`.
2. Runs a vector search against the LanceDB `items` table.
3. If no user vector exists or the search returns no results, falls back to `tops` with `vector_column_name=booking_number_desc`.

> Only available when the deployed model is of type `two_tower`.

---

### `similar_offer`

Retrieves items similar to one or more input items by searching the `vector` column using each item's Two Tower embedding from `metadata/item.docs`. Input items are automatically excluded from the results.

**Minimal payload (single item):**
```json
{
  "model_type": "similar_offer",
  "items": ["product-6344516"]
}
```

**Minimal payload (multiple items — results are aggregated and re-ranked by mean `_distance`):**
```json
{
  "model_type": "similar_offer",
  "items": ["product-6344516", "product-6344517"]
}
```

**Logic:**
1. For each item in `items`, looks up its vector in `metadata/item.docs` and runs a vector search.
2. If multiple items are given, results are merged and sorted by mean `_distance` across items.
3. If no vector is found for any input item, falls back to `tops` with `vector_column_name=booking_number_desc` (no fallback when using the `metadata_graph` embedding model).

---

### `tops`

Returns items ranked by a trend column. Ranking is done by dot product against a fixed approximate vector `[-0.0001]`, which surfaces items with the lowest pre-computed rank value first (= most booked / most trending).

**Minimal payload:**
```json
{
  "model_type": "tops"
}
```

**With a specific trend column:**
```json
{
  "model_type": "tops",
  "vector_column_name": "booking_trend_desc"
}
```

Default `vector_column_name` is `booking_number_desc`.

---

### `filter`

Alias for `tops` — identical behaviour, same handler. Intended to signal intent when the primary goal is filtered retrieval rather than trend ranking.

**Minimal payload:**
```json
{
  "model_type": "filter",
  "params": { "category": { "$eq": "LIVRE" } }
}
```

---

### Filtering (`params`)

All modes support the `params` field. Filters are translated into a SQL `WHERE` clause on the LanceDB `items` table.

**Scalar-indexed columns** (efficient filtering): `category`, `subcategory_id`, `search_group_name`, `stock_price`.

Any other column in the items table can also be filtered, but without a scalar index.

**Supported operators:**

| Operator | SQL equivalent |
|----------|----------------|
| `$eq` | `=` |
| `$neq` | `!=` |
| `$lt` / `$gt` / `$lte` / `$gte` | `<` / `>` / `<=` / `>=` |
| `$in` / `$nin` | `IN` / `NOT IN` |
| `$and` / `$or` | `AND` / `OR` |

**Examples:**
```json
{ "category": { "$eq": "LIVRE" } }

{ "stock_price": { "$lte": 10 } }

{ "$and": [
    { "category": { "$eq": "MUSIQUE" } },
    { "is_geolocated": { "$eq": 1 } }
] }
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

### Troubleshooting

1. Retrieve lancedb database from the docker image: If you want to use the same vector database as the one which was already build and deploy (for debug purposes), you can retrieve the LanceDB vector database from the docker image.

   To do this, run:

   ```sh
   DOCKER_IMAGE_TAG=<docker_image_tag> make download-vector-database
   ```

   where `<docker_image_tag>` is the tag of the docker image you want to use. You can find those in [Artifact Registry](https://console.cloud.google.com/artifacts/docker/passculture-infra-prod/europe-west1/pass-culture-artifact-registry?authuser=2&project=passculture-infra-prod).

   - For instance:

     ```sh
     DOCKER_IMAGE_TAG=europe-west1-docker.pkg.dev/passculture-infra-prod/pass-culture-artifact-registry/data-gcp/retrieval-vector/prod/retrieval_recommendation_v1_2_prod:two_towers_user_recommendation_prod_v20250428 make download-vector-database
     ```

   - ⚠️ If you use a production model, please delete the Docker image locally after use. ⚠️
