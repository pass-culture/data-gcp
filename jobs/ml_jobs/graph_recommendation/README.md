# Graph Recommendation

Utilities to turn parquet exports of book offers into a PyTorch Geometric graph
that can be consumed by embedding pipelines (for example
[`torch_geometric.nn.models.Node2Vec`](https://pytorch-geometric.readthedocs.io/en/latest/generated/torch_geometric.nn.models.Node2Vec.html)).

## Installation

This project is managed with [uv](https://github.com/astral-sh/uv). Install the
package in editable mode to expose the CLI and Python modules:

```bash
make install
```

## Building the book → metadata graph

### Standard Bipartite Graph

The entry point lives in `src/graph_recommendation/graph_builder.py`. It
produces a bipartite `torch_geometric.data.Data` object with the following
characteristics:

* Book nodes are indexed by the `item_id` column.
* Metadata nodes are created for each distinct value in `DEFAULT_METADATA_COLUMNS`
  (`gtl_label_level_1` → `_level_4`, `artist_id`).
* Empty values are skipped; every remaining `(book, metadata)` pair contributes
  a bidirectional edge.

### Heterogeneous Graph

The heterograph builder in `src/graph_recommendation/heterograph_builder.py` creates
a `torch_geometric.data.HeteroData` object with typed nodes and edges:

* **Node types**: `"book"` for books, plus one type per metadata column
  (e.g., `"artist_id"`, `"gtl_label_level_1"`)
* **Edge types**: `("book", "is_{metadata}", "{metadata}")` and
  `("{metadata}", "{metadata}_of", "book")`
* This structure enables heterogeneous graph neural networks and metapath-based
  algorithms like MetaPath2Vec

### CLI

```bash
# Build and save standard bipartite graph
python -m scripts.cli build-graph \
  data/book_item_for_graph_recommendation.parquet \
  --output data/book_metadata_graph.pt \
  --nrows 5000  # optional sampling for quick iterations

# Build and save heterogeneous graph
python -m scripts.cli build-heterograph \
  data/book_item_for_graph_recommendation.parquet \
  --output data/book_metadata_heterograph.pt \
  --nrows 5000  # optional sampling for quick iterations

# Train MetaPath2Vec model on heterogeneous graph
python -m scripts.cli train-metapath2vec \
  data/book_item_for_graph_recommendation.parquet \
  --output-embeddings data/book_metadata_embeddings.parquet \
  --num-workers 8 \
  --nrows 5000  # optional sampling for quick iterations
```

* `build-graph` materialises the standard bipartite graph and serialises it
  with `torch.save`.
* `build-heterograph` materialises the heterogeneous graph and serialises it
  with `torch.save`.
* `train-metapath2vec` builds a heterogeneous graph and trains a MetaPath2Vec
  model on it, saving the resulting node embeddings as a parquet file.

### Python API

```python
from pathlib import Path
import torch

from src.graph_recommendation.graph_builder import (
    build_book_metadata_graph,
    build_book_metadata_graph_from_dataframe,
    DEFAULT_METADATA_COLUMNS,
)
from src.graph_recommendation.heterograph_builder import (
    build_book_metadata_heterograph,
    build_book_metadata_heterograph_from_dataframe,
)

# Standard bipartite graph
graph_data = build_book_metadata_graph(
    Path("data/book_item_for_graph_recommendation.parquet"),
    nrows=10_000,
)
torch.save(graph_data, Path("data/book_metadata_graph.pt"))

# Heterogeneous graph
hetero_graph_data = build_book_metadata_heterograph(
    Path("data/book_item_for_graph_recommendation.parquet"),
    nrows=10_000,
)
torch.save(hetero_graph_data, Path("data/book_metadata_heterograph.pt"))

# Or reuse a dataframe if it is already in memory
graph_data = build_book_metadata_graph_from_dataframe(
    dataframe,
    metadata_columns=DEFAULT_METADATA_COLUMNS,
    id_column="item_id",
)

hetero_graph_data = build_book_metadata_heterograph_from_dataframe(
    dataframe,
    metadata_columns=DEFAULT_METADATA_COLUMNS,
    id_column="item_id",
)
```

### Graph Attributes

Both graph types carry helper attributes to reconnect embeddings to the raw
identifiers:

**Standard bipartite graph (`Data`):**

* `book_ids` / `metadata_ids` / `node_ids`
* `book_mask` / `metadata_mask`
* `metadata_type_to_id` and the ordered `metadata_columns`
* `node_type` marking the type id of every node (0 for books)
* Access `graph_data.edge_index` for the COO representation expected by PyG
  models.

**Heterogeneous graph (`HeteroData`):**

* `book_ids` / `metadata_ids` / `metadata_ids_by_column`
* `metadata_columns` listing active metadata types
* Access `graph_data[src_type, edge_type, dst_type].edge_index` for typed edges
* Node types accessible via `graph_data.node_types` and `graph_data.edge_types`

## Development

Run the test suite with:

```bash
make test
```

The `tests/graph_builder_test.py` module covers the main invariants: undirected
edges, node masks, and the behaviour when no metadata values are present.

## Resources

###  Queries to get the training data

Get products for books with metadata and artists.

```sql
    WITH
        offers AS (
            SELECT
                offer_id,
                offer_product_id,
                item_id,
                offer_name,
                offer_subcategory_id,
                rayon
            FROM
                `passculture-data-prod.analytics_prod.global_offer`
            WHERE
                offer_category_id = "LIVRE" ),
        offers_with_extended_metadata AS (
            SELECT
                offers.offer_id,
                offers.offer_product_id,
                offers.item_id,
                offers.offer_name,
                offers.offer_subcategory_id,
                offers.rayon,
                offer_metadata.gtl_type,
                offer_metadata.gtl_label_level_1,
                offer_metadata.gtl_label_level_2,
                offer_metadata.gtl_label_level_3,
                offer_metadata.gtl_label_level_4,
                offer_metadata.author,
                artist_link.artist_id,
                artist_link.artist_type,
                artist.artist_name
            FROM
                offers
            LEFT JOIN
                `passculture-data-prod.analytics_prod.global_offer_metadata` offer_metadata
            USING
                (offer_id)
            LEFT JOIN
                `passculture-data-prod.raw_prod.applicative_database_product_artist_link` artist_link
            ON
                offers.offer_product_id = CAST(artist_link.offer_product_id AS STRING)
            LEFT JOIN
                `passculture-data-prod.raw_prod.applicative_database_artist` artist
            USING
                (artist_id)),
        offer_with_score AS (
            SELECT
                *,
                CAST(gtl_label_level_1 IS NOT NULL AS int) AS has_gtl
            FROM
                offers_with_extended_metadata)
    SELECT
        offer_id AS example_offer_id,
        offer_product_id,
        item_id,
        offer_name,
        offer_subcategory_id,
        rayon,
        gtl_type,
        gtl_label_level_1,
        gtl_label_level_2,
        gtl_label_level_3,
        gtl_label_level_4,
        author,
        artist_id,
        artist_name,
        artist_type
    FROM
        offer_with_score
        QUALIFY
        ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY has_gtl DESC ) = 1;
```

* The data is exported at this GCS location : `gs://data-bucket-prod/sandbox_prod/lmontier/graph_recommendation/book_item_for_graph_recommendation.parquet`
  * To download it:

    ```bash
    gsutil cp gs://data-bucket-prod/sandbox_prod/lmontier/graph_recommendation/book_item_for_graph_recommendation.parquet ./data/
    ```
