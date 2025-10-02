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

The entry point lives in `src/graph_recommendation/graph_builder.py`. It
produces a bipartite `torch_geometric.data.Data` object with the following
characteristics:

* Book nodes are indexed by the `item_id` column.
* Metadata nodes are created for each distinct value in `DEFAULT_METADATA_COLUMNS`
  (`rayon`, `gtl_label_level_1` → `_level_4`, `artist_id`).
* Empty values are skipped; every remaining `(book, metadata)` pair contributes
  a bidirectional edge.

### CLI

```bash
python -m scripts.cli build-graph \
  data/book_item_for_graph_recommendation.parquet \
  --output data/book_metadata_graph.pt \
  --nrows 5000  # optional sampling for quick iterations
```

* `build-graph` materialises the graph and serialises it with `torch.save`.
* `summary` runs the same pipeline but only prints node/edge counts.

### Python API

```python
from pathlib import Path
import torch

from graph_recommendation.graph_builder import (
    build_book_metadata_graph,
    build_book_metadata_graph_from_dataframe,
    DEFAULT_METADATA_COLUMNS,
)

graph_data = build_book_metadata_graph(
    Path("data/book_item_for_graph_recommendation.parquet"),
    nrows=10_000,
)
torch.save(graph_data, Path("data/book_metadata_graph.pt"))

# Or reuse a dataframe if it is already in memory
graph_data = build_book_metadata_graph_from_dataframe(
    dataframe,
    metadata_columns=DEFAULT_METADATA_COLUMNS,
    id_column="item_id",
)
```

The returned `Data` instance carries helper attributes to reconnect embeddings
to the raw identifiers:

* `book_ids` / `metadata_ids` / `node_ids`
* `book_mask` / `metadata_mask`
* `metadata_type_to_id` and the ordered `metadata_columns`
* `node_type` marking the type id of every node (0 for books)

Access `graph_data.edge_index` for the COO representation expected by PyG
models.

## Development

Run the test suite with:

```bash
pytest
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
