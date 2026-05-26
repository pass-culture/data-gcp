# Graph Recommendation

Utilities to turn parquet exports of book and music offers into a PyTorch Geometric graph
that can be consumed by embedding pipelines such as Node2Vec and MetaPath2Vec.

## Installation

This project is managed with [uv](https://github.com/astral-sh/uv). Install the
package in editable mode to expose the CLI and Python modules:

```bash
make install
```

## Building the item → metadata graph

### Standard Bipartite Graph

The entry point lives in `src/graph_recommendation/graph_builder.py`. It
produces a bipartite `torch_geometric.data.Data` object with the following
characteristics:

* Item nodes are indexed by the `item_id` column.
* Metadata nodes are created for each distinct value in `DEFAULT_METADATA_COLUMNS`
  (`gtl_label_level_1` → `_level_4`, `artist_id`).
* Empty values are skipped; every remaining `(item, metadata)` pair contributes
  a bidirectional edge.

### Heterogeneous Graph

The heterograph builder in `src/heterograph_builder.py` creates
a `torch_geometric.data.HeteroData` object with typed nodes and edges supporting
multiple item types (books, music, …):

* **Node types**: `"item"` for all items (books, music, …), plus one type per metadata column
  (e.g., `"artist_id"`, `"gtl_label_level_1"`)
* **Edge types**: `("item", "is_{metadata}", "{metadata}")` and
  `("{metadata}", "{metadata}_of", "item")`
* `gtl_id` is prefixed per item type (`b-` for books, `m-` for music) at the DBT source
  level to prevent spurious cross-type similarity. `gtl_label_level_*` columns are
  not prefixed as their textual values are already distinct across item types.
* The input parquet file must contain an `item_type` column
* This structure enables heterogeneous graph neural networks and metapath-based
  algorithms like MetaPath2Vec

## Training Embeddings

### MetaPath2Vec

The `embedding_builder.py` module provides training functionality for MetaPath2Vec
models on heterogeneous graphs. The training pipeline:

* Uses a predefined metapath that traverses item-artist and item-GTL relationships
* Generates random walks following the metapath structure
* Trains embeddings using skip-gram with negative sampling
* Supports GPU acceleration when available
* Implements learning rate scheduling and early stopping
* Saves checkpoints and extracts item embeddings

### CLI

```bash
# Build and save standard bipartite graph
python -m scripts.cli build-graph \
  data/item_for_graph_recommendation.parquet \
  --output data/item_metadata_graph.pt \
  --nrows 5000  # optional sampling for quick iterations

# Build and save heterogeneous graph
python -m scripts.cli build-heterograph \
  data/item_for_graph_recommendation.parquet \
  --output data/item_metadata_heterograph.pt \
  --nrows 5000  # optional sampling for quick iterations

# Train MetaPath2Vec model on heterogeneous graph
python -m scripts.cli train-metapath2vec \
  data/item_for_graph_recommendation.parquet \
  --output-embeddings data/item_metadata_embeddings.parquet \
  --nrows 5000  # optional sampling for quick iterations
```

* `build-graph` materialises the standard bipartite graph and serialises it
  with `torch.save`.
* `build-heterograph` materialises the heterogeneous graph and serialises it
  with `torch.save`.
* `train-metapath2vec` builds a heterogeneous graph, trains a MetaPath2Vec
  model, and saves the resulting item embeddings as a parquet file with columns
  `node_ids` (item identifiers) and `embedding` (embedding vectors).

### Python API

```python
from pathlib import Path
import torch

from src.graph_builder import (
    build_book_metadata_graph,
    build_book_metadata_graph_from_dataframe,
    DEFAULT_METADATA_COLUMNS,
)
from src.heterograph_builder import (
    build_heterograph_from_parquet,
    build_multitype_metadata_heterograph_from_dataframe,
)
from src.constants import GTL_METADATA_COLUMNS, SHARED_METADATA_COLUMNS
from src.config import TrainingConfig
from src.embedding_builder import train_metapath2vec

# Standard bipartite graph (books only)
graph_data = build_book_metadata_graph(
    Path("data/item_for_graph_recommendation.parquet"),
    nrows=10_000,
)
torch.save(graph_data, Path("data/item_metadata_graph.pt"))

# Heterogeneous graph (multi-type: books + music)
hetero_graph_data = build_heterograph_from_parquet(
    Path("data/item_for_graph_recommendation.parquet"),
    nrows=10_000,
)
torch.save(hetero_graph_data, Path("data/item_metadata_heterograph.pt"))

# Train MetaPath2Vec embeddings
embeddings_df = train_metapath2vec(
    graph_data=hetero_graph_data,
    training_config=TrainingConfig(num_workers=8),
)
embeddings_df.to_parquet("data/item_embeddings.parquet", index=False)

# Or reuse a dataframe if it is already in memory
# (dataframe must contain an item_type column and a prefixed gtl_id)
hetero_graph_data = build_multitype_metadata_heterograph_from_dataframe(
    dataframe,
    gtl_metadata_columns=list(GTL_METADATA_COLUMNS),
    shared_metadata_columns=list(SHARED_METADATA_COLUMNS),
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

* `book_ids` — all item ids (books + music), legacy name kept for compatibility
* `item_ids_by_type` / `gtl_ids_by_type` / `item_types` — per-type mappings
* `metadata_ids` / `metadata_ids_by_column` / `metadata_columns`
* Access `graph_data[src_type, edge_type, dst_type].edge_index` for typed edges
* Node types accessible via `graph_data.node_types` and `graph_data.edge_types`

## Development

Run the test suite with:

```bash
make test
```

The test suite includes:

* `tests/graph_builder_test.py` - Tests for standard bipartite graph construction
  covering undirected edges, node masks, and error handling when no metadata
  values are present
* `tests/heterograph_builder_test.py` - Tests for heterogeneous graph construction
  validating node/edge types and metadata mappings
* `tests/embedding_builder_test.py` - Tests for MetaPath2Vec training pipeline
  including model initialization, training loop, and embedding extraction

## Resources

### DBT model

The training data is generated by the DBT model
`ml_graph_recommendation__item_with_metadata_to_embed` which exports books and music
items with their metadata. The `gtl_id` field is prefixed with the item type initial
(`b-` for books, `m-` for music) directly in the DBT model.

* The data is exported to GCS and then to BigQuery before being loaded as a parquet
  file for training.
