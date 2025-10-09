# AI Coding Agent Instructions - Graph Recommendation

## Project Overview

This is a **PyTorch Geometric (PyG) graph builder** for book recommendations at
pass-culture. It transforms book offer data from parquet exports into graphs
connecting books to metadata (rayon, GTL levels, artists) for downstream
embedding pipelines like Node2Vec.

**Key Architecture Options**:

1. **Bipartite graphs** (`graph_builder.py`) - books connect to metadata nodes
   but not to other books directly
2. **Heterogeneous graphs** (`heterograph_builder.py`) - typed nodes and edges
   enabling heterogeneous GNNs and MetaPath2Vec

## Development Workflow

### Environment & Dependencies

- **Package manager**: [uv](https://github.com/astral-sh/uv) (not pip)
- **Python version**: 3.12+ (see `pyproject.toml`)
- Install: `uv sync` (or `make install`)
- Run tests: `PYTHONPATH=. uv run pytest tests` (or `make test`)

### Testing Strategy

- Tests live in `tests/` with the pattern `*_test.py`
- Pytest is configured via `.vscode/settings.json`
- Key test invariants in `tests/graph_builder_test.py`:
  - Undirected edges (bidirectional symmetry)
  - Node masks (`book_mask` / `metadata_mask`)
  - Error handling when no metadata values present

### CLI Usage

```bash
# Build and save bipartite graph
python -m scripts.cli build-graph data/book_item_for_graph_recommendation.parquet \
  --output data/book_metadata_graph.pt --nrows 5000

# Build and save heterogeneous graph
python -m scripts.cli build-heterograph data/book_item_for_graph_recommendation.parquet \
  --output data/book_metadata_heterograph.pt --nrows 5000

# Train MetaPath2Vec model on heterogeneous graph
python -m scripts.cli train-metapath2vec data/book_item_for_graph_recommendation.parquet \
  --output-embeddings data/book_metadata_embeddings.parquet --num-workers 8 --nrows 5000
```

## Code Conventions

### Import Handling

- **Absolute imports only**: `ban-relative-imports = "all"` in ruff config
- Always use `from src.graph_recommendation.graph_builder import ...`
- Never use relative imports like `from ..graph_builder import ...`

### Type Annotations

- Use `from __future__ import annotations` at the top of every module
- Lazy imports for type checking: `if TYPE_CHECKING:` blocks
- Strict type checking enabled: `flake8-type-checking.strict = true`
- Runtime-evaluated base classes: Pydantic models exempt from TCH rules

### Docstrings & Style

- **Google-style docstrings** (enforced via ruff)
- Max doc length: 100 characters
- Selected linters: pycodestyle, Pyflakes, flake8-bugbear, isort, flake8-simplify, pyupgrade, flake8-pytest-style

### Data Normalization

- Empty/NaN/whitespace-only values are normalized to `None` via `_normalize_dataframe()`
- This prevents creating nodes for meaningless metadata

## Key Data Structures

### Standard Bipartite Graph (PyG Data Object)

The returned `torch_geometric.data.Data` instance includes custom attributes for mapping embeddings back to identifiers:

- `edge_index`: COO format edges (required by PyG models)
- `book_ids` / `metadata_ids` / `node_ids`: Ordered lists of original identifiers
- `book_mask` / `metadata_mask`: Boolean tensors for node filtering
- `node_type`: Tensor marking type ID (0 = book, 1+ = metadata types)
- `metadata_type_to_id`: Dict mapping column names to type IDs
- `metadata_columns`: Ordered list of metadata column names used

### Heterogeneous Graph (PyG HeteroData Object)

The returned `torch_geometric.data.HeteroData` instance includes:

- `book_ids`: Ordered list of book identifiers
- `metadata_ids`: Flattened list of (column, value) tuples
- `metadata_ids_by_column`: Dict mapping column names to value lists
- `metadata_columns`: Ordered list of active metadata column names
- `node_types` / `edge_types`: PyG properties listing all node/edge type names
- `[src_type, edge_type, dst_type].edge_index`: Typed edge indices

### Metadata Columns

Default columns from `DEFAULT_METADATA_COLUMNS`:

- `gtl_label_level_1` through `gtl_label_level_4`, `artist_id`

Metadata nodes use composite keys: `(column_name, value)` tuples stored in `metadata_ids`.

## Data Pipeline Context

### Source Data

- **Origin**: BigQuery export from `analytics_prod.global_offer` (books only)
- **Storage**: GCS at `gs://data-bucket-prod/sandbox_prod/lmontier/graph_recommendation/`
- **Download**: `gsutil cp gs://data-bucket-prod/.../*.parquet ./data/`
- **Schema**: See SQL query in README showing LEFT JOINs with metadata and artist tables

### Graph Construction Logic

1. Books indexed by `item_id` column
2. Each distinct metadata value creates a unique node
3. Every `(book, metadata_value)` pair → bidirectional edge
4. Empty values skipped during edge creation
5. Nodes numbered sequentially: books first, then metadata

## Common Gotchas

- **PYTHONPATH required**: Tests need `PYTHONPATH=.` to resolve `src.*` imports
- **Edge direction**: Edges are stored sorted, with both directions explicitly included
- **ValueError on empty graphs**: Raised if no valid metadata values found (prevents silent failures)
- **Sampling determinism**: `nrows` parameter uses `random_state=42` for reproducibility

## Extension Points

When adding features:

- New metadata columns: Update `DEFAULT_METADATA_COLUMNS` and document in README
- Alternative node types: Modify `metadata_type_to_id` logic in bipartite
  graph builder or the metadata type creation in heterograph builder
- Custom edge weights: Extend the `edges` set to include weight tuples in
  bipartite graphs, or add edge attributes in heterogeneous graphs
- Filters: Use `filters` parameter in both `build_book_metadata_graph()` and
  `build_book_metadata_heterograph()` for parquet row filtering
