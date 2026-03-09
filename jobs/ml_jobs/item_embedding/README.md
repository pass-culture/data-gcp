# Item Embedding Microservice

A microservice for generating embeddings from item metadata using Sentence Transformers.

## Features

- **Multiple Vector Types**: Supports multiple embedding configurations from a single input dataset
- **YAML-driven Configuration**: Easily customizable vector definitions with schema validation
- **Multi-GPU Support**: Automatically distributes work across GPUs for large datasets
- **Retry Logic**: Upload retries for resilient GCS writes
- **Fail-fast Validation**: Validates config and features before loading models

## Installation

```bash
# Using uv (recommended)
uv sync

# Or using pip
pip install -e .
```

## Usage

### Basic Usage

```bash
python main.py \
  --input-parquet-filename gs://bucket/input_items.parquet \
  --output-parquet-filename gs://bucket/output_embeddings.parquet
```

### With Custom Configuration

```bash
python main.py \
  --config-file-name my_config \
  --batch-size 200 \
  --input-parquet-filename gs://bucket/input_items.parquet \
  --output-parquet-filename gs://bucket/output_embeddings.parquet
```

### Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--config-file-name` | `default` | YAML config file name in `configs/` (without `.yaml`) |
| `--batch-size` | `100` | Batch size for `encoder.encode()` (32-256 recommended) |
| `--input-parquet-filename` | *required* | Path to input parquet file (local or `gs://`) |
| `--output-parquet-filename` | *required* | Path to output parquet file (local or `gs://`) |

## Configuration

Create a YAML file in the `configs/` directory:

```yaml
vectors:
  - name: "semantic_content_STS"
    features:
      - offer_name
      - category_id
      - subcategory_id
      - offer_description
    encoder_name: "google/embeddinggemma-300m"
    prompt_name: "STS"  # Optional, model-dependent

  - name: "semantic_content_Clustering"
    features:
      - offer_name
      - category_id
    encoder_name: "google/embeddinggemma-300m"
    prompt_name: "Clustering"
```

### Configuration Fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Unique identifier for the embedding vector (becomes output column name) |
| `features` | Yes | List of input DataFrame columns to concatenate as prompt |
| `encoder_name` | Yes | HuggingFace model name or path |
| `prompt_name` | No | Prompt template name (model-dependent) |

## Input Format

Input parquet file must contain:
- An `item_id` column
- All columns referenced in the `features` configuration

## Output Format

Output parquet file contains:
- `item_id`: Item identifiers from the input
- One column per configured vector (e.g., `semantic_content_STS`) containing embedding arrays

## Architecture

```
main.py          — CLI entry point, orchestrates load → embed → upload with timing
config.py        — YAML config loading, schema validation, Vector model
embed_items.py   — Core embedding logic: prompt building, encoder management, GPU dispatch
storage.py       — GCS parquet I/O with retry logic
constants.py     — Environment variables and secret name mapping
utils.py         — Secret Manager access with caching
```

## Performance

- **Vectorized prompt building**: Column-wise pandas operations instead of row-by-row iteration
- **Single encoder.encode() call**: Batching delegated to SentenceTransformer internally
- **Multi-GPU**: Automatic `encode_multi_process()` when >1 GPU detected and dataset ≥ 100k items
- **Encoder deduplication**: Each unique model is loaded once, even if used by multiple vectors

## Troubleshooting

| Problem | Solution |
|---------|----------|
| Out of memory | Reduce `--batch-size` or use a smaller model |
| Slow processing | Increase `--batch-size`, verify GPU is used (`nvidia-smi`) |
| Missing features error | Check that input parquet columns match config `features` |
| Upload failures | Automatic retry (3 attempts). Check GCS permissions if persistent |

## Dependencies

- `sentence-transformers` — Embedding models
- `torch` — GPU detection and tensor operations
- `pandas` / `pyarrow` — Data I/O
- `gcsfs` — GCS filesystem access
- `google-cloud-secret-manager` — HuggingFace token retrieval
- `pydantic` — Configuration validation
- `typer` — CLI interface
- `loguru` — Logging
