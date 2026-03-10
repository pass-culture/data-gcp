# Item Embedding Microservice

A microservice for generating embeddings from item metadata using Sentence Transformers.

## Features

- **Multiple Vector Types**: Supports multiple embedding configurations from a single input dataset
- **YAML-driven Configuration**: Easily customizable vector definitions with schema validation
- **Multi-GPU Support**: Automatically distributes work across GPUs for large datasets
- **Fail-fast Validation**: Validates config and features before loading models

## Installation

```bash
make install
```

## Usage

### Default Usage

```bash
python main.py \
  --input-parquet-filename gs://bucket/input_items.parquet \
  --output-parquet-filename gs://bucket/output_embeddings.parquet
```

### With Custom Configuration

```bash
python main.py \
  --config-file-name my_config \
  --input-parquet-filename gs://bucket/input_items.parquet \
  --output-parquet-filename gs://bucket/output_embeddings.parquet
```

### Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--config-file-name` | `default` | YAML config file name in `configs/` (without `.yaml`) |
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
- An `content_hash` column hashing the state of item metadata
- All columns referenced in the `features` configuration

## Output Format

Output parquet file contains:
- `item_id`: Item identifiers from the input
- `content_hash`: Hash of the item state
- One column per configured vector (e.g., `semantic_content_STS`) containing embedding arrays

## Architecture

```
main.py          — CLI entry point, orchestrates load → embed → upload
config.py        — YAML config loading, schema validation, Vector model
embedding.py     — Core embedding logic: prompt building, encoder management, GPU dispatch
gcs_utils.py     — GCS parquet I/O with retry logic
constants.py     — Environment variables and secret name mapping
gcp_secrets.py   — Secret Manager access with caching
```

## Performance

- **Cached prompt building**: Cache prompts if two vectors use the same features with different prompt names
- **Single encoder.encode() call**: Batching delegated to SentenceTransformer internally
- **Multi-GPU**: Automatic `encode_multi_process()` when >1 GPU detected
- **Encoder deduplication**: Each unique model is loaded once, even if used by multiple vectors

## Troubleshooting

| Problem | Solution |
|---------|----------|
| Out of memory | Reduce `--batch-size` or add more GPU kernels |
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
