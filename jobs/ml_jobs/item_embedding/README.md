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
uv run main.py \
  --input-parquets-folder-path gs://bucket/item_embedding_prod/<run_timestamp>/input_item_metadata \
  --output-parquets-folder-path gs://bucket/item_embedding_prod/<run_timestamp>/output_item_metadata
```

### With Custom Configuration

```bash
uv run main.py \
  --input-parquets-folder-path gs://bucket/item_embedding_prod/<run_timestamp>/input_item_metadata \
  --output-parquets-folder-path gs://bucket/item_embedding_prod/<run_timestamp>/output_item_metadata \
  --config-file-name my_config
```

### Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--config-file-name` | `default` | YAML config file name in `configs/` (without `.yaml`) |
| `--input-parquets-folder-path` | *required* | Path to input parquet folder (local or `gs://`) |
| `--output-parquets-folder-path` | *required* | Path to output parquet folder (local or `gs://`) |

## Configuration

Create a YAML file in the `configs/` directory:

```yaml
vectors:
  - name: "semantic_content"
    features:
      - offer_name
      - category_id
      - subcategory_id
      - offer_description
    encoder_name: "google/embeddinggemma-300m"
    prompt_name: "query"  # Optional, model-dependent
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

## What do we embed?

Each vector in the config file defines a `name` and the `features` to embed. Features are concatenated, in the order listed, into one prompt per item as `feature : value` pairs (null/empty features are omitted).

For example, the `semantic_content` vector in `default.yaml` encodes:

```text
offer_name : Manuel pratique de l'apprenti guerrier ; l'art chamanique du temps présent
category_id : LIVRE
subcategory_id : LIVRE_PAPIER
offer_label_concat : Religion & Esotérisme Autres religions Animisme / Chamanisme Théologie / Pratiques
author_concat : maja cardot
offer_description : Le chamanisme est le monde de la sensibilité construite et de la magie du vivant, à tous les niveaux. Le travail de l'apprenti guerrier, c'est la voie royale et complexe de la découverte de soi pour s'éveiller et faire fleurir, non seulement ses qualités, mais aussi ses pouvoirs dormants dans le fin fond de l'inconscient.
```

### Sequence length and truncation

A prompt longer than the model's `max_sequence_length` (2048 tokens for `embeddinggemma-300m`) is truncated by the model, dropping the **end** of the prompt first. Keep the longest feature (typically `offer_description`) **last** so truncation only trims that field and never the identifying ones.

This is a non-issue for our catalogue. As of 6 July 2026, across ~5M items with the default config:

| Metric | Value |
|--------|-------|
| Mean tokens | 167 |
| p99.9 tokens | 923 |
| Items > 1024 tokens | ~3,000 (0.06%) |
| Items > 2048 tokens | 239 (0.005%) |

Only 239 items in the entire catalogue are truncated at 2048, and each keeps its first 2048 tokens. No chunking is needed.

## Output Format

Output parquet file contains:
- `item_id`: Item identifiers from the input
- `content_hash`: Hash of the item state
- One column per configured vector (e.g., `semantic_content`) containing embedding arrays

## Architecture

```
main.py           — CLI entry point, orchestrates load → embed → upload
config.py         — YAML config loading, schema validation, Vector model
embedding.py      — Core embedding logic: prompt building, encoder management, GPU dispatch
gcs_utils.py      — GCS parquet I/O with retry logic
constants.py      — Environment variables and secret name mapping
gcp_secrets.py    — Secret Manager access with caching
setup_encoders.py - load encoder models, set the precision and pooling if available
```

## Performance

- **Cached prompt building**: Cache prompts if two vectors use the same features with different prompt names
- **Single encoder.encode() call**: Batching delegated to SentenceTransformer internally
- **Multi-GPU**: Automatic `encode_multi_process()` when >1 GPU detected
- **Encoder deduplication**: Each unique model is loaded once, even if used by multiple vectors

## Precision & hardware

- **Precision** is selected automatically: `bfloat16` on Ampere+ GPUs (compute capability ≥ 8, e.g. L4), `float32` otherwise (e.g. T4). `float16` is never used — Gemma models overflow in fp16 and produce NaN embeddings.
- **Machine sizing**: the model is small (~300M params) and prompts are short (mean ~167 tokens), so a single T4 handles the workload. Extra GPUs (T4 or L4) speed up large runs through the multi-process pool; prefer L4 for full-catalogue runs, where bf16 halves memory and improves throughput.
- **Batch size** is set by `BATCH_SIZE` in `constants.py`. It is the main memory/speed lever; sequence length is capped by the model itself (2048).

## Capacity & sizing

The job runs on a **single GCE VM** with N GPUs driven by the multi-process pool — it does not distribute across machines. So sizing means picking one VM and its GPU count.

The full catalogue is ~5M items, mean ~167 tokens (short text), so the run is throughput-bound, not memory-bound. The key driver is precision: **T4 (Turing) has no bf16 and Gemma NaNs in fp16, so T4 runs in fp32** and loses the tensor-core speedup — L4 (bf16) is roughly 8–12× faster per GPU for this model.


**Recommendation:** full catalogue → one `g2-standard-48` (4× L4) in `europe-west1-c` with `provisioning_model=FLEX_START`. Incremental runs (new/changed items only) are small enough that 4× T4 is fine and more widely available. Avoid CPU: it is far too slow and cannot be scaled across machines here.

Most probably you will run into stockout even with flexstart so the solution would be to launch a GCloud reservation to provision the machine: **Must be sumbmitted 87 hours before start date, the minimum reservation duration is 24 hours**
```bash
gcloud compute future-reservations create draft-test-reservation \
    --project=passculture-data-prod \
    --zone=europe-west1-c \
    --machine-type=g2-standard-48 \
    --accelerator=count=4,type=nvidia-l4 \
    --total-count=1 \
    --start-time="2026-07-07T12:00:00+02:00" \
    --end-time="2026-07-08T12:00:00+02:00" \
    --planning-status=SUBMITTED \ ## DRAFT if you want to test
    --auto-delete-auto-created-reservations
```

then the day of the start of the reserbation trigger the `item_embedding` DAG with:
    - `reservation_name` = that reservation name
    - `provisioning_model` = **`STANDARD`** (required — FLEX_START will error out)
    - `gce_zone` = `europe-west1-c`, `instance_type` = `g2-standard-48`, `gpu_type` = `nvidia-l4`, `gpu_count` = `4` — **must match the reservation exactly**, or the insert won't consume it.
    - The `Creating <name>:` log line in the `gce_start_task` will show the `reservationAffinity` block so you can confirm it targeted the reservation.


## Troubleshooting

| Problem | Solution |
|---------|----------|
| Out of memory | Lower `BATCH_SIZE` in `constants.py`, or use a GPU with more memory |
| NaN / empty embeddings | Ensure precision is bf16 or fp32, never fp16 (see Precision & hardware) |
| Slow processing | Raise `BATCH_SIZE` in `constants.py`; verify the GPU is used (`nvidia-smi`) |
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
