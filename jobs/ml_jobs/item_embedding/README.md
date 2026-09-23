# Item Embedding

Generates item embeddings from catalogue metadata using Sentence Transformers.

The work is split by **vector** (an embedding flavour, e.g. `semantic_content`,
`movies_content`, `books_content`) and, within a vector, by **functionality**
into GCS-staged steps. The `item_embedding` Airflow DAG runs, per selected
vector: export input → prepare (preprocess + build prompts) → embed → load.

## Design: where each fact lives

| Fact | Owner |
|------|-------|
| Which items belong to a vector (filtering) + which columns exist | **dbt** — `orchestration/dags/data_gcp_dbt/models/machine_learning/embeddings/ml_embedding__input_<name>_embeddings.sql` |
| How columns become a prompt (features, labels, template, preprocessors) + encoder | **YAML** — `configs/<name>.yaml` |
| Wiring (name → input table, output table) | **DAG** — `AVAILABLE_VECTORS` in `orchestration/dags/jobs/ml/item_embedding.py` |

The vector **`name`** is the single join key across all three: it's the dbt
model suffix, the YAML filename, and the value passed to every CLI via
`--config-file-name`.

## Pipeline steps

Each step is a CLI in `cli/`, reads a GCS folder, and writes a GCS folder for
the next step. Run from the job root as a module:

```bash
# 1. prepare: apply preprocessors + render the prompt (template or "label :
#    value"); drop items with an empty prompt
uv run python -m cli.prepare \
  --config-file-name movies_content \
  --input-parquets-folder-path  gs://.../movies_content/input \
  --output-parquets-folder-path gs://.../movies_content/prompts

# 2. embed: encode prompts → embeddings
uv run python -m cli.embed \
  --config-file-name movies_content \
  --input-parquets-folder-path  gs://.../movies_content/prompts \
  --output-parquets-folder-path gs://.../movies_content/embeddings
```

### Data contract between steps

| Stage | Columns |
|-------|---------|
| input (from dbt via BQ export) | `item_id`, `content_hash`, `to_embed`, + the vector's feature columns |
| prompts | `item_id`, `content_hash`, `prompt` |
| embeddings | `item_id`, `content_hash`, `embedding` (`list<float>`) |

## Module map

```
config.py           Vector model + load_vector_config (one vector per YAML)
preprocessing.py    reusable column cleaners + apply_preprocessors
prompt_building.py  build_prompts: template or "label : value"
embedding.py        encode() + LongPromptTracker + find_long_prompts
setup_encoders.py   load_encoder + multi-GPU pool
gcs_utils.py        parquet streaming reads + writers
cli/                prepare.py (preprocess + build prompts), embed.py — one CLI per step
configs/            one YAML per vector (see configs/README.md)
```

## Configuration

One YAML per vector in `configs/`. See [configs/README.md](configs/README.md)
for the full field reference and registered preprocessors. Filtering is **not**
in the YAML — it's owned by the vector's dbt input model.

## Output

Each vector is loaded into its own BigQuery staging table
(`ml_embeddings_<env>.<name>_embeddings_tmp`), one `embedding` column of
`REPEATED FLOAT`. A downstream dbt model merges the staging tables (out of scope
for this job).

## Running the whole thing

Use the `item_embedding` DAG. Pick vectors with the `vectors` param (defaults to
all). Embeds run **sequentially on one shared VM** (chained to avoid GPU
contention), so for `embed_all` trigger the DAG **one vector at a time** (single
`vectors` entry) to give each full-catalogue vector its own VM/sizing.

## Sequence length & truncation

Prompts longer than `MAX_SEQ_LENGTH` (`constants.py`, 512) are silently
truncated by the encoder (dropping the end). The embed step flags likely
over-length prompts (cheap char pre-filter → exact tokenization of candidates)
and logs a per-run summary via `LongPromptTracker`; flagged items are still
embedded, not dropped. Keep the longest feature (usually `offer_description`)
last so only it gets trimmed.

## Precision & hardware

- **Precision** auto-selected: `bfloat16` on Ampere+ GPUs (e.g. L4), else
  `float32`. `float16` is never used (Gemma overflows → NaN).
- **Sizing**: model is small (~300M) and prompts short (mean ~167 tokens), so a
  single T4 handles incremental runs; prefer L4 (bf16) for the full catalogue.

## Testing

```bash
make install
uv run pytest
```
