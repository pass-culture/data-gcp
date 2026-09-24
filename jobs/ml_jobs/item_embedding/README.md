# Item Embedding

Generates item embeddings from catalogue metadata using Sentence Transformers.

The work is split by **vector** (an embedding flavour, e.g. `all_items_metadata`,
`movies_metadata`, `books_metadata`) and, within a vector, by **functionality**
into GCS-staged steps. The `item_embedding` Airflow DAG runs, per selected
vector: export input → prepare (preprocess + build prompts) → embed → load.

## Design: where each fact lives

| Fact | Owner |
|------|-------|
| Which items belong to a vector (filtering) + which columns exist | **dbt** — input models in `orchestration/dags/data_gcp_dbt/models/machine_learning/input/ml_input__*_to_embed.sql` (dataset `ml_input`) |
| How columns become a prompt (features, labels, template, preprocessors) + encoder | **YAML** — `configs/<name>.yaml` |
| Wiring (name → input table, output table) | **DAG** — `AVAILABLE_VECTORS` in `orchestration/dags/jobs/ml/item_embedding.py` |

The vector **`name`** is the YAML filename and the value passed to every CLI via
`--config-file-name` (it also names the output staging table). Each vector's dbt
input table is wired explicitly in `AVAILABLE_VECTORS`.

## Pipeline steps

Each step is a CLI in `cli/`, reads a GCS folder, and writes a GCS folder for
the next step. Run from the job root as a module:

```bash
# 1. prepare: apply preprocessors + render the prompt (template or "label :
#    value"); drop items with an empty prompt
uv run python -m cli.prepare \
  --config-file-name movies_metadata \
  --input-parquets-folder-path  gs://.../movies_metadata/input \
  --output-parquets-folder-path gs://.../movies_metadata/prompts

# 2. embed: encode prompts → embeddings
uv run python -m cli.embed \
  --config-file-name movies_metadata \
  --input-parquets-folder-path  gs://.../movies_metadata/prompts \
  --output-parquets-folder-path gs://.../movies_metadata/embeddings
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
(`ml_semantic_embedding_<env>.<name>_tmp`), one `embedding` column of
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

## Need to add a new vector to embed ?

1. Create a new config yaml file in `configs/`. Follow the `configs/README.md` to fill the yaml or follow the pattern in the existing yaml files. Please follow the naming convention of the vectors, ususally `name of your vector = <which items?>_<which features?>` (e.g. all_items_metadata).

2. Next, you need the input data of your new vector, you can either:
    a. use an existing model in `data-gcp/orchestration/dags/data_gcp_dbt/models/machine_learning/input/` that already contain the all items and features you want to embedd.

    b. create a new input DBT model for your vector in that same directory which contans exactly the items you want to embed and their features. Please follow the naming convention: `<ml_input__<name of your vector>_to_embed>`. If you do create a new input model, make sure you run the DBT model before running the embedding job so the table is created in BigQuery. To do so run
    ```bash
    dbt run -s <name of your new model> -t <ENV_SHORT_NAME>
    ```

3. In the `item_embedding.py`DAG, add the new vector to the list of `AVAILABLE_VECTORS` by creating a new VectorPipeline with your vector name, input table and output table followi,g the conventions of other vectors.

4. Run the DAG (on the airflow of the environmenet of your choice). You can choose to embedd only one vector during the run.
