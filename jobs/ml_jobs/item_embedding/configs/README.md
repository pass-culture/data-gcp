# Vector configs

One YAML file per embedding vector. Each file describes, at the top level, how
that vector turns item columns into a prompt and which encoder embeds it. The
filename (without `.yaml`) is the vector's `name` and the value passed to the
CLIs via `--config-file-name`.

Filtering (which items belong to a vector) and column selection are **not** here
— they are owned by the vector's dbt input model in
`orchestration/dags/data_gcp_dbt/models/machine_learning/embeddings/`. A config
only references columns that its dbt model produces.

## Fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | yes | Vector name; must equal the filename and the dbt model suffix. |
| `features` | yes | Columns (from the dbt input model) used to build the prompt. |
| `encoder_name` | yes | HuggingFace model name/path. |
| `prompt_name` | no | The encoder's named prompt to prepend (e.g. `document`). |
| `labels` | no | Per-feature label override for the default `"label : value"` prompt. |
| `prompt_template` | no | `str.format` template rendered per item instead of the default concat. Every `{field}` must be in `features`. |
| `preprocessors` | no | Map of `feature -> registered preprocessor` (see `preprocessing.py`), applied in the preprocess step. |

## How a vector is embedded

Each vector flows through two GCS-staged steps (see `cli/`), one DAG task each:

1. `prepare` — apply `preprocessors` to the feature columns, then render the
   prompt (`prompt_template`, or the default `"label : value"`).
2. `embed` — encode the prompts with `encoder_name`/`prompt_name`.

## Default vs template

- **Default** (no `prompt_template`): non-null features are concatenated as
  `label : value` lines (`label` from `labels`, else the column name).
- **Template**: a natural-language string; null features render as `""` (not
  `"None"`); an item with all features null yields an empty prompt and is
  dropped in the build_prompts step.

## Registered preprocessors

| Name | Description |
|------|-------------|
| `normalize_whitespace` | Collapse whitespace runs to single spaces, strip ends. |
| `clean_description` | Strip URLs / known boilerplate, then `normalize_whitespace`. |
| `format_movie_genres` | Format a `{"movies": {"genres": [...]}}` envelope as `"A, B"`. |
| `format_book_classification` | Format a `{"books": {"gtl1": ...}}` envelope as a labeled chevron chain. |

Add a vector: create a dbt input model + a YAML here + a `VectorPipeline` entry
in the DAG. The `name` ties the three together.
