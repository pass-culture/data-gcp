# AGENTS.md: ml_jobs

Owned by DS. Each folder is an independent ML microservice, run by an Airflow DAG in `orchestration/dags/jobs/ml/`.

## Jobs

| Job | Purpose | Tests |
|---|---|---|
| `algo_training` | Train the Two-Tower recommendation model (TensorFlow, GPU) and deploy it to Vertex AI | yes |
| `artist` | Link products to artists, enrich them with Wikidata, build artist similarity | yes |
| `endpoint_monitoring` | Monitoring tests for the recommendation endpoints | yes |
| `event_linkage` | Group offers into events by image, name and description similarity (GPU) | yes |
| `finance` | Pricing forecast for the finance team (Prophet) | yes |
| `graph_recommendation` | Book/metadata graph embeddings (PyG, Node2Vec, MetaPath2Vec) | yes |
| `item_embedding` | Sentence-Transformers item embeddings, one YAML config per vector in `configs/` | yes |
| `item_linkage` | Link products and offers through semantic vectors | yes |
| `ranking_endpoint` | LightGBM ranking model and its serving image | no |
| `retrieval_vector` | Recommendation retrieval API on LanceDB | yes |
| `semantic_db_creation` | Build the LanceDB semantic database used by `retrieval_vector` | yes |

`_template/` is legacy: do not copy it.

## Working on a job

From `jobs/ml_jobs/<job>/` (as in `.github/workflows/reusable_job_test.yml`):

```bash
uv sync --all-extras
PYTHONPATH=. uv run pytest tests
```

- The Python version is per job: `requires-python` in the job's `pyproject.toml` is authoritative, READMEs can be stale.
- Add dependencies with `uv add <pkg>` from the job folder.
- Lint from the repo root with `make ruff_check` (fix: `make ruff_fix`). The job's `[tool.ruff]` applies automatically.
  Do not run the job's own `ruff`: its version differs from CI (0.5.7).
- `ranking_endpoint` contains two uv projects: the job root and `app/`.

## Code conventions

For new or refactored code. Older jobs use top-level scripts: do not spread that pattern.

- Layout: `main.py` (typer dispatcher) → thin `cli/<domain>.py` commands → logic in `src/` → `tests/`.
  References: `item_embedding/cli/embed.py`, `artist/main.py`.
- Absolute imports from the job root (`from src.x import ...`), hence `PYTHONPATH=.`. No relative imports.
- CLI options with `typer.Option(...)`. Logging with `loguru`.
- Environment from `ENV_SHORT_NAME` and `GCP_PROJECT_ID`, read in `src/constants.py`.
  Secrets from Secret Manager, never hard-coded.
- Data I/O: prefer parquet files on GCS. The DAG exports from and loads into BigQuery; avoid direct BigQuery access in the job.
- Tests never call GCP or MLflow: mock them. Examples: `graph_recommendation/tests/conftest.py` (disables MLflow),
  `artist/tests/conftest.py` (patches GCP clients).

## Link with Airflow

- Each job runs on a GCE VM from a DAG in `orchestration/dags/jobs/ml/`, built with `orchestration/dags/common/operators/gce.py`:
  `StartGCEOperator` → `InstallDependenciesOperator` (`uv sync` of the job on the DAG's branch)
  → `SSHGCEOperator` (runs the CLI) → `DeleteGCEOperator`.
- When you change a CLI signature, extras or `requires-python`, update the DAG (command, `python_version`)
  and run the orchestration tests (see `orchestration/AGENTS.md`).
  `tests/dags_test.py` checks that each DAG's `python_version` satisfies the job's `requires-python`.

## New job

Created from `automations/configs/templates/common_layout/` by `MS_NAME=<name> make create_microservice_ml`.
It auto-commits, so ask the user to run it.

## Gotchas

- GPU dependencies are platform-specific: TensorFlow CUDA on Linux only (`algo_training`), PyTorch `cu128` index
  (`event_linkage`). Do not change indexes or pins without asking.

## Never

- Run training, model deployment or image builds, e.g. `algo_training/deploy_model.py`, `ranking_endpoint/deploy_model.py`,
  `retrieval_vector/cli/build_and_push_docker_image.py`, `*/deploy_to_docker_regist*.sh`.
- Log to the prod MLflow (`https://mlflow.passculture.team`) or write to prod buckets or BigQuery.
