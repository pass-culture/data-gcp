# AGENTS.md: orchestration

Owned by DE (Airflow) and DA (dbt), except `dags/jobs/ml/` (DS, see `jobs/ml_jobs/AGENTS.md`).
Keep changes minimal and ask before refactoring.
Uses the root uv environment (`uv sync` at the repo root).

## Airflow

- `dags/jobs/`: DAG files. Schedules live in `dags/jobs/crons.py`.
- `dags/common/`: shared operators, hooks and `config.py`.
- `dags/dependencies/`: per-DAG helpers and SQL.
- `plugins/`, `tests/`.

Tests, from `orchestration/` (as in `.github/workflows/reusable_test_orchestration.yml`):

```bash
export AIRFLOW_HOME=$(pwd) DAG_FOLDER=./dags PYTHONPATH=$PYTHONPATH:./dags
export GCP_PROJECT=passculture-data-ehp GOOGLE_CLOUD_PROJECT=passculture-data-ehp GCLOUD_PROJECT=passculture-data-ehp
uv run airflow db migrate
uv run pytest tests
```

`tests/dags_test.py` imports every DAG and checks schedules and job Python versions.
To try a DAG on a real Airflow, use the `test-dag-from-diff` skill, on dev or stg only, and only when asked.

## dbt (`dags/data_gcp_dbt/`)

- Layers in `models/`: `raw`, `intermediate`, `mart`, `export`, `metrics`, `machine_learning`.
- Model names: `<raw|int|mrt|exp|ml>_<domain>__<name>`. Docs go in `_schema/` YAML (`yaml-doc-test` skill).

Validate from `dags/data_gcp_dbt/`:

```bash
uv run dbt compile -s <model>
uv run dbt build -s <model>   # default target `local`: writes to tmp_${PERSONAL_DBT_USER}_dev
uv run scripts/defer_local_run.py run -s <model> --defer-to prod   # reads prod state, writes locally
```

- SQL lint: `make sqlfmt_check` (CI) and the sqlfluff pre-commit hook, both from the repo root.
- Mart models must also pass `make precommit_docs_run` (dbt-checkpoint: properties file, all columns documented).
- Only use the `local` target. Never use the `dev`, `stg`, `prod`, `sandbox` or `elementary` targets or profiles:
  they write to shared or prod datasets.
