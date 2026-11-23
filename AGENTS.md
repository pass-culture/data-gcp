# AGENTS.md

Instructions for coding agents working in `data-gcp`, the pass Culture data platform on GCP:
Airflow orchestration, dbt on BigQuery, Python ETL and ML jobs.
Background: `docs/project_structure_and_data_flow.md`.

Each area has its own `AGENTS.md`. Read the one closest to the files you touch.

## Map and ownership

| Path | Owner | Content | Read |
|---|---|---|---|
| `orchestration/` | DE (Airflow), DA (dbt) | Airflow DAGs, dbt project in `orchestration/dags/data_gcp_dbt/` | `orchestration/AGENTS.md` |
| `orchestration/dags/jobs/ml/` | DS | DAGs that run the ML jobs | `jobs/ml_jobs/AGENTS.md` |
| `jobs/etl_jobs/` | DE | Ingestion and reverse-ETL microservices | `jobs/etl_jobs/AGENTS.md` |
| `jobs/ml_jobs/` | DS | ML microservices | `jobs/ml_jobs/AGENTS.md` |
| `.github/` | CI owners | CI/CD workflows | `.github/workflows/README.md` |
| `automations/` | | Microservice scaffolding, dependency constraint sync | |
| `data_gouv/` | | Open-data publication to data.gouv.fr | |
| `docs/` | | MkDocs site | `docs/README.md` |

Owners come from `.github/CODEOWNERS`. Outside the user's own area, keep changes minimal and ask before refactoring.

## Python environments

- The root uv project (Python 3.12) serves `orchestration/` and the linters. Install with `uv sync` from the root.
- Every job under `jobs/` is its own uv project, excluded from the root workspace, with its own Python version.
  Run `uv` commands from the job folder.
- Change dependencies with `uv add` / `uv remove`, never by editing `uv.lock`. Security bumps: `vulnerability-fix` (one job) or `dependency-vulnerability-pr` (all jobs of a folder).

## Validate before finishing

CI is the source of truth. Run what CI runs, from the repo root:

```bash
make ruff_check     # fix with: make ruff_fix
make sqlfmt_check   # only if SQL under orchestration/dags changed; fix with: make sqlfmt_fix
```

Then run the tests of the area you touched (see its `AGENTS.md`).

Finally, run the checks that only pre-commit covers (YAML/JSON, whitespace, sqlfluff, ggshield secrets, uv-secure):

```bash
pre-commit run --files <changed files>
```

- sqlfluff needs GCP auth and ggshield needs GitGuardian auth. If a hook fails for an auth reason, report it.
  Never bypass hooks (`--no-verify`, `SKIP=`).
- When pre-commit and CI disagree (e.g. sqlfmt 0.32.0 in pre-commit vs 0.29.0 in CI), CI wins.

## Never

- Push or merge to `master` (deploys to stg) or `production` (deploys to prod).
- Run anything against prod: GCP project `passculture-data-prod`, dbt `--target prod`, `ENV=prod`, prod buckets or MLflow.
- Run `make install`, `make auth` (interactive, edit `~/.zshrc`) or `make create_microservice*` (auto-commits).
  Ask the user to run them.
- Commit `.env*`, credentials, `target/`, or test DAG copies (`*__test__*.py`).
- Put secrets or personal data in code, commits or PRs: this repository is public.

## Pull requests

- Base branch `master`, squash and merge. Templates: `.github/PULL_REQUEST_TEMPLATE/`.
- The title must pass `.github/workflows/on_pr__title_checker.yml`:
  `^\((DA|DE|AE|DS|HF|BSR|PC)(-[0-9]+)?\) (build|lint|ci|docs|feat|fix|perf|refactor|test|chore|dbt)\(\w+.*\): \w+.*`,
  e.g. `(DS-123) feat(item_embedding): add multilingual vector`.
- Merges to `master` are blocked outside 12:00–18:00 Paris time, unless the title starts with `(HF)`.
- Full workflow: `data-pr-workflow` skill. Where it disagrees with the title check above, the CI wins.

## Skills

Shared skills live in `.agents/skills/`:

- `data-pr-workflow`: branch, commit and PR conventions.
- `pr-review`: review a PR and rate it /10.
- `vulnerability-fix`: fix vulnerable packages in one `uv.lock`, run the tests, report a confidence level.
- `dependency-vulnerability-pr`: one subagent per job of a folder (`ml_jobs` by default), two PRs split by confidence.
- `test-dag-from-diff`: upload test copies of changed DAGs to dev/stg Airflow.
- `yaml-doc-test`: write dbt schema YAML and docs for mart models.
- `sql-commenter`: comment BigQuery/dbt SQL without changing logic.
- `code-logic-schema`: draw Mermaid diagrams of a diff.
- `gce-to-gke-migration`: move DAGs from GCE VM operators to Kubernetes pods.

## Maintaining these files

Keep each `AGENTS.md` a short map, not a manual: link existing docs instead of copying them,
and add a rule only after an agent repeats the same mistake.
