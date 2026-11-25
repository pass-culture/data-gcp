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
- Change dependencies with `uv add` / `uv remove`, never by editing `uv.lock`. Security bumps: `vulnerability-fix` skill.

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

- Create PRs with the `data-pr-workflow` skill: branch name, commits, title, description and #data-review message.
  Do not use `.github/PULL_REQUEST_TEMPLATE/` nor `.github/pull_request_template.md`: they are obsolete.
- Base branch `master`, squash and merge.
- The title is checked by `.github/workflows/on_pr__title_checker.yml`, e.g. `(DS-123) feat(item_embedding): add multilingual vector`.
- Merges to `master` are blocked outside 12:00–18:00 Paris time, unless the title starts with `(HF)`.

## Skills

Shared skills live in `.agents/skills/`:

- `data-pr-workflow`: branch, commit and PR conventions.
- `pr-review`: review a PR and rate it /10.
- `vulnerability-fix`: fix vulnerable packages in one `uv.lock`.
- `dependency-vulnerability-pr`: triage Dependabot alerts into safe and breaking PRs.
- `code-scanning-triage`: triage Semgrep/SonarCloud code scanning alerts (fix or inline suppression), one PR per team.
- `test-dag-from-diff`: upload test copies of changed DAGs to dev/stg Airflow.
- `yaml-doc-test`: write dbt schema YAML and docs for mart models.
- `sql-commenter`: comment BigQuery/dbt SQL without changing logic.
- `code-logic-schema`: draw Mermaid diagrams of a diff.
- `gce-to-gke-migration`: move DAGs from GCE VM operators to Kubernetes pods.

## Maintaining these files

Keep each `AGENTS.md` a short map, not a manual: link existing docs instead of copying them,
and add a rule only after an agent repeats the same mistake.
