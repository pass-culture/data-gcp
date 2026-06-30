---
name: testing-services
description: Audit test coverage and write tests for this monorepo's components — Python microservices (jobs/ml_jobs, jobs/etl_jobs), Airflow orchestration (orchestration/tests), and dbt models (generic, singular, data, and unit tests). Use when the user wants to add or improve tests, check test coverage, audit how a service or model is tested, write a pytest suite, or create dbt tests. Trigger on mentions of pytest, conftest, fixtures, dbt test, generic tests, data_tests, unit tests, or "is this tested?".
---

# Testing Services

Two activities are supported: **auditing** existing tests (assess coverage and quality) and **creating** new tests. The repo has four test domains, each with its own conventions. First identify the domain, then follow the matching reference file.

## Step 1: Identify the domain and route

| If the target is in... | Domain | Read |
|---|---|---|
| `jobs/ml_jobs/<service>/` or `jobs/etl_jobs/<group>/<service>/` | Python microservice (pytest) | [reference/python-jobs.md](reference/python-jobs.md) |
| `orchestration/dags/` (DAGs, operators, plugins) | Airflow orchestration (pytest) | [reference/airflow-dags.md](reference/airflow-dags.md) |
| `orchestration/dags/data_gcp_dbt/models/` (`.sql` / `.yml`) | dbt model | [reference/dbt.md](reference/dbt.md) |

If the target spans more than one domain, handle each domain separately and read each reference file as you reach it. Do not read reference files for domains that aren't involved.

## Step 2: Audit workflow

Run this whether the user asked for an audit or asked to add tests (you must know current coverage before adding to it). Copy the checklist and track progress:

```
Audit Progress:
- [ ] 1. Run scripts/audit_coverage.py on the target to list untested modules/models
- [ ] 2. Locate existing tests (tests/ dir, or data_tests in schema .yml)
- [ ] 3. Map each unit of behavior to the tests covering it
- [ ] 4. Identify gaps: untested functions, branches, edge cases, columns
- [ ] 5. Run the existing tests to confirm they pass (see domain reference)
- [ ] 6. Report coverage + gaps, ranked by risk
```

**Step 1** — start with the structural gap finder; it auto-detects domain from the path:

```bash
# Python service: which source modules have no test file
python scripts/audit_coverage.py jobs/ml_jobs/<service>
# dbt: which models have no data test (run via uv for PyYAML)
uv run python scripts/audit_coverage.py orchestration/dags/data_gcp_dbt/models/<area>
```

It reports file/model -> test existence (a fast "what has zero tests" list), not line coverage; the domain reference file gives the deeper per-domain checks for step 4.

**Step 4 — what counts as a gap** depends on the domain (uncovered functions/branches for Python; uncovered models/columns/freshness for dbt). The domain reference file lists the specific checks. Report gaps before writing anything so the user can prioritize.

**Step 4b — classify every gap by severity** using the four levels below. Always include the severity in the audit report so the user can triage.

| Severity | Criteria |
|---|---|
| 🚨 **Critique** | A bug would produce **silently wrong results** (incorrect metrics, corrupted data, wrong predictions) with no exception raised. Covers: data transformation pipelines, metric computations, split/aggregation logic where bad output is indistinguishable from good output at runtime. |
| 🔴 **Élevé** | A bug would **break a key business flow** or leave an important branch (e.g. CV vs non-CV, logistic vs linear growth, weekly vs daily frequency) completely unvalidated. Covers: conditional evaluation paths, core model methods (`evaluate`, `run_backtest`, `predict`), feature-engineering branches. |
| 🟠 **Moyen** | Logic is probably correct but has **no safety net**: error-handling paths (ValueError, missing columns), factory/dispatch functions, concrete shared utilities, serialization. A regression here would be caught by users, not by CI. |
| 🟡 **Faible** | External-boundary wrappers (GCP, MLflow, HTTP), visualisation helpers, and orchestration entry points. Bugs are visible quickly; mocking cost is high relative to the logic tested. |

Apply these rules in order — assign the **highest** level that matches. Report all gaps grouped by severity before writing any test.

## Step 3: Create tests

1. Read the matching reference file for the exact conventions (naming, fixtures, mocking, file location) — these differ per domain and matter for CI.
2. **Mirror the closest existing test** in that same service/model. Match its structure, naming, fixture style, and assertion style rather than introducing a new pattern.
3. Write tests for the gaps surfaced in Step 2, prioritizing high-risk untested behavior.
4. **Run pre-commit checks** on the new files before running the tests (see §Pre-commit rules below). Fix all violations, then re-run until both ruff and the end-of-file fixer are clean.
5. Run the new tests (command is in the reference file) and iterate until green.
6. Report what was added and the new coverage picture.

## Pre-commit rules (apply to every generated Python file)

The repo enforces pre-commit hooks on every commit. Violations block the commit. Always run these two commands from the **service root** (where the local `pyproject.toml` lives) after writing or editing test files:

```bash
# 1. Format (ruff format auto-fixes style; re-run until "N files left unchanged")
uv run ruff format tests/

# 2. Lint with auto-fix (ruff check --fix handles most issues automatically)
uv run ruff check --fix tests/
```

If `ruff check` reports errors it cannot auto-fix, fix them manually, then re-run. Common issues to watch for:

| Hook | Common violation | Fix |
|---|---|---|
| `end-of-file-fixer` | File does not end with a newline | Ensure the last line of every file is a blank newline |
| `ruff format` | Trailing whitespace, quote style, blank lines, line length | Auto-fixed by `ruff format` — just re-run |
| `SIM117` (flake8-simplify) | Nested `with` statements | Combine into a single `with a(), b():` statement |
| `I` (isort) | Imports not sorted / grouped | Auto-fixed by `ruff check --fix` |
| `UP` (pyupgrade) | Old-style type hints (`Optional[X]`, `Union[X, Y]`) | Use `X \| None`, `X \| Y` (Python 3.10+ syntax) |
| `PT` (pytest-style) | `assert` on exception outside `pytest.raises` | Use `pytest.raises` context manager |
| `TCH` (type-checking) | Runtime import that should be under `TYPE_CHECKING` | Move import to `if TYPE_CHECKING:` block |

> **Note:** each service may extend the root ruff config in its own `pyproject.toml` with stricter rules (e.g. `SIM`, `PT`, `B`, `UP`). Always check `<service>/pyproject.toml` — if a `[tool.ruff]` section exists there, those rules apply to tests in that service.

## Conventions that apply everywhere

- **Match the neighbors.** Conventions vary by domain and even by service (e.g. `*_test.py` in ml_jobs vs `test_*.py` in etl_jobs). Always copy the local convention; never standardize across domains.
- **Test behavior, not implementation.** Assert on outputs and observable effects, not internal calls, unless mocking an external boundary (GCP, network, LLM).
- **Mock external boundaries only** — Secret Manager, BigQuery, HTTP APIs, model endpoints. Keep pure logic unmocked.
- **One concern per test**, with a name that states the behavior and expectation.
- **Make failures detectable.** A test that can't fail is a gap, not coverage.

## Quality bar before finishing

- [ ] New tests follow the local naming/layout convention (verified against a neighbor)
- [ ] `uv run ruff format tests/` outputs "N files left unchanged" (zero reformatted)
- [ ] `uv run ruff check tests/` outputs "All checks passed!" (zero errors)
- [ ] Tests run green with the domain's documented command
- [ ] Each new test fails if its asserted behavior breaks (sanity-check at least the riskiest one)
- [ ] No real external calls (secrets/BQ/network/endpoints are mocked)
- [ ] Audit gaps were reported to the user, not silently filled
