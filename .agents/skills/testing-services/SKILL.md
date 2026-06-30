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

## Step 3: Create tests

1. Read the matching reference file for the exact conventions (naming, fixtures, mocking, file location) — these differ per domain and matter for CI.
2. **Mirror the closest existing test** in that same service/model. Match its structure, naming, fixture style, and assertion style rather than introducing a new pattern.
3. Write tests for the gaps surfaced in Step 2, prioritizing high-risk untested behavior.
4. Run the new tests (command is in the reference file) and iterate until green.
5. Report what was added and the new coverage picture.

## Conventions that apply everywhere

- **Match the neighbors.** Conventions vary by domain and even by service (e.g. `*_test.py` in ml_jobs vs `test_*.py` in etl_jobs). Always copy the local convention; never standardize across domains.
- **Test behavior, not implementation.** Assert on outputs and observable effects, not internal calls, unless mocking an external boundary (GCP, network, LLM).
- **Mock external boundaries only** — Secret Manager, BigQuery, HTTP APIs, model endpoints. Keep pure logic unmocked.
- **One concern per test**, with a name that states the behavior and expectation.
- **Make failures detectable.** A test that can't fail is a gap, not coverage.

## Quality bar before finishing

- [ ] New tests follow the local naming/layout convention (verified against a neighbor)
- [ ] Tests run green with the domain's documented command
- [ ] Each new test fails if its asserted behavior breaks (sanity-check at least the riskiest one)
- [ ] No real external calls (secrets/BQ/network/endpoints are mocked)
- [ ] Audit gaps were reported to the user, not silently filled
