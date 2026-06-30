# Testing Airflow orchestration

Covers `orchestration/dags/` — DAGs, custom operators, and plugins. Tests live in `orchestration/tests/`.

## Contents
- Layout and naming
- Running the tests
- The DAG fixture pattern (conftest)
- What to test / gap checklist

## Layout and naming

Tests sit in `orchestration/tests/` (e.g. `dags_test.py`, `test_delayed_waiting_operator.py`), with shared fixtures in `orchestration/tests/conftest.py`. Both `*_test.py` and `test_*.py` appear — match the closest existing file.

## Running the tests

The `dags/` directory must be on `PYTHONPATH` so tests can import from `common` and operator modules:

```bash
# from orchestration/
export PYTHONPATH="$PYTHONPATH:./dags"
uv run pytest tests
```

This mirrors CI (`reusable_test_orchestration.yml`).

## The DAG fixture pattern (conftest)

`conftest.py` adds `dags/` to `sys.path` and exposes reusable Airflow fixtures — a `test_dag`, and mock `DagRun`s for manual vs scheduled runs:

```python
DAGS_PATH = Path(__file__).parent.parent / "dags"
sys.path.insert(0, str(DAGS_PATH))

@pytest.fixture
def test_dag():
    return DAG("test_dag", start_date=datetime.datetime(2024, 1, 1),
               schedule="@daily", catchup=False)

@pytest.fixture
def manual_dag_run():
    dag_run = Mock(); dag_run.run_id = "manual__2024-01-15T12:00:00"; return dag_run
```

Use `test_dag` to instantiate operators under test; use the mock `DagRun` fixtures to drive run-type-dependent branching. Reuse these fixtures rather than redefining DAGs inline.

## What to test / gap checklist

- [ ] **DAG integrity** — DAGs import without errors and have no cycles / broken dependencies (the `dags_test.py` pattern)
- [ ] **Custom operator logic** — instantiate with `test_dag`, exercise `execute()` / branching with mocked context
- [ ] **Run-type branching** — behavior differs between `manual__` and `scheduled__` runs (use the mock `DagRun` fixtures)
- [ ] **External calls mocked** — no live GCP/Airflow connections; patch hooks/clients
- [ ] **Edge cases** — missing config/Variables, empty upstream results, retries/timeouts where relevant
