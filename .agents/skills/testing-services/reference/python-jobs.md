# Testing Python microservices (pytest)

Covers `jobs/ml_jobs/<service>/` and `jobs/etl_jobs/<group>/<service>/`.

## Contents

- Layout and naming (differs between ml_jobs and etl_jobs)
- Running the tests
- Test structure conventions
- Mocking external boundaries (the conftest pattern)
- What to test / gap checklist

## Layout and naming

Each service has a `tests/` directory next to its source. **Naming differs by group — match the service you are in:**

- `jobs/ml_jobs/*` → suffix style: `preprocessing_utils_test.py`, `hello_test.py`
- `jobs/etl_jobs/*` → prefix style: `test_contentful_client.py`

Mirror the source tree inside `tests/` (e.g. `src/utils/preprocessing_utils.py` → `tests/utils/preprocessing_utils_test.py`). Test data fixtures live in `tests/<area>/data/` (e.g. CSV/parquet samples). Add an `__init__.py` where neighboring test dirs have one.

## Running the tests

From the **service root** (the dir containing `pyproject.toml`/`requirements.txt` and `tests/`):

```bash
PYTHONPATH=. pytest tests
```

ml_jobs services expose this as `make test` (see the service `Makefile`). CI runs `pytest tests || uv run pytest tests` with `PYTHONPATH="."`, so verify the bare `pytest tests` form works. Some services pin config in a local `pytest.ini`.

## Test structure conventions

- **Group with classes**: `class TestPreprocessingUtils:` with nested `class TestCleanNames:` for sub-behaviors. ml_jobs often use `@staticmethod` test methods inside these.
- **Fixtures** via `@pytest.fixture` for shared setup (clients, config dicts, sample frames).
- **DataFrame assertions**: use `pd.testing.assert_frame_equal`. When index/column order is irrelevant, copy the existing helper:

```python
def assert_frame_equal_ignore_index_nor_orders(result_df, expected_df):
    pd.testing.assert_frame_equal(
        result_df.loc[:, lambda df: df.columns.sort_values()].reset_index(drop=True),
        expected_df.loc[:, lambda df: df.columns.sort_values()].reset_index(drop=True),
    )
```

- Build small input frames, call the function, compare to an explicit expected frame. Always include an empty-input case.

## Mocking external boundaries (the conftest pattern)

Services that touch GCP/external APIs put mocks in `tests/conftest.py`. The established pattern (see `jobs/etl_jobs/external/contentful/tests/conftest.py`):

1. **Inject a mock module before imports** so importing the service code never fetches real secrets:

```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

class MockUtils:
    def access_secret_data(self, project_id, secret_id, version_id="latest", default=None):
        return {"contentful-token": "test-token"}.get(secret_id, f"test-{secret_id}")

sys.modules["jobs.etl_jobs.external.contentful.utils"] = MockUtils()
sys.modules["utils"] = MockUtils()  # also cover relative imports
```

2. **Patch external clients** with fixtures yielding `MagicMock`:

```python
@pytest.fixture
def mock_contentful_client():
    with patch("contentful.Client") as mock_client_class:
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        yield mock_client
```

Mock **only** the external boundary (Secret Manager, BigQuery, HTTP/SDK clients, model endpoints). Leave the service's own logic real so the test exercises it.

## What to test / gap checklist

Start with the structural gap finder to list modules that have **no** test file at all:

```bash
python scripts/audit_coverage.py jobs/ml_jobs/<service>   # or jobs/etl_jobs/<group>/<service>
```

Then go deeper on the modules it flags, plus those it marks covered but thinly tested:

- [ ] Every public function / class method in `src/` (or the service module) has at least one test
- [ ] Both branches of conditional logic; transformations on realistic data
- [ ] Edge cases: empty input, missing/null fields, malformed rows, boundary values
- [ ] External calls are mocked (no live secrets/BQ/network) — a leaked real call is itself a gap
- [ ] A failure path / error handling, where the code has one
