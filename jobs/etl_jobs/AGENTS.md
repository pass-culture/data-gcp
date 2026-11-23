# AGENTS.md: etl_jobs

Owned by DE. Keep changes minimal and ask before refactoring.

## Layout

- `external/<job>/`: jobs calling third-party APIs (legacy pattern).
- `connectors/`, `http_tools/`: shared API clients and rate limiters (current pattern for external APIs).
- `internal/<job>/`: jobs calling internal APIs and GCP resources.
- `jobs/<job>/`: jobs built on `connectors/`.

Each job is its own uv project (`pyproject.toml`, `uv.lock`) with a typer CLI in `main.py`.

## Tests

From the job folder, for jobs with a `tests/` folder (as in `.github/workflows/reusable_job_test.yml`):

```bash
uv sync --all-extras
PYTHONPATH=. uv run pytest tests
```

## Never

- Run `main.py` or `make run` against real sources or BigQuery unless asked. Use `--dry-run` when the job has it.
