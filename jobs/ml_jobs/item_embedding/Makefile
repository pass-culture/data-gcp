PYTHON_VERSION := 3.12

setup:
	uv venv $(PYTHON_VERSION)
install:
	uv sync
test:
	PYTHONPATH=. uv run pytest tests
ruff_fix:
	uv run ruff check --fix
	uv run ruff format
ruff_check:
	uv run ruff check
	uv run ruff format --check
