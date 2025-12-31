setup:
	uv venv

install:
	uv sync

ruff_fix:
	uv run ruff check --fix
	uv run ruff format

ruff_check:
	uv run ruff check
	uv run ruff format --check
