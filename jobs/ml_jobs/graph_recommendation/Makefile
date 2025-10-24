PYTHON_VERSION := 3.12

setup:
	uv venv $(PYTHON_VERSION)
install:
	uv sync

test:
	PYTHONPATH=. uv run pytest tests
