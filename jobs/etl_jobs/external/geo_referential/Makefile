YEAR ?= 2026
DENSITY_YEAR ?= 2024
FRR_YEAR ?= 2025
DATASET ?= raw_dev

.PHONY: install test lint dry-run run

install:
	uv sync

test:
	uv run pytest tests

lint:
	uv run ruff check . && uv run ruff format --check .

dry-run:
	uv run main.py --year $(YEAR) --density-year $(DENSITY_YEAR) --frr-year $(FRR_YEAR) \
		--destination-dataset-id $(DATASET) --dry-run

run:
	uv run main.py --year $(YEAR) --density-year $(DENSITY_YEAR) --frr-year $(FRR_YEAR) \
		--destination-dataset-id $(DATASET)
