install:
	uv sync

test:
	PYTHONPATH=. uv run pytest tests/ -v

test-unit:
	PYTHONPATH=. uv run pytest tests/unit/ -v

test-integration:
	PYTHONPATH=. uv run pytest tests/integration/ -v

test-coverage:
	PYTHONPATH=. uv run pytest tests/ --cov=src --cov=scripts --cov-report=html --cov-report=term

get_books_from_yesterday:
	PYTHONPATH=. uv run --env-file .env python ./scripts/extract_new_offers_from_titelive.py \
		--offer-category paper \
		--min-modified-date $$(date -d "yesterday" +%Y-%m-%d) \
		--output-file-path data/yesterday_books.parquet

parse_products_from_yesterday:
	PYTHONPATH=. uv run python ./scripts/parse_offers.py \
		--min-modified-date $$(date -d "yesterday" +%Y-%m-%d) \
		--input-file-path data/yesterday_books.parquet \
		--output-file-path data/yesterday_books_parsed.parquet
