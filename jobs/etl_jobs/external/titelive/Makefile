install:
	uv sync

get_books_from_yesterday:
	PYTHONPATH=. uv run --env-file .env python ./scripts/extract_new_offers_from_titelive.py \
		--offer-category paper \
		--min-modified-date $$(date -d "yesterday" +%Y-%m-%d) \
		--output-file-path data/yesterday_books.parquet

make parse_products_from_yesterday:
	PYTHONPATH=. uv run python ./scripts/parse_offers.py \
		--min-modified-date $$(date -d "yesterday" +%Y-%m-%d) \
		--input-file-path data/yesterday_books.parquet \
		--output-file-path data/yesterday_books_parsed.parquet
