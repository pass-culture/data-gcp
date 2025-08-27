test:
	PYTHONPATH=. pytest tests

get_books_from_yesterday:
	PYTHONPATH=. uv run --env-file .env python ./scripts/extract_new_offers_from_titelive.py --offer-category paper --min-modified-date $$(date -d "yesterday" +%Y-%m-%d)
