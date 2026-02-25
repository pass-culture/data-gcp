install-api:
	uv sync --no-dev
streamlit:
	PYTHONPATH=. streamlit run streamlits/st_visualise_api_response.py
start_app:
	uv run --no-dev hypercorn --bind 0.0.0.0:8085 app.app:app --workers=1
test:
	PYTHONPATH=. pytest tests
