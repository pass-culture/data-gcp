test:
	PYTHONPATH=. pytest tests

streamlit:
	PYTHONPATH=. streamlit run streamlits/st_visualise_api_response.py
install-api:
	uv sync --no-dev
start_app:
	uv run --no-dev hypercorn --bind 0.0.0.0:8082 app.app:app --workers=1
