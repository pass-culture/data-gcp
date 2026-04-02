##################       Dev       ##################
install:
	uv sync --no-dev
	[ -f .env ] || cp .env.template .env # only if .env doesn't exist, to avoid overwriting existing .env

test:
	PYTHONPATH=. uv run pytest tests

streamlit:
	PYTHONPATH=. streamlit run streamlits/st_visualise_api_response.py

################## 	         API          ##################
start_app:
	uv run --env-file .env --no-dev uvicorn app.app:app --host 0.0.0.0 --port 8085 --workers=2


##################       Deployment       ##################
build_and_push_docker_image:
	PYTHONPATH=. uv run --env-file .env --no-dev cli/build_and_push_docker_image.py

deploy_model:
	PYTHONPATH=. uv run --env-file .env --no-dev cli/deploy_model.py
