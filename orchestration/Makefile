SHELL := /bin/bash

sync:
	uv sync --no-cache-dir

compile:
	uv pip compile pyproject.toml -o airflow/orchestration-requirements.txt --python-platform=linux
	uv pip compile pyproject.toml -o k8s-airflow/k8s-worker-requirements.txt --python-platform=linux

auth:
	gcloud auth application-default login --no-launch-browser

build:
	docker compose down -v;
	docker compose build --no-cache;
	make start

build_with_cache:
	docker compose down -v;
	docker compose build;
	make start

start:
	make auth
	docker compose up -d
	@echo "Waiting for Airflow to start..."
	@timeout 60 sh -c 'until curl -s http://localhost:8080 > /dev/null; do sleep 2; done' || (echo "Airflow did not start in time. Exiting." && exit 1)
	@echo "Airflow is ready!"
	python -m webbrowser "http://localhost:8080"

stop:
	docker compose stop;

show_airflow_logs:
	docker compose logs -f;
