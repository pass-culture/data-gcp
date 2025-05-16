SHELL := /bin/bash

sync:
	uv sync --no-cache-dir

compile:
	uv export --format requirements-txt -o airflow/orchestration-requirements.txt --python=python3.10
	uv export --format requirements-txt -o k8s-airflow/k8s-worker-requirements.txt --python=python3.10

auth:
	gcloud auth application-default login

build:
	make auth
	docker compose down -v;
	docker compose build --no-cache;
	make _start

build_with_cache:
	make auth
	docker compose down -v;
	docker compose build;
	make _start

start:
	make auth
	make _start

_start:
	docker compose up -d
	@echo "Waiting for Airflow to start..."
	@timeout 60 sh -c 'until curl -s http://localhost:8080 > /dev/null; do sleep 2; done' || (echo "Airflow did not start in time. Exiting." && exit 1)
	@echo "Airflow is ready!"
	python -m webbrowser "http://localhost:8080"

stop:
	docker compose stop;
show_airflow_logs:
	docker compose logs -f;
