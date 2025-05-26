SHELL := /bin/bash

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
