SHELL := /bin/bash


# Age threshold in minutes 3h
ADC_MAX_AGE_MIN := 180

IS_WSL := $(if $(WSL_DISTRO_NAME),yes,no)

# Path to ADC file wsl windows vs linux/macOS
ifeq ($(IS_WSL),yes)
	ADC_FILE := mnt/c/$(HOME)/AppData/gcloud/application_default_credentials.json
else
	ADC_FILE := $(HOME)/.config/gcloud/application_default_credentials.json
endif

check-auth:
	@if [ ! -f "$(ADC_FILE)" ]; then \
		echo "No ADC file found, authenticating…"; \
		gcloud auth application-default login; \
	elif find "$(ADC_FILE)" -mmin +$(ADC_MAX_AGE_MIN) | grep -q .; then \
		echo "ADC file older than 3h, re-authenticating…"; \
		gcloud auth application-default login; \
	else \
		echo "ADC file is fresh ( ≤3h )—no need to re-authenticate."; \
	fi


auth:
	make check-auth

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

## ── DAG testing helpers ──────────────────────────────────────────────────────
## DAG=  filename or stem (with or without .py).  ENV defaults to dev.
## Examples:
##   make test-dag DAG=bigquery_alerts
##   make test-dag DAG=bigquery_alerts.py ENV=stg

ENV ?= dev

create-test-dag: ## Create a local test copy of a DAG (no upload). make create-test-dag DAG=<name>
	@PATTERN=$$(echo "$(DAG)" | sed 's/\.py$$//').py; \
	DAG_PATH=$$(find dags -name "$$PATTERN" -not -path "*/__pycache__/*" 2>/dev/null); \
	[ -z "$$DAG_PATH" ] && { echo "❌ No DAG found matching: $$PATTERN"; exit 1; }; \
	COUNT=$$(echo "$$DAG_PATH" | wc -l | tr -d ' '); \
	[ "$$COUNT" -gt 1 ] && { echo "❌ Multiple matches — be more specific:"; echo "$$DAG_PATH"; exit 1; }; \
	uv run scripts/create_test_dag.py "$$DAG_PATH"

push-dag: check-auth ## Upload a DAG file to GCS. make push-dag DAG=<name> [ENV=dev]
	@PATTERN=$$(echo "$(DAG)" | sed 's/\.py$$//').py; \
	DAG_PATH=$$(find dags -name "$$PATTERN" -not -path "*/__pycache__/*" 2>/dev/null); \
	[ -z "$$DAG_PATH" ] && { echo "❌ No DAG found matching: $$PATTERN"; exit 1; }; \
	COUNT=$$(echo "$$DAG_PATH" | wc -l | tr -d ' '); \
	[ "$$COUNT" -gt 1 ] && { echo "❌ Multiple matches — be more specific:"; echo "$$DAG_PATH"; exit 1; }; \
	echo "🚀  $$DAG_PATH  →  gs://airflow-data-bucket-$(ENV)/$$DAG_PATH"; \
	gcloud storage cp "$$DAG_PATH" "gs://airflow-data-bucket-$(ENV)/$$DAG_PATH"; \
	echo "✅ Done. DAG should appear in Airflow $(ENV) within 1-2 min."

test-dag: check-auth ## Create a test copy and push it. make test-dag DAG=<name> [ENV=dev]
	@PATTERN=$$(echo "$(DAG)" | sed 's/\.py$$//').py; \
	DAG_PATH=$$(find dags -name "$$PATTERN" -not -path "*/__pycache__/*" 2>/dev/null); \
	[ -z "$$DAG_PATH" ] && { echo "❌ No DAG found matching: $$PATTERN"; exit 1; }; \
	COUNT=$$(echo "$$DAG_PATH" | wc -l | tr -d ' '); \
	[ "$$COUNT" -gt 1 ] && { echo "❌ Multiple matches — be more specific:"; echo "$$DAG_PATH"; exit 1; }; \
	uv run scripts/create_test_dag.py --force "$$DAG_PATH" || exit 1; \
	_GIT_USER=$$(git config user.name 2>/dev/null | tr '[:upper:]' '[:lower:]' | tr ' ' '_'); \
	_DAG_USER=$${_GIT_USER:-$$(whoami | tr '[:upper:]' '[:lower:]')}; \
	STEM=$$(basename "$$DAG_PATH" .py); \
	TEST_DAG="$$(dirname "$$DAG_PATH")/$${STEM}_test_$${_DAG_USER}.py"; \
	echo "🚀  $$TEST_DAG  →  gs://airflow-data-bucket-$(ENV)/$$TEST_DAG"; \
	gcloud storage cp "$$TEST_DAG" "gs://airflow-data-bucket-$(ENV)/$$TEST_DAG"; \
	echo "✅ Done. DAG should appear in Airflow $(ENV) within 1-2 min."
