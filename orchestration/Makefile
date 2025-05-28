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
		echo "ADC file is fresh (≤3h)—no need to re-authenticate."; \
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
	docker compose run --rm create-pools
	@echo "Waiting for Airflow to start..."
	@timeout 60 sh -c 'until curl -s http://localhost:8080 > /dev/null; do sleep 2; done' || (echo "Airflow did not start in time. Exiting." && exit 1)
	@echo "Airflow is ready!"
	python -m webbrowser "http://localhost:8080"

stop:
	docker compose stop;

show_airflow_logs:
	docker compose logs -f;
