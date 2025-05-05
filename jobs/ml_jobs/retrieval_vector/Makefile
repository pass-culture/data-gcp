SHELL := /bin/bash

DOCKER_IMAGE_TAG ?= europe-west1-docker.pkg.dev/passculture-infra-prod/pass-culture-artifact-registry/data-gcp/retrieval-vector/dev/retrieval_recommendation_v1_2_dev:dummy_user_recommendation_dev_v20250430

install-api:
	uv sync --no-dev
	DOCKER_IMAGE_TAG=$(DOCKER_IMAGE_TAG) make download-vector-database

download-vector-database:
	gcloud auth login
	gcloud auth configure-docker europe-west1-docker.pkg.dev
	(docker rm temp-copy-container || true) && docker create --name temp-copy-container $(DOCKER_IMAGE_TAG)
	docker cp temp-copy-container:metadata . && docker rm temp-copy-container

start:
	uv run --no-dev hypercorn --bind 0.0.0.0:8080 app.app:app --workers=1 --log-config="json:app/logging/config.json"
