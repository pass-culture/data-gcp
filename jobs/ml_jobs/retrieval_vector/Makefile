SHELL := /bin/bash

DOCKER_IMAGE_TAG ?= europe-west1-docker.pkg.dev/passculture-infra-prod/pass-culture-artifact-registry/data-gcp/retrieval-vector/prod/retrieval_recommendation_v1_2_prod:two_towers_user_recommendation_prod_v20250429

install-api:
	uv venv -p 3.10
	source .venv/bin/activate && uv pip sync api-requirements.txt
	DOCKER_IMAGE_TAG=$(DOCKER_IMAGE_TAG) make download-vector-database

download-vector-database:
	gcloud auth configure-docker europe-west1-docker.pkg.dev
	docker create --name temp-copy-container $(DOCKER_IMAGE_TAG)
	docker cp temp-copy-container:metadata . && docker rm temp-copy-container

start:
	source .venv/bin/activate && hypercorn --bind 0.0.0.0:8080 app.app:app --workers=1 --log-config="json:app/logging/config.json"
