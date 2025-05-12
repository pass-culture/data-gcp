SHELL := /bin/bash

install-api:
	uv sync --no-dev

download-vector-database:
	if [ "$(DOCKER_IMAGE_TAG)" == "" ]; then \
		echo "Please set the DOCKER_IMAGE_TAG environment variable"; \
		exit 1; \
	fi
	gcloud auth login
	gcloud auth configure-docker europe-west1-docker.pkg.dev
	(docker rm temp-copy-container || true) && docker create --name temp-copy-container $(DOCKER_IMAGE_TAG)
	docker cp temp-copy-container:metadata . && docker rm temp-copy-container

start:
	uv run --no-dev hypercorn --bind 0.0.0.0:8080 app.app:app --workers=1 --log-config="json:app/logging/config.json"
