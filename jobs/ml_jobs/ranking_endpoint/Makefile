SHELL := /bin/bash

install-api:
	uv pip sync -p 3.10 api-requirements.txt

download-ranking-model:
	if [ "$(DOCKER_IMAGE_TAG)" == "" ]; then \
		echo "Please set the DOCKER_IMAGE_TAG environment variable"; \
		exit 1; \
	fi
	gcloud auth login
	gcloud auth configure-docker europe-west1-docker.pkg.dev
	(docker rm temp-copy-container || true) && docker create --name temp-copy-container $(DOCKER_IMAGE_TAG)
	docker cp temp-copy-container:app/metadata app/metadata && docker rm temp-copy-container

start:
	source .venv/bin/activate && cd app && hypercorn --bind 0.0.0.0:8080 app:app --workers=1 --log-config="json:custom_logging/config.json"

start-dev:
	source .venv/bin/activate && cd app && hypercorn --bind 0.0.0.0:8080 app:app --reload --workers 1
