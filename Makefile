VENV_BASE_NAME = data-gcp-base

install_base:
	curl -LsSf https://astral.sh/uv/install.sh | sh
	make initiate_env
	make get_gcp_credentials
	pyenv install 3.10.4 -s
	pyenv virtualenv 3.10.4 $(VENV_BASE_NAME)
	@eval "$$(pyenv init -)" && pyenv activate $(VENV_BASE_NAME) && uv pip install --upgrade pip

clean_install:
	pyenv virtualenv-delete  -f $(VENV_BASE_NAME)
	find . -name ".python-version" -type f -exec cat {} \; | sort | uniq | xargs -n 1 pyenv virtualenv-delete -f || true
	find . -name ".python-version" -type f -delete

install_microservice:
	@eval "$$(pyenv init -)" && cd $(MICROSERVICE_PATH) && pyenv virtualenv $(VENV_BASE_NAME) $(VENV_NAME) && pyenv local $(VENV_NAME) && pyenv activate $(VENV_NAME) && uv pip install -r $(REQUIREMENTS_NAME)

install:
	make clean_install
	make install_base
	MICROSERVICE_PATH=. VENV_NAME=data-gcp REQUIREMENTS_NAME=linter-requirements.txt make install_microservice
	MICROSERVICE_PATH=jobs/ml_jobs/algo_training VENV_NAME=data-gcp-algo-training REQUIREMENTS_NAME=requirements.txt make install_microservice
	MICROSERVICE_PATH=jobs/ml_jobs/record_linkage VENV_NAME=data-gcp-record-linkage REQUIREMENTS_NAME=requirements.txt make install_microservice
	MICROSERVICE_PATH=jobs/ml_jobs/artist_linkage VENV_NAME=data-gcp-artist-linkage REQUIREMENTS_NAME=requirements.txt make install_microservice
	MICROSERVICE_PATH=orchestration VENV_NAME=data-gcp-orchestration REQUIREMENTS_NAME=airflow/orchestration-requirements.txt make install_microservice

initiate_env:
	cp -n .env.template .env.local

get_gcp_credentials:
ifeq (,$(wildcard ${HOME}/.config/gcloud/application_default_credentials.json))
	gcloud auth application-default login
endif


install_ubuntu_libs:
	sudo apt-get update
	sudo apt-get install -y make build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev gcc libpq-dev python3-dev

upload_dags_to_dev:
	$(eval COMPOSER_BUCKET_PATH=$(shell gcloud composer environments describe data-composer-dev --location europe-west1 --format='value(config.dagGcsPrefix)'))
	gsutil cp -r orchestration/dags/${path} $(COMPOSER_BUCKET_PATH)/${path}