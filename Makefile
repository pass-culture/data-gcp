install:
	make initiate_env
	make get_gcp_credentials
	pyenv install 3.10.4 -s
	pyenv local 3.10.4
	pip install --upgrade pip
	MICROSERVICE_PATH=. VENV_NAME=data-gcp REQUIREMENTS_NAME=linter-requirements.txt make install_microservice
	MICROSERVICE_PATH=apps/recommendation/api VENV_NAME=data-gcp-api REQUIREMENTS_NAME=api-dev-requirements.txt make install_microservice
	MICROSERVICE_PATH=jobs/ml_jobs/algo_training VENV_NAME=data-gcp-algo-training REQUIREMENTS_NAME=requirements.txt make install_microservice
	MICROSERVICE_PATH=jobs/ml_jobs/record_linkage VENV_NAME=data-gcp-record-linkage REQUIREMENTS_NAME=requirements.txt make install_microservice
	MICROSERVICE_PATH=orchestration VENV_NAME=data-gcp-orchestration REQUIREMENTS_NAME=airflow/orchestration-requirements.txt make install_microservice
	MICROSERVICE_PATH=apps/fraud/compliance/api VENV_NAME=data-gcp-compliance REQUIREMENTS_NAME=requirements.txt make install_microservice


install_microservice:
	cd $(MICROSERVICE_PATH) && (pyenv virtualenv $(VENV_NAME) || echo "pyenv-virtualenv $(VENV_NAME) already exists") && pyenv local $(VENV_NAME) && pip install --upgrade pip && pip install -r $(REQUIREMENTS_NAME)

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