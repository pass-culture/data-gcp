install_base:
	make initiate_env
	pyenv install 3.10.4 -s
	curl -LsSf https://astral.sh/uv/install.sh | sh

install_microservice:
	# Recreate the venv if RECREATE_VENV is set to 1
	@if [ "$(RECREATE_VENV)" = "1" ]; then \
		eval "$$(pyenv init -)" && cd $(MICROSERVICE_PATH) && rm -f .python-version && pyenv virtualenv-delete -f $(VENV_NAME) && pyenv virtualenv $(PHYTON_VENV_VERSION) $(VENV_NAME) && pyenv local $(VENV_NAME); \
	fi
	# Install the requirements
	@eval "$$(pyenv init -)" && cd $(MICROSERVICE_PATH) && pyenv activate $(VENV_NAME) && uv pip install -r $(REQUIREMENTS_NAME)

install:
	# Log in with GCP credentials if NO_GCP_INIT is not 1
	@if [ "$(NO_GCP_INIT)" != "1" ]; then \
		make get_gcp_credentials; \
	fi
	make install_base
	MICROSERVICE_PATH=. PHYTON_VENV_VERSION=3.10.4 VENV_NAME=data-gcp REQUIREMENTS_NAME=requirements.txt RECREATE_VENV=$(CLEAN_INSTALL) make install_microservice
	MICROSERVICE_PATH=jobs/ml_jobs/algo_training PHYTON_VENV_VERSION=3.10.4 VENV_NAME=data-gcp-algo-training REQUIREMENTS_NAME=requirements.txt RECREATE_VENV=$(CLEAN_INSTALL) make install_microservice
	MICROSERVICE_PATH=jobs/ml_jobs/record_linkage PHYTON_VENV_VERSION=3.10.4 VENV_NAME=data-gcp-record-linkage REQUIREMENTS_NAME=requirements.txt RECREATE_VENV=$(CLEAN_INSTALL) make install_microservice
	MICROSERVICE_PATH=jobs/ml_jobs/artist_linkage PHYTON_VENV_VERSION=3.10.4 VENV_NAME=data-gcp-artist-linkage REQUIREMENTS_NAME=requirements.txt RECREATE_VENV=$(CLEAN_INSTALL) make install_microservice
	MICROSERVICE_PATH=orchestration PHYTON_VENV_VERSION=3.10.4 VENV_NAME=data-gcp-orchestration REQUIREMENTS_NAME=airflow/orchestration-requirements.txt RECREATE_VENV=$(CLEAN_INSTALL) make install_microservice
	MICROSERVICE_PATH=jobs/ml_jobs/linkage_candidates_items PHYTON_VENV_VERSION=3.10.4 VENV_NAME=data-gcp-linkage-candidates-items REQUIREMENTS_NAME=requirements.txt RECREATE_VENV=$(CLEAN_INSTALL) make install_microservice

clean_install:
	CLEAN_INSTALL=1 make install

initiate_env:
	cp -n .env.template .env.local

get_gcp_credentials:
ifeq (,$(wildcard ${HOME}/.config/gcloud/application_default_credentials.json))
	gcloud auth application-default login
endif

install_ubuntu_libs:
	sudo apt-get update -y
	sudo apt-get install -y make build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev gcc libpq-dev python3-dev

upload_dags_to_dev:
	$(eval COMPOSER_BUCKET_PATH=$(shell gcloud composer environments describe data-composer-dev --location europe-west1 --format='value(config.dagGcsPrefix)'))
	gsutil cp -r orchestration/dags/${path} $(COMPOSER_BUCKET_PATH)/${path}

prerequisites_on_debian_vm:
	curl https://pyenv.run | bash || echo "Pyenv already installed"
	curl -LsSf https://astral.sh/uv/install.sh | sh
	sudo rm /etc/apt/sources.list.d/kubernetes.list || echo "Kubernetes list already removed"
	sudo apt update  --fix-missing --allow-releaseinfo-change -y
	make install_ubuntu_libs
	sudo apt install -y libmariadb-dev
	echo 'export PYENV_ROOT="$$HOME/.pyenv"' >> ~/.profile
	echo 'export PATH="$$PYENV_ROOT/bin:$$PATH"' >> ~/.profile
	echo 'eval "$$(pyenv init --path)"' >> ~/.profile
	echo 'eval "$$(pyenv init -)"' >> ~/.profile
	echo 'eval "$$(pyenv virtualenv-init -)"' >> ~/.profile
	echo '. "$$HOME/.cargo/env"' >> ~/.profile
	bash

create_microservice:
	python automations/create_microservice.py --ms-name $(MS_NAME)
	git add .
	git commit -am "Add $(MS_NAME) microservice"
	make clean_install
