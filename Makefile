#######################################################################################
########                    Install and setup the project                      ########
#######################################################################################
SHELL := /bin/bash

install:
	# Log in with GCP credentials if NO_GCP_INIT is not 1
	@if [ "$(NO_GCP_INIT)" != "1" ]; then \
		make _get_gcp_credentials; \
	fi
	make _initiate_env
	pyenv install 3.10.4 -s
	curl -LsSf https://astral.sh/uv/install.sh | sh
	MICROSERVICE_PATH=. PHYTON_VENV_VERSION=3.10.4 VENV_NAME=data-gcp REQUIREMENTS_NAME=requirements.txt RECREATE_VENV=$(CLEAN_INSTALL) make _install_microservice
	pre-commit install

install_engineering:
	make install
	MICROSERVICE_PATH=orchestration PHYTON_VENV_VERSION=3.10.4 VENV_NAME=data-gcp-orchestration REQUIREMENTS_NAME=airflow/orchestration-requirements.txt RECREATE_VENV=$(CLEAN_INSTALL) make _install_microservice
	MICROSERVICE_PATH=orchestration/dags PHYTON_VENV_VERSION=3.10.4 VENV_NAME=data-gcp-dbt REQUIREMENTS_NAME=dbt-requirements.txt RECREATE_VENV=$(CLEAN_INSTALL) make _install_microservice
	make _init_dbt

install_science:
	make install_engineering
	MICROSERVICE_PATH=jobs/ml_jobs/algo_training PHYTON_VENV_VERSION=3.10.4 VENV_NAME=data-gcp-algo-training REQUIREMENTS_NAME=requirements.txt RECREATE_VENV=$(CLEAN_INSTALL) make _install_microservice


#######################################################################################
########                          Simplified Install                           ########
#######################################################################################
install_simplified:
	# Log in with GCP credentials if NO_GCP_INIT is not 1
	@if [ "$(NO_GCP_INIT)" != "1" ]; then \
		make _get_gcp_credentials; \
	fi
	make _initiate_env
	curl -LsSf https://astral.sh/uv/install.sh | sh
	uv python install 3.10
	uv venv --python 3.10
	uv pip install -r requirements.txt
	pre-commit install

install_analytics:
	make install_simplified
	uv pip install -r orchestration/dags/dbt-requirements.txt
	make _init_dbt
	echo "Please setup the current venv in your IDE to make it run permanently : https://www.notion.so/passcultureapp/Comment-installer-DBT-e25f7e24813c4d48baa43d641651caf8"



#######################################################################################
########                                 Utils                                 ########
#######################################################################################

_init_dbt:
	cd orchestration/dags/data_gcp_dbt && dbt deps
	cd orchestration/dags/data_gcp_dbt && dbt debug

_install_microservice:
	# deactivate the current venv if it exists
	source deactivate || echo "No venv activated"

	# Recreate the venv if RECREATE_VENV is set to 1
	@if [ "$(RECREATE_VENV)" = "1" ]; then \
		eval "$$(pyenv init -)" && cd $(MICROSERVICE_PATH) && rm -f .python-version && pyenv virtualenv-delete -f $(VENV_NAME); \
	fi

	# Create the venv if it does not exist
	@if [ -z "$$(pyenv virtualenvs | grep '\s$(VENV_NAME)\s')" ]; then \
		eval "$$(pyenv init -)" && cd $(MICROSERVICE_PATH) && pyenv virtualenv $(PHYTON_VENV_VERSION) $(VENV_NAME); \
	fi

	# Set the venv to the current directory
	eval "$$(pyenv init -)" && cd $(MICROSERVICE_PATH) && pyenv local $(VENV_NAME)

	# Install the requirements
	@eval "$$(pyenv init -)" && cd $(MICROSERVICE_PATH) && pyenv activate $(VENV_NAME) && uv pip sync $(REQUIREMENTS_NAME)


_initiate_env:
	cp -n .env.template .env.local


_get_gcp_credentials:
ifeq (,$(wildcard ${HOME}/.config/gcloud/application_default_credentials.json))
	gcloud auth application-default login
endif

_install_ubuntu_libs:
	sudo apt-get update -y
	sudo apt-get install -y make build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev gcc libpq-dev python3-dev

prerequisites_on_debian_vm:
	curl https://pyenv.run | bash || echo "Pyenv already installed"
	curl -LsSf https://astral.sh/uv/install.sh | sh
	sudo rm /etc/apt/sources.list.d/kubernetes.list || echo "Kubernetes list already removed"
	sudo apt update  --fix-missing --allow-releaseinfo-change -y
	make _install_ubuntu_libs
	sudo apt install -y libmariadb-dev
	echo 'export PYENV_ROOT="$$HOME/.pyenv"' >> ~/.profile
	echo 'export PATH="$$PYENV_ROOT/bin:$$PATH"' >> ~/.profile
	echo 'eval "$$(pyenv init --path)"' >> ~/.profile
	echo 'eval "$$(pyenv init -)"' >> ~/.profile
	echo 'eval "$$(pyenv virtualenv-init -)"' >> ~/.profile
	echo '. "$$HOME/.cargo/env"' >> ~/.profile
	bash



#######################################################################################
########                              Automations                              ########
#######################################################################################

create_microservice:
	python automations/create_microservice.py --ms-name $(MS_NAME) --ms-type $(MS_TYPE)
	git add .
	git commit -am "Add $(MS_NAME) as $(MS_TYPE) microservice"

create_microservice_ml:
	MS_TYPE=ml MS_NAME=$(MS_NAME) make create_microservice
	MICROSERVICE_PATH=jobs/ml_jobs/$(MS_NAME) PHYTON_VENV_VERSION=3.10.4 VENV_NAME=data-gcp-$(MS_NAME) REQUIREMENTS_NAME=requirements.txt make _install_microservice

create_microservice_etl_external:
	MS_TYPE=etl_external MS_NAME=$(MS_NAME) make create_microservice
	MICROSERVICE_PATH=jobs/etl_jobs/external/$(MS_NAME) PHYTON_VENV_VERSION=3.10.4 VENV_NAME=data-gcp-$(MS_NAME) REQUIREMENTS_NAME=requirements.txt make _install_microservice

create_microservice_etl_internal:
	MS_TYPE=etl_internal MS_NAME=$(MS_NAME) make create_microservice
	MICROSERVICE_PATH=jobs/etl_jobs/internal/$(MS_NAME) PHYTON_VENV_VERSION=3.10.4 VENV_NAME=data-gcp-$(MS_NAME) REQUIREMENTS_NAME=requirements.txt make _install_microservice

ruff_fix:
	ruff check --fix
	ruff format

ruff_check:
	ruff check
	ruff format --check
