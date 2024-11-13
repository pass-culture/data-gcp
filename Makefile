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
	uv || echo -e "\n \n Please restart you shell in order to allow uv commands \n \n"
	uv venv --python 3.10
	source .venv/bin/activate && uv pip install -r requirements.txt && pre-commit install

install_analytics:
	make install_simplified
	source .venv/bin/activate && uv pip install -r orchestration/dags/data_gcp_dbt/dbt-requirements.txt
	source .venv/bin/activate && make _init_dbt
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
	@if [ ! -f .env.local ]; then cp .env.template .env.local; fi


_get_gcp_credentials:
ifeq (,$(wildcard ${HOME}/.config/gcloud/application_default_credentials.json))
	gcloud auth application-default login
endif

_install_ubuntu_libs:
	sudo apt-get update -y
	sudo apt-get install -y make build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev gcc libpq-dev python3-dev

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

sqlfluff_fix:
	cd orchestration/dags/data_gcp_dbt && sqlfluff fix --dialect bigquery

sqlfluff_format:
	cd orchestration/dags/data_gcp_dbt && sqlfluff format --dialect bigquery

sqlfluff_check:
	cd orchestration/dags/data_gcp_dbt && sqlfluff lint echo $(./scripts/test.sh) --dialect bigquery

precommit_install:
	@eval "$$(pyenv init -)" && pyenv activate data-gcp && pre-commit install

sqlfmt_fix:
	sqlfmt orchestration/dags --exclude "**/.venv/**" --exclude "**/venv/**" --exclude "orchestration/dags/data_gcp_dbt/target/**" --exclude "**/orchestration/dags/dependencies/applicative_database/sql/raw/**"

sqlfmt_check:
	sqlfmt --check orchestration/dags --exclude "**/.venv/**" --exclude "**/venv/**"  --exclude "orchestration/dags/data_gcp_dbt/target/**" --exclude "**/orchestration/dags/dependencies/applicative_database/sql/raw/**"

docs_run:
	source .venv/bin/activate && mkdocs serve

precommit_docs_run:
	pre-commit run --all-files --config .pre-commit-ci-dbt-config.yaml
