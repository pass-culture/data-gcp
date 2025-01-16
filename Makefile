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
	curl -LsSf https://astral.sh/uv/install.sh | sh
	MICROSERVICE_PATH=. PHYTON_VERSION=3.10 REQUIREMENTS_NAME=requirements.txt make _install_microservice
	source .venv/bin/activate && pre-commit install

install_engineering:
	make install
	MICROSERVICE_PATH=orchestration PHYTON_VERSION=3.10 REQUIREMENTS_NAME=airflow/orchestration-requirements.txt make _install_microservice
	MICROSERVICE_PATH=orchestration/dags/data_gcp_dbt PHYTON_VERSION=3.10 REQUIREMENTS_NAME=dbt-requirements.txt make _install_microservice
	make _init_dbt

install_science:
	make install_engineering
	MICROSERVICE_PATH=jobs/ml_jobs/algo_training PHYTON_VERSION=3.10 REQUIREMENTS_NAME=requirements.txt make _install_microservice

install_analytics:
	make install
	source .venv/bin/activate && uv pip install -r orchestration/dags/data_gcp_dbt/dbt-requirements.txt
	source .venv/bin/activate && make _init_dbt
	echo "Please setup the current venv in your IDE to make it run permanently : https://www.notion.so/passcultureapp/Comment-installer-DBT-e25f7e24813c4d48baa43d641651caf8"

install_ubuntu_libs:
	sudo apt-get update -y
	sudo apt-get install -y make build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev gcc libpq-dev python3-dev libmariadb-dev clang

install_macos_libs:
	brew install mysql-client@8.4 pkg-config


#######################################################################################
########                                 Utils                                 ########
#######################################################################################

_install_microservice:
	# Recreate the venv
	cd $(MICROSERVICE_PATH) && uv venv --python $(PHYTON_VERSION)

	# Install the requirements
	cd $(MICROSERVICE_PATH) && source .venv/bin/activate && uv pip sync $(REQUIREMENTS_NAME)

_init_dbt:
	cd orchestration/dags/data_gcp_dbt && (source .venv/bin/activate || echo "\n Warning: No .venv found in data_gcp_dbt. Ignore this if you are in the analytics team. \n")  && dbt deps && dbt debug

_initiate_env:
	@if [ ! -f .env.local ]; then cp .env.template .env.local; fi

_get_gcp_credentials:
ifeq (,$(wildcard ${HOME}/.config/gcloud/application_default_credentials.json))
	gcloud auth application-default login
endif


delete_python_version_files:
	@echo "Finding all .python-version files..."
	@files=$$(find . -name ".python-version"); \
	if [ -z "$$files" ]; then \
		echo "No .python-version files found."; \
	else \
		echo "Found the following .python-version files:"; \
		echo "$$files"; \
		read -p "Do you want to delete these files? (yes/no): " confirm; \
		if [ "$$confirm" = "yes" ]; then \
			echo "Deleting .python-version files..."; \
			find . -name ".python-version" -exec rm -f {} +; \
			echo "Files deleted."; \
		else \
			echo "Deletion aborted."; \
		fi \
	fi

#######################################################################################
########                              Automations                              ########
#######################################################################################

create_microservice:
	python automations/create_microservice.py --ms-name $(MS_NAME) --ms-type $(MS_TYPE)
	git add .
	git commit -am "Add $(MS_NAME) as $(MS_TYPE) microservice"

create_microservice_ml:
	MS_TYPE=ml MS_NAME=$(MS_NAME) make create_microservice
	MICROSERVICE_PATH=jobs/ml_jobs/$(MS_NAME) PHYTON_VERSION=3.10 VENV_NAME=data-gcp-$(MS_NAME) REQUIREMENTS_NAME=requirements.txt make _install_microservice

create_microservice_etl_external:
	MS_TYPE=etl_external MS_NAME=$(MS_NAME) make create_microservice
	MICROSERVICE_PATH=jobs/etl_jobs/external/$(MS_NAME) PHYTON_VERSION=3.10 VENV_NAME=data-gcp-$(MS_NAME) REQUIREMENTS_NAME=requirements.txt make _install_microservice

create_microservice_etl_internal:
	MS_TYPE=etl_internal MS_NAME=$(MS_NAME) make create_microservice
	MICROSERVICE_PATH=jobs/etl_jobs/internal/$(MS_NAME) PHYTON_VERSION=3.10 VENV_NAME=data-gcp-$(MS_NAME) REQUIREMENTS_NAME=requirements.txt make _install_microservice

ruff_fix:
	ruff check --fix
	ruff format

ruff_check:
	ruff check
	ruff format --check

sqlfluff_fix:
	cd orchestration/dags/data_gcp_dbt && sqlfluff fix --dialect bigquery

sqlfluff_check:
	cd orchestration/dags/data_gcp_dbt && sqlfluff lint echo $(./scripts/test.sh) --dialect bigquery

sqlfmt_fix:
	sqlfmt orchestration/dags --exclude "**/.venv/**" --exclude "**/venv/**" --exclude "orchestration/dags/data_gcp_dbt/target/**" --exclude "**/orchestration/dags/dependencies/applicative_database/sql/raw/**"

sqlfmt_check:
	sqlfmt --check orchestration/dags --exclude "**/.venv/**" --exclude "**/venv/**"  --exclude "orchestration/dags/data_gcp_dbt/target/**" --exclude "**/orchestration/dags/dependencies/applicative_database/sql/raw/**"

docs_run:
	source .venv/bin/activate && mkdocs serve

precommit_docs_run:
	pre-commit run --all-files --config .pre-commit-ci-dbt-config.yaml
