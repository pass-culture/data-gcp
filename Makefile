#######################################################################################
########                    Install and setup the project                      ########
#######################################################################################
SHELL := /bin/bash
BASE_PYTHON_VERSION := 3.10

export PERSONAL_DBT_USER :=

configure_personal_user:
	@read -p "Enter your username (e.g., adamasio for Alain Damasio): " username; \
	if ! echo "$$username" | grep -Eq "^[a-z]+$$"; then \
	    echo "Invalid username. Use lowercase letters only, no spaces, underscores, or numbers."; \
	    exit 1; \
	fi; \
	echo "Using username: $$username"; \
	if echo "$$SHELL" | grep -q "zsh"; then \
	    shell_file="$$(echo $$HOME/.zshrc)"; \
	elif echo "$$SHELL" | grep -q "bash"; then \
	    shell_file="$$(echo $$HOME/.bashrc)"; \
	else \
	    echo "Unable to detect active shell. Defaulting to ~/.bashrc."; \
	    shell_file="$$(echo $$HOME/.bashrc)"; \
	fi; \
	echo "Configuring shell file: $$shell_file"; \
	if [ ! -f "$$shell_file" ]; then \
	    echo "Shell file $$shell_file does not exist. Creating it now."; \
	    touch "$$shell_file"; \
	fi; \
	if grep -q "^export PERSONAL_DBT_USER=" "$$shell_file" 2>/dev/null; then \
	    sed -i.bak "/^export PERSONAL_DBT_USER=/d" "$$shell_file"; \
	    echo "export PERSONAL_DBT_USER=$$username" >> "$$shell_file"; \
	else \
	    echo "export PERSONAL_DBT_USER=$$username" >> "$$shell_file"; \
	fi; \
	echo "Environment variable PERSONAL_DBT_USER set to $$username in $$shell_file."; \
	echo "Please run 'source $$shell_file' or restart your terminal to apply changes."


_base_install:
	make configure_personal_user
	# Log in with GCP credentials if NO_GCP_INIT is not 1
	@if [ "$(NO_GCP_INIT)" != "1" ]; then \
		make _get_gcp_credentials; \
	fi

	curl -LsSf https://astral.sh/uv/install.sh | sh
	uv venv --python $(BASE_PHYTON_VERSION)
	source .venv/bin/activate && uv sync

_dbt_install:
	source .venv/bin/activate && uv pip install -r orchestration/airflow/orchestration-requirements.txt
	source .venv/bin/activate && cd orchestration/dags/data_gcp_dbt && dbt deps && dbt debug

install:
	make _base_install
	make _dbt_install

	echo "Please setup the current venv in your IDE to make it run permanently : https://www.notion.so/passcultureapp/Comment-installer-DBT-e25f7e24813c4d48baa43d641651caf8"


install_analytics:
	make install

install_engineering:
	make install


install_ubuntu_libs:
	sudo apt-get update -y
	sudo apt-get install -y make build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev gcc libpq-dev python3-dev libmariadb-dev clang

install_macos_libs:
	brew install mysql-client@8.4 pkg-config


#######################################################################################
########                                 Utils                                 ########
#######################################################################################

_init_dbt:
	cd orchestration/dags/data_gcp_dbt && dbt deps && dbt debug

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
	cd $(MS_BASE_PATH)/$(MS_NAME) && uv init --no-workspace -p 3.12 && uv add -r requirements.in && uv sync
	cd $(MS_BASE_PATH)/$(MS_NAME) && cat pyproject.toml.template >> pyproject.toml && rm requirements.in pyproject.toml.template
	git add . && git commit -am "auto: Add $(MS_NAME) as $(MS_TYPE) microservice"

create_microservice_ml:
	MS_TYPE=ml MS_NAME=$(MS_NAME) MS_BASE_PATH=jobs/ml_jobs make create_microservice

create_microservice_etl_external:
	MS_TYPE=etl_external MS_NAME=$(MS_NAME) MS_BASE_PATH=jobs/etl_jobs/external make create_microservice

create_microservice_etl_internal:
	MS_TYPE=etl_internal MS_NAME=$(MS_NAME) MS_BASE_PATH=jobs/etl_jobs/internal make create_microservice

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
	sqlfmt --check orchestration/dags --exclude "**/.venv/**" --exclude "**/venv/**"  --exclude "orchestration/dags/data_gcp_dbt/target/**" --exclude "**/orchestration/dags/dependencies/applicative_database/sql/raw/**" --exclude "**/orchestration/dags/data_gcp_dbt/snapshots/**"

docs_run:
	source .venv/bin/activate && mkdocs serve

precommit_docs_run:
	pre-commit run --all-files --config .pre-commit-ci-dbt-config.yaml
