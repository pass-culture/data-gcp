#######################################################################################
########                    Install and setup the project                      ########
#######################################################################################
SHELL := /bin/bash
BASE_PYTHON_VERSION := 3.10

export PERSONAL_DBT_USER :=


install:
	make _base_install
	make _dbt_install

	echo "Please setup the current venv in your IDE to make it run permanently : https://www.notion.so/passcultureapp/Comment-installer-DBT-e25f7e24813c4d48baa43d641651caf8"


install_analytics:
	echo "Please use `make install` instead"
	make install

install_engineering:
	echo "Please use `make install` instead"
	make install

#######################################################################################
########                                 Utils                                 ########
#######################################################################################



install_ubuntu_libs:
	sudo apt-get update -y
	sudo apt-get install -y make build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev gcc libpq-dev python3-dev libmariadb-dev clang

install_macos_libs:
	brew install mysql-client@8.4 pkg-config

_get_gcp_credentials:
ifeq (,$(wildcard ${HOME}/.config/gcloud/application_default_credentials.json))
	gcloud auth application-default login
endif


_configure_personal_user:
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


_dbt_install:
	source .venv/bin/activate && uv pip install -r orchestration/airflow/orchestration-requirements.txt
	source .venv/bin/activate && cd orchestration/dags/data_gcp_dbt && dbt deps && dbt debug

_base_install:
	make _configure_personal_user
	# Log in with GCP credentials if NO_GCP_INIT is not 1
	@if [ "$(NO_GCP_INIT)" != "1" ]; then \
		make _get_gcp_credentials; \
	fi

	curl -LsSf https://astral.sh/uv/install.sh | sh
	uv venv --python $(BASE_PHYTON_VERSION)
	uv sync

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

#######################################################################################
########                              Pre-commit                              ########
#######################################################################################

ruff_fix:
	uv run ruff check --fix
	uv run ruff format

ruff_check:
	uv run ruff check
	ruff format --check

sqlfluff_fix:
	cd orchestration/dags/data_gcp_dbt && uv run sqlfluff fix --dialect bigquery

sqlfluff_check:
	cd orchestration/dags/data_gcp_dbt && uv run sqlfluff lint echo $(./scripts/test.sh) --dialect bigquery

sqlfmt_fix:
	uv run sqlfmt orchestration/dags --exclude "**/.venv/**" --exclude "**/venv/**" --exclude "orchestration/dags/data_gcp_dbt/target/**" --exclude "**/orchestration/dags/dependencies/applicative_database/sql/raw/**"

sqlfmt_check:
	uv run sqlfmt --check orchestration/dags --exclude "**/.venv/**" --exclude "**/venv/**"  --exclude "orchestration/dags/data_gcp_dbt/target/**" --exclude "**/orchestration/dags/dependencies/applicative_database/sql/raw/**" --exclude "**/orchestration/dags/data_gcp_dbt/snapshots/**"

precommit_docs_run:
	pre-commit run --all-files --config .pre-commit-ci-dbt-config.yaml

#######################################################################################
########                              Documentation                            ########
#######################################################################################

docs_run:
	uv run mkdocs serve
