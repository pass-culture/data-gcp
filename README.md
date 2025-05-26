# Data GCP

Repository for the data team on Google Cloud Platform (GCP).

This repository contains Airflow DAGs, DBT models, ML models, and necessary scripts for job orchestration.

- DAGs are located in `orchestration/dags/`
- Scripts called by DAGs should be placed in `jobs/`, divided into two categories:
  - ETL jobs: for data extraction, transformation, and loading
  - ML jobs: for machine learning microservices

The project overview is available in the github site [here](https://pass-culture.github.io/data-gcp/) with main data models, glossary,documentation and technical references.

## Repository Structure

```
+-- orchestration : Airflow DAGs (Cloud Composer)
| +-- airflow
| +-- dags
|    +-- dependencies
|    +-- data_gcp_dbt
+-- jobs
| +-- etl_jobs
|   +-- external
|     +-- ...
|
|   +-- internal
|     +-- ...
|
| +-- ml_jobs
|   +-- ...
```

## Installation

#### 0. Prerequisites

- Access to GCP service accounts and [Google Cloud CLI](https://cloud.google.com/sdk/docs/install)
- Make installed
  - Linux: `sudo apt install make`
  - macOS: `brew install make`
- Clone the project

  ```bash
  git clone git@github.com:pass-culture/data-gcp.git
  cd data-gcp
  ```

#### 1. Project Installation

Install the project:

```bash
make install
```

> This installation is simplified to include all necessary requirements for the `orchestration` part in a single virtual environment. It also installs **pre-commit** hooks for the project, ensuring code quality from the first commit.

#### 2. Installing a Specific Microservice Environment

To install a specific microservice environment:

```bash
MICROSERVICE_PATH=jobs/ml_jobs/retrieval_vector
cd $(MICROSERVICE_PATH) && uv sync
```

> This command creates a dedicated virtual environment for the microservice.

#### 3. Troubleshooting

- Install required libraries (not necessary on macOS)
  - [Ubuntu]:
    - `make install_ubuntu_libs`
  - [macOS]:
    - `make install_macos_libs`
    - Add the following lines to your `~/.zshrc`:

      ```bash
      export MYSQLCLIENT_LDFLAGS="-L/opt/homebrew/opt/mysql-client/lib -lmysqlclient -rpath /usr/local/mysql/lib"
      export MYSQLCLIENT_CFLAGS="-I/opt/homebrew/opt/mysql-client/include -I/opt/homebrew/opt/mysql-client/include/mysql"
      ```

## Orchestration

Airflow DAGs orchestrating various DA/DE/DS jobs are detailed in [the orchestration folder's README.md](/orchestration/README.md)

DAGs are automatically deployed upon merging to master/production (see [continuous deployment documentation](.github/workflows/README.md))

## CI/CD

Pipelines are detailed in the [GitHub Actions README](.github/workflows/README.md)

## Automation

### ML Jobs

To create a new ML or ETL microservice, you can use the following commands:

- `MS_NAME=my_microservice make create_microservice_ml`: Creates an ML microservice in the `jobs/ml_jobs` directory
- `MS_NAME=my_microservice make create_microservice_etl_internal`: Creates an ETL microservice in the `jobs/etl_jobs/internal` directory
- `MS_NAME=my_microservice make create_microservice_etl_external`: Creates an ETL microservice in the `jobs/etl_jobs/external` directory

where `my_microservice` is the name of your microservice. Example:

```bash
MS_NAME=algo_llm make create_microservice_ml
```

This will:

1. Create an `algo_llm` directory in `jobs/ml_jobs` with all necessary files for the microservice
2. Commit the changes
3. Launch the installation of the new microservice
