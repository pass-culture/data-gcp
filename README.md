# Data GCP 🚀

[![Python Version](https://img.shields.io/badge/python-3.12-blue)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MPL--2.0-orange)](LICENSE)
[![Documentation](https://img.shields.io/badge/docs-pass--culture.github.io-blue)](https://pass-culture.github.io/data-gcp/)

> Data Engineering Platform for Pass Culture on Google Cloud Platform (GCP)

## 📚 Overview

This repository contains the core components of our data platform:

- **Airflow DAGs** for workflow orchestration
- **DBT models** for data transformation
- **ML models** for machine learning services
- **ETL jobs** for data processing

## 📖 Documentation

- [Project Overview](https://pass-culture.github.io/data-gcp/) - Main data models, glossary, and technical references
- [Orchestration Guide](/orchestration/README.md) - Airflow DAGs documentation
- [CI/CD Documentation](.github/workflows/README.md) - Deployment and pipeline details

## 🏗️ Architecture

```
+-- orchestration
| +-- dags
|    +-- dependencies
|    +-- jobs
|    +-- data_gcp_dbt
+-- jobs
| +-- etl_jobs
|   +-- external
|     +-- ...
|   +-- internal
|     +-- ...
| +-- ml_jobs
|   +-- ...
```

## 🚀 Getting Started

### Prerequisites

- [Google Cloud CLI](https://cloud.google.com/sdk/docs/install)
- Access to our GCP service accounts
- Make installed
  - Linux: `sudo apt install make`
  - macOS: `brew install make`
- ggshield installed
   - Linux: `sudo apt install ggshield`
   - Mac: `brew install ggshield`
- ggshield authenticated `ggshield auth login` use github auth with your work account

### Installation

1. **Clone the repository**
   ```bash
   git clone git@github.com:pass-culture/data-gcp.git
   cd data-gcp
   ```

2. **Install the project**
   ```bash
   make install
   ```
   > This installation includes all necessary requirements for the `orchestration` part in a single virtual environment and sets up pre-commit hooks for code quality.

### Troubleshooting

#### Ubuntu
```bash
make install_ubuntu_libs
```

#### macOS
```bash
make install_macos_libs
```
Add to your `~/.zshrc`:
```bash
export MYSQLCLIENT_LDFLAGS="-L/opt/homebrew/opt/mysql-client/lib -lmysqlclient -rpath /usr/local/mysql/lib"
export MYSQLCLIENT_CFLAGS="-I/opt/homebrew/opt/mysql-client/include -I/opt/homebrew/opt/mysql-client/include/mysql"
```


## 🛠️ Development

### Creating New Microservices

#### ML Microservice
```bash
MS_NAME=my_microservice make create_microservice_ml
```

#### ETL Microservice (Internal)
```bash
MS_NAME=my_microservice make create_microservice_etl_internal
```

#### ETL Microservice (External)
```bash
MS_NAME=my_microservice make create_microservice_etl_external
```

### Install specific dependencies

```bash
uv sync --group <airflow|dbt|dev|docs>
```

### Run pre-commit hooks

```bash
make ruff_fix / ruff_check / sqlfluff_fix / sqlfluff_check / sqlfmt_fix / sqlfmt_check
```



## 🔄 CI/CD

Our CI/CD pipelines are managed through GitHub Actions. See the [workflows documentation](.github/workflows/README.md) for details.

## 🤝 Contributing

1. Create a new branch for your feature
2. Make your changes
3. Submit a pull request

## 📝 License

This project is licensed under the Mozilla Public License Version 2.0 - see the [LICENSE](LICENSE) file for details.
