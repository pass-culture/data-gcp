# Project Structure and Data Flow

This document explains the organization of the `jobs/` and `orchestration/` directories and how they interact in the data lifecycle of the Pass Culture Data Platform.

## 📁 Directory Overview

### 1. `jobs/`
The `jobs/` directory contains the core logic for data processing, including ETL (Extract, Transform, Load) and Machine Learning tasks. These are typically standalone Python packages or scripts that will be run on GCE VMs. These are on general managed by Airflow DAGs in `orchestrations/`.

*   **`etl_jobs/`**: Responsibility for moving data between systems.
    *   **`external/`**: Scripts that fetch/push data from/to external APIs (e.g., AppsFlyer, Brevo, Qualtrics).
    *   **`connectors/`**: Attempt for another way to fetch data from external APIs (`external/` is the old way).
    *   **`internal/`**: Scripts that interact with internal API (to fetch/push data) to access Pass Culture services or GCP resources.
*   **`ml_jobs/`**: Responsibility for machine learning workflows.
    *   Includes subdirectories for specific ML tasks like `algo_training/`, `embeddings/`, `recommendation/`, and `ranking_endpoint/`.
*   **`playground_vm/`**: Environment for testing and prototyping on virtual machines.

### 2. `orchestration/`
The `orchestration/` directory has the same structure of an Airflow project. It manages the scheduling, coordination, and execution of the jobs defined above, primarily using Apache Airflow and dbt.

*   **`dags/`**: The heart of Airflow orchestration. It is synchronized through the GitHub Workflows.
    *   **`jobs/`**: Contains the DAG (Directed Acyclic Graph) definition files. These files define the workflow and schedule (e.g., `crons.py`). Some the operators spin a GCE VM, install code that is in `jobs/etl_jobs`, shutsdown the GCE VM and run SQL that is in the `dependencies/`.
    *   **`dependencies/`**: Contains SQL of some external system imports (or other) and python files containig configurations for these SQL statements. This is where the actual calls to `jobs/` are often wrapped.
    *   **`common/`**: Shared Airflow operators, hooks, and utilities used across multiple DAGs.
    *   **`data_gcp_dbt/`**: The dbt (data build tool) project. It contains SQL models for transforming raw data into analytics-ready tables within BigQuery.
*   **`airflow/`**: Airflow configuration files.
*   **`k8s-airflow/`**: Kubernetes configurations for deploying Airflow.
*   **`plugins/`**: Custom Airflow plugins.

---

## 🔄 Data Flow

The flow of data through the platform typically follows these stages:

### 1. Extraction & Ingestion (Raw Data Loading)
Data is pulled from various sources (production databases, external and internal APIs, flat files in GCS buckets) using either:
* **ETL Jobs** located in `jobs/etl_jobs/`
* Federated SQL queries present in `dags/dependencies/`
* Simple calls to operators to access GCS resources.

These jobs are triggered by **Airflow DAGs** (`orchestration/dags/jobs/`).

The ingested data is loaded into **BigQuery** as "raw" tables. This stage ensures that a copy of the source data is available in the data warehouse.

### 2. Transformation (dbt)
Once the raw data is in BigQuery, **dbt models** (`orchestration/dags/data_gcp_dbt/`) are executed by Airflow. dbt performs:
*   **Cleaning**: Renaming columns, casting types, and basic filtering.
*   **Modeling**: Joining tables, applying business logic, and creating aggregated views.
*   **Snapshoting**: Tracking changes in data over time.

### 3. Reverse Transformation (reverse ETL) and push to external systems
The transformed data is loaded into **BigQuery** as "analytics" tables.

It is done throuhd the same setup as "1. Extraction & Ingestion".

### 4. Advanced Processing (ML)
Transformed data is often used as input for **ML Jobs** (`jobs/ml_jobs/`). These jobs perform tasks such as:
*   Generating embeddings for recommendations.
*   Training models for offer ranking.
*   Calculating user clusters.
The results are usually written back to BigQuery or exported to other services.
