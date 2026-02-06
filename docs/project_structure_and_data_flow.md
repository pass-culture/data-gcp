# Project Structure and Data Flow

This document explains the organization of the `jobs/` and `orchestration/` directories and how they interact in the data lifecycle of the Pass Culture Data Platform.

## 📁 Directory Overview

### 1. `jobs/`
The `jobs/` directory contains the core logic for data processing, including ETL (Extract, Transform, Load) and Machine Learning tasks. These are typically standalone Python packages or scripts.

*   **`etl_jobs/`**: Responsibility for moving data between systems.
    *   **`external/`**: Scripts that fetch data from external APIs (e.g., AppsFlyer, Brevo, Qualtrics).
    *   **`internal/`**: Scripts that interact with internal Pass Culture services or GCP resources.
    *   **`connectors/`**: Reusable components for connecting to various data sources.
*   **`ml_jobs/`**: Responsibility for machine learning workflows.
    *   Includes subdirectories for specific ML tasks like `algo_training/`, `embeddings/`, `recommendation/`, and `ranking_endpoint/`.
*   **`playground_vm/`**: Environment for testing and prototyping on virtual machines.

### 2. `orchestration/`
The `orchestration/` directory manages the scheduling, coordination, and execution of the jobs defined above, primarily using Apache Airflow and dbt.

*   **`dags/`**: The heart of Airflow orchestration.
    *   **`jobs/`**: Contains the DAG (Directed Acyclic Graph) definition files. These files define the workflow and schedule (e.g., `crons.py`).
    *   **`dependencies/`**: Contains the business logic and helper functions used by the DAGs. This is where the actual calls to `jobs/` are often wrapped.
    *   **`common/`**: Shared Airflow operators, hooks, and utilities used across multiple DAGs.
    *   **`data_gcp_dbt/`**: The dbt (data build tool) project. It contains SQL models for transforming raw data into analytics-ready tables within BigQuery.
*   **`airflow/`**: Airflow configuration files.
*   **`k8s-airflow/`**: Kubernetes configurations for deploying Airflow.
*   **`plugins/`**: Custom Airflow plugins.

---

## 🔄 Data Flow

The flow of data through the platform typically follows these stages:

### 1. Extraction & Ingestion
Data is pulled from various sources (production databases, external APIs, flat files) using **ETL Jobs** located in `jobs/etl_jobs/`. These jobs are triggered by **Airflow DAGs** (`orchestration/dags/jobs/`).

### 2. Raw Data Loading
The ingested data is loaded into **BigQuery** as "raw" tables. This stage ensures that a copy of the source data is available in the data warehouse.

### 3. Transformation (dbt)
Once the raw data is in BigQuery, **dbt models** (`orchestration/dags/data_gcp_dbt/`) are executed by Airflow. dbt performs:
*   **Cleaning**: Renaming columns, casting types, and basic filtering.
*   **Modeling**: Joining tables, applying business logic, and creating aggregated views.
*   **Snapshoting**: Tracking changes in data over time.

### 4. Advanced Processing (ML)
Transformed data is often used as input for **ML Jobs** (`jobs/ml_jobs/`). These jobs perform tasks such as:
*   Generating embeddings for recommendations.
*   Training models for offer ranking.
*   Calculating user clusters.
The results are usually written back to BigQuery or exported to other services.

### 5. Orchestration & Monitoring
**Airflow** (`orchestration/`) acts as the conductor for the entire process, ensuring that tasks run in the correct order, handling retries, and providing visibility into the status of the data pipeline.
