# Data stack

The data environment at pass Culture is built around a structured pipeline that takes data from collection to real-world use, through processing and distribution.

## 🔄 Data Collection

Internal sources: from our applications, users, and backend systems.

External sources: sourced from public providers like data.gouv.fr and INSEE.

## 🧱 Data Processing & Transformation

At the heart of this setup is the Data Engineering team, responsible for processing (ETL) and orchestrating data flows using:

Google Cloud for infrastructure,

Airflow for orchestration,

dbt for data transformation and modeling.

## 🧭 Data Delivery

Once processed, the data is made available to several downstream services:

Data Analytics: for analysis and reporting (via BigQuery),

Backend: to provide fast access to aggregated statistics (via ClickHouse),

Data Science: to train machine learning models and expose them through APIs (using TensorFlow and Python).

## 🧑‍💼 Final Use Cases

The refined data powers multiple concrete use cases:

Internal dashboards (via Metabase),

Partner-facing statistics (via our pro interface),

Personalized recommendations for users in the app.
