# Adage ETL Pipeline

Imports cultural partner data and student engagement statistics from the ADAGE API (Aide au Développement et à l'Accompagnement des projets d'éducation artistique et culturelle) into BigQuery.

## Overview

The pipeline runs two sequential jobs:

1. **Partner import** — fetches cultural venues modified in the last 30 days and upserts them into a historical BigQuery table.
2. **Stats import** — fetches student involvement statistics for the current educational year(s) and writes them to a partitioned BigQuery table.

## Pipeline steps

```
main.py
  └── import_adage(since_date)
        ├── CREATE TABLE IF NOT EXISTS analytics_{env}.adage_historical
        ├── GET /partenaire-culturel?dateModificationMin=...
        ├── APPEND rows → raw_{env}.adage
        └── MERGE raw_{env}.adage → analytics_{env}.adage_historical

  └── get_adage_stats()
        ├── Determine active educational year IDs (based on current date)
        ├── For each year ID:
        │     └── GET /stats-pass-culture/{id}
        │           → flatten departements / academies / regions metrics
        └── WRITE_TRUNCATE → clean_{env}.adage_involved_student (date-partitioned)
```

## BigQuery tables

| Table | Dataset | Write mode | Description |
|---|---|---|---|
| `adage` | `raw_{env}` | APPEND | Raw API snapshots, one row per partner per run |
| `adage_historical` | `analytics_{env}` | MERGE (upsert by `id`) | Latest state of each cultural partner |
| `adage_involved_student` | `clean_{env}` | WRITE_TRUNCATE (daily partition) | Student stats per educational year, geographic level, and school level |

## Environment variables

| Variable | Description |
|---|---|
| `PROJECT_NAME` | GCP project ID (default: `passculture-data-prod`) |
| `ENV_SHORT_NAME` | Deployment environment: `dev`, `stg`, or `prod` (default: `prod`) |

## Secret Manager keys

| Secret | Environment |
|---|---|
| `adage_import_api_key` | `dev` |
| `adage_import_api_key_stg` | `stg` |
| `adage_import_api_key_prod` | `prod` |

The API key is passed as the `X-omogen-api-key` header on every request.

## API endpoints

| Environment | Base URL |
|---|---|
| `prod` | `https://omogen-api-pr.phm.education.gouv.fr/adage-api/v1` |
| `stg` | `https://omogen-api-tst-pr.phm.education.gouv.fr/adage-api-staging/v1` |
| `dev` | `https://omogen-api-tst-pr.phm.education.gouv.fr/adage-api-test/v1` |

## Educational year mapping

Stats are collected for the educational year(s) whose date range includes today:

| ADAGE ID | School year |
|---|---|
| 7 | 2021–2022 |
| 8 | 2022–2023 |
| 9 | 2023–2024 |
| 10 | 2024–2025 |
| 11 | 2025–2026 |

## Running locally

```bash
uv sync
PROJECT_NAME=<gcp-project> ENV_SHORT_NAME=dev python main.py
```

## Dependencies

Managed with `uv` (Python 3.12+). Key dependencies: `google-cloud-bigquery`, `google-cloud-secret-manager`, `pandas`, `pandas-gbq`, `requests`.
