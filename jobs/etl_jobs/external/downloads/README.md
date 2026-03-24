# Downloads ETL

Fetches app download stats from Apple App Store Connect and Google Play Store, then loads them into BigQuery.

## Workflow

1. **Apple** — Calls the App Store Connect API day by day over the previous month, concatenates results, deletes existing rows for that date range in `apple_download_stats`, and appends the new data.
2. **Google** — Downloads monthly install CSVs from a GCS bucket (last month + current month if available), deletes existing rows in `google_download_stats`, and appends.

## Usage

```bash
uv run main.py --provider google --execution-date 2024-03-01
uv run main.py --provider apple --execution-date 2024-03-01
# --execution-date defaults to today
```

## Environment variables

| Variable | Description |
|---|---|
| `PROJECT_NAME` | GCP project ID (default: `passculture-data-ehp`) |
| `ENV_SHORT_NAME` | Environment suffix used for dataset and secret names (default: `dev`) |

## Secrets (via GCP Secret Manager)

| Secret | Used by |
|---|---|
| `api-apple-{ENV_SHORT_NAME}` | Apple JWT signing key (ES256 private key) |
| `downloads_bucket_name_{ENV_SHORT_NAME}` | GCS bucket containing Google Play reports |
