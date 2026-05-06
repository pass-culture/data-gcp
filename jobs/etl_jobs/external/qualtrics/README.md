# Qualtrics ETL

Bidirectional sync with Qualtrics XM Directory.

DAGs:
- `import_qualtrics` — weekly (Monday 00:00 UTC)
- `export_qualtrics_data` — monthly (25th 06:00 UTC)

## Secrets to create in Secret Manager

| Secret | How to get value |
|---|---|
| `qualtrics_token_{env}` | Qualtrics → Account Settings → Qualtrics IDs → API Token |
| `qualtrics_data_center_{env}` | Same page → Datacenter ID (e.g. `fra1`) |
| `qualtrics_directory_id_{env}` | Same page → Directory ID (`POOL_xxx`) |
| `qualtrics_ir_jeunes_automation_id_{env}` | **Update value** → mailing list ID (`CG_xxx`), see below |
| `qualtrics_ir_ac_automation_id_{env}` | **Update value** → mailing list ID (`CG_xxx`), see below |
| `applicative_external_connection_id_{env}` | Airflow env var `APPLICATIVE_EXTERNAL_CONNECTION_ID` |

### Get mailing list IDs

```bash
curl -s "https://<DATA_CENTER>.qualtrics.com/API/v3/directories/<DIRECTORY_ID>/mailinglists" \
  -H "X-API-TOKEN: <TOKEN>" | python3 -m json.tool
```

Find `"[Import] Indicateur de réputation Jeunes"` → copy its `id` (`ML_xxx`).
Find `"[Import] Indicateur de réputation Acteur Culturel"` → copy its `id`.

## Local run

```bash
uv sync

export PROJECT_NAME=passculture-data-prod
export ENV_SHORT_NAME=prod
export APPLICATIVE_EXTERNAL_CONNECTION_ID="passculture-data-prod.europe-west1.your-conn-id"

# Import opt-out users
uv run python main.py --task import_opt_out_users

# Import survey answers
uv run python main.py --task import_all_survey_answers

# Export beneficiaries to Qualtrics
uv run python main.py --task export_beneficiary --ds 2026-05-25 \
  --mailing-list-id CG_xxx

# Export venues to Qualtrics
uv run python main.py --task export_venue --ds 2026-05-25 --mailing-list-id ML_xxx
```
