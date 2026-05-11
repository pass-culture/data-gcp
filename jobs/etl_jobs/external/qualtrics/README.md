# Qualtrics ETL

Bidirectional sync with Qualtrics XM Directory.

DAGs:
- `import_qualtrics` — weekly (Monday 00:00 UTC)
- `export_qualtrics` — monthly (25th 06:00 UTC)


## Local run

```bash
uv sync

export PROJECT_NAME=
export ENV_SHORT_NAME=
export APPLICATIVE_EXTERNAL_CONNECTION_ID=

# Import opt-out users
uv run python main.py --task import_opt_out_users

# Import survey answers
uv run python main.py --task import_all_survey_answers

# Export beneficiaries to Qualtrics
uv run python main.py --task export_beneficiary --ds 2026-05-25 \
  --mailing-list-id xxx

# Export venues to Qualtrics
uv run python main.py --task export_venue --ds 2026-05-25 --mailing-list-id xxx
```
