# Metabase Migration Tool

Outil de migration automatisee des cartes Metabase lors de renommages de tables/colonnes BigQuery.

## Architecture

```
main.py              # CLI Typer — commande `migrate` avec --dry-run
config.py            # Configuration (env vars + GCP Secret Manager)

api/
  client.py          # Client HTTP Metabase (auth, retry, types)
  models.py          # Modeles Pydantic v2 (Card, Table, Field, etc.)

migration/
  card.py            # Migration native SQL + query builder
  field_walker.py    # Tree walker MBQL pour remplacement safe des field IDs

discovery/
  bigquery.py        # Decouverte des cartes impactees via BigQuery

data/
  mappings.json      # Correspondances ancien -> nouveau nom de colonne

tests/               # 42 tests unitaires (pytest)
docker/              # Metabase + PostgreSQL local
scripts/             # Scripts utilitaires (setup Docker, exploration staging)
docs/                # Documentation complete (PRD, ADRs, guides)
```

## Usage

```bash
# Installation
just sync

# Tests
just test

# Docker local (sans GCP)
just docker-setup
just docker-test-migrate

# Staging (necessite gcloud auth)
just stg-explore
just stg-dry-run <legacy_table> <new_table> <legacy_schema> <new_schema> <card_ids>
```

Pour la documentation complete, voir [`docs/`](docs/).
