"""Enrich + reconcile Notion dashboard docs → int_metabase_<env>.dashboard_documentation.

uv run main.py enrich
uv run main.py enrich --force
"""

import logging
from datetime import datetime, timezone

import typer
from google.cloud import bigquery

from core import bq, enrich, reconcile
from core.prompts import PROMPT_VERSION
from core.utils import DASHBOARD_DOCS_TABLE, GCP_PROJECT

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

app = typer.Typer()

_ARRAY_FIELDS = [
    "questions_answered",
    "key_metrics",
    "dimensions",
    "caveats",
    "related_concepts",
]
_SCALAR_FIELDS = ["summary", "audience", "definition_alignment", "confidence"]


@app.callback()
def _main():
    """Keep Typer in multi-command mode so subcommands are invoked by name."""


def _spec_from_cache(prev: dict) -> dict:
    """Reuse the enriched fields of a previously-stored row (unchanged doc)."""
    spec = {f: list(prev.get(f) or []) for f in _ARRAY_FIELDS}
    spec.update({f: prev.get(f) for f in _SCALAR_FIELDS})
    return spec


def _build_row(
    doc: dict, spec: dict, flags: dict, s_hash: str, execution_date: datetime
) -> dict:
    dash_id = doc.get("dashboard_id")
    return {
        "page_id": doc["page_id"],
        "title": doc.get("title"),
        "notion_url": doc.get("notion_url"),
        "dashboard_id": int(dash_id) if dash_id is not None else None,
        "dashboard_url": doc.get("dashboard_url"),
        **spec,
        **flags,
        "source_hash": s_hash,
        "prompt_version": PROMPT_VERSION,
        "execution_date": execution_date.isoformat(),
    }


@app.command("enrich")
def enrich_docs(
    force: bool = typer.Option(
        False, "--force", help="Re-enrich every doc, ignoring the cache."
    ),
    limit: int = typer.Option(
        None, "--limit", help="Only process the first N docs (local testing)."
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Enrich + log rows but skip the BQ write."
    ),
):
    """Enrich + reconcile dashboard docs into int_metabase_<env>.dashboard_documentation."""
    client = bigquery.Client(project=GCP_PROJECT)
    execution_date = datetime.now(timezone.utc)

    docs = bq.read_notion_docs(client)
    if not docs:
        logger.warning("No source docs in the latest notion partition — nothing to do.")
        return
    if limit is not None:
        docs = docs[:limit]
        logger.info("Limiting to %s docs (local testing).", len(docs))

    previous = {} if force else bq.read_existing_docs(client)
    catalog = bq.read_asset_catalog_dashboards(client)
    card_ids = bq.read_dashboard_card_ids(client)
    card_change = bq.read_dashboard_card_last_change(client)
    agent = enrich.build_agent(GCP_PROJECT)

    rows, enriched, reused, failed = [], 0, 0, 0
    for doc in docs:
        pid = doc["page_id"]
        dash_id = (
            int(doc["dashboard_id"]) if doc.get("dashboard_id") is not None else None
        )
        catalog_row = catalog.get(dash_id)
        s_hash = enrich.source_hash(doc.get("body_md"))

        prev = previous.get(pid)
        if prev is not None and prev.get("source_hash") == s_hash:
            spec = _spec_from_cache(prev)
            reused += 1
        else:
            try:
                ctx = enrich.build_catalog_context(
                    catalog_row,
                    card_ids.get(dash_id, []),
                    lambda cid: bq.card_sql_snippet(client, cid),
                )
                spec = enrich.enrich_one(agent, doc, ctx).model_dump()
                enriched += 1
                logger.info("✓ enriched %s", (doc.get("title") or "")[:60])
            except Exception as e:
                failed += 1
                logger.error("✗ %s: %s", (doc.get("title") or "")[:50], e)
                continue

        flags = reconcile.reconcile(doc, catalog_row, card_change.get(dash_id))
        rows.append(_build_row(doc, spec, flags, s_hash, execution_date))

    logger.info(
        "Prepared %s rows (enriched=%s reused=%s failed=%s)",
        len(rows),
        enriched,
        reused,
        failed,
    )
    if dry_run:
        import json

        for r in rows:
            logger.info("DRY-RUN row: %s", json.dumps(r, ensure_ascii=False)[:800])
        logger.info("Dry-run — %s not written.", DASHBOARD_DOCS_TABLE)
        return

    bq.save_docs(client, rows, execution_date)
    logger.info("Done: %s rows → %s", len(rows), DASHBOARD_DOCS_TABLE)


if __name__ == "__main__":
    app()
