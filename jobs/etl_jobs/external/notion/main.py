"""Export the Notion dashboard-docs database into the BigQuery raw layer.

    uv run main.py export     # export → raw_<env>.notion_dashboard_docs

Secrets (Secret Manager on GCP_PROJECT_ID):
    notion_api_key_<env>
    notion_dashboard_database_id_<env>
"""

import json
import logging
from datetime import datetime, timezone

import typer

from core.bq import save_to_bq
from core.notion_api import NotionClient
from core.parsing import (
    detect_dashboard,
    prop_to_text,
    skip_reason,
    title_of,
)
from core.utils import (
    NOTION_DOCS_TABLE,
    NOTION_VERSION,
    get_notion_db_id,
    get_notion_token,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

app = typer.Typer()


@app.callback()
def _main():
    """Keep Typer in multi-command mode so subcommands are invoked by name."""


def _client() -> NotionClient:
    token = get_notion_token()
    if not token:
        raise RuntimeError(
            "No Notion token: set NOTION_TOKEN or grant access to notion_api_key_<env>."
        )
    return NotionClient(token, NOTION_VERSION)


def _collect_rows(notion: NotionClient, db_id: str, execution_date: datetime):
    """Yield one dict per kept doc; log and drop skipped docs."""
    kept = skipped = 0
    for pg in notion.query_db(db_id):
        pid = pg["id"]
        props = pg.get("properties", {})
        props_text = {k: prop_to_text(v) for k, v in props.items()}
        title = title_of(props) or "(untitled)"
        body = notion.page_markdown(pid)
        dash_url, dash_id = detect_dashboard(props_text, pg.get("url", ""), body)

        reason = skip_reason(title, dash_id, body)
        if reason:
            skipped += 1
            logger.info("skip (%s): %s", reason, title[:60])
            continue

        kept += 1
        logger.info("[%s] %s  dash=%s", kept, title[:60], dash_id)
        yield {
            "page_id": pid,
            "title": title,
            "notion_url": pg.get("url"),
            "dashboard_id": dash_id,
            "dashboard_url": dash_url,
            "properties": json.dumps(props_text, ensure_ascii=False),
            "body_md": f"# {title}\n\n{body}\n",
            "last_edited": pg.get("last_edited_time"),
            "execution_date": execution_date,
        }
    logger.info("collected %s docs, skipped %s", kept, skipped)


@app.command()
def export():
    """Export the dashboard-docs DB → raw_<env>.notion_dashboard_docs."""
    try:
        notion = _client()
        db_id = get_notion_db_id()
        execution_date = datetime.now(timezone.utc)
        rows = list(_collect_rows(notion, db_id, execution_date))
        save_to_bq(rows, execution_date)
        logger.info("Done: %s docs → %s", len(rows), NOTION_DOCS_TABLE)
    except typer.Exit:
        # typer.Exit is a subclass of Exception — re-raise before broad except catches it
        raise
    except Exception as e:
        logger.exception(f"ETL job failed: {e}")
        raise typer.Exit(code=1) from e


if __name__ == "__main__":
    app()
