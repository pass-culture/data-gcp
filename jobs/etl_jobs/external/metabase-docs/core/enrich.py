"""LLM enrichment of Notion dashboard docs via Vertex AI Gemini (pydantic-ai)."""

import hashlib
import logging

from pydantic_ai import Agent
from pydantic_ai.models.google import GoogleModel, GoogleModelSettings
from pydantic_ai.providers.google import GoogleProvider

from core import prompts
from core.prompts import DashboardSpec
from core.utils import SQL_SNIPPET_CARDS, VERTEX_LOCATION, VERTEX_MODEL

logger = logging.getLogger(__name__)


def build_agent(project: str) -> Agent[None, DashboardSpec]:
    """A pydantic-ai agent that forces a validated DashboardSpec out of Vertex Gemini."""
    model = GoogleModel(
        VERTEX_MODEL,
        provider=GoogleProvider(
            vertexai=True, project=project, location=VERTEX_LOCATION
        ),
    )
    return Agent(
        model,
        output_type=DashboardSpec,
        system_prompt=prompts.ENRICH_SYSTEM,
        model_settings=GoogleModelSettings(temperature=0.1),
    )


def source_hash(body: str) -> str:
    """Cache key: changes when the doc body OR the prompt version changes."""
    return hashlib.sha256(
        (prompts.PROMPT_VERSION + "\n" + (body or "")).encode()
    ).hexdigest()[:16]


def build_catalog_context(catalog_row: dict, card_ids: list, card_sql_fn) -> str:
    """Member cards, classification and sample card SQL for one dashboard."""
    if catalog_row is None:
        return "(no matching dashboard in asset_catalog)"

    member_cards = list(catalog_row.get("member_cards") or [])
    lines = []
    squad, tier = catalog_row.get("squad"), catalog_row.get("tier")
    if squad or tier:
        lines.append(
            f"Classification: squad={squad or '?'} tier={tier or '?'} "
            f"certified={catalog_row.get('certified')}"
        )
    markdown = catalog_row.get("dashboard_markdown")
    if markdown:
        lines.append(f"In-Metabase description: {str(markdown)[:800]}")

    lines.append(f"Member cards ({len(member_cards)}):")
    lines.extend(f"- {name}" for name in member_cards[:25])

    snippets = []
    for cid in (card_ids or [])[:SQL_SNIPPET_CARDS]:
        sql = card_sql_fn(cid)
        if sql:
            snippets.append(f"- card {cid}: {sql}")
    if snippets:
        lines.append("\nSample card SQL (compiled, truncated):")
        lines.extend(snippets)
    return "\n".join(lines)


def enrich_one(
    agent: Agent[None, DashboardSpec], doc: dict, catalog_context: str
) -> DashboardSpec:
    """Call Gemini and return the validated structured spec (LLM output only)."""
    prompt = prompts.ENRICH_TEMPLATE.format(
        title=doc.get("title", ""),
        dashboard_url=doc.get("dashboard_url") or "(none)",
        catalog_context=catalog_context[:6000],
        raw_markdown=(doc.get("body_md") or "")[:12000],
    )
    return agent.run_sync(prompt).output
