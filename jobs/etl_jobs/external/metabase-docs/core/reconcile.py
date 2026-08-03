"""Structural reconcile of a doc against the live dashboard (no LLM)."""

from datetime import datetime, timezone


def _as_utc(value) -> datetime | None:
    """Coerce a BQ TIMESTAMP / ISO string to a tz-aware UTC datetime."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def reconcile(doc: dict, catalog_row: dict | None, last_card_change) -> dict:
    """Return {dashboard_exists, doc_is_stale} for one doc."""
    dashboard_exists = catalog_row is not None and not catalog_row.get(
        "is_archived", False
    )

    doc_is_stale = False
    last_edited = _as_utc(doc.get("last_edited"))
    card_change = _as_utc(last_card_change)
    if dashboard_exists and last_edited is not None and card_change is not None:
        doc_is_stale = last_edited < card_change

    return {"dashboard_exists": dashboard_exists, "doc_is_stale": doc_is_stale}
