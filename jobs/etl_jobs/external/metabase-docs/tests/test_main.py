from datetime import datetime, timezone

from core.prompts import DashboardSpec
from main import _build_row, _spec_from_cache


def test_spec_from_cache_roundtrips():
    prev = {"summary": "cached", "questions_answered": ["q"], "confidence": "high"}
    spec = _spec_from_cache(prev)
    assert spec["summary"] == "cached"
    assert spec["questions_answered"] == ["q"]
    assert spec["caveats"] == []


def test_build_row_shape():
    doc = {
        "page_id": "p1",
        "title": "T",
        "notion_url": "u",
        "dashboard_id": 881,
        "dashboard_url": "/dashboard/881",
    }
    spec = DashboardSpec(summary="s").model_dump()
    flags = {"dashboard_exists": True, "doc_is_stale": False}
    row = _build_row(
        doc, spec, flags, "abc123", datetime(2026, 7, 1, tzinfo=timezone.utc)
    )
    assert row["page_id"] == "p1"
    assert row["dashboard_id"] == 881
    assert row["dashboard_exists"] is True
    assert row["source_hash"] == "abc123"
    assert row["execution_date"] == "2026-07-01T00:00:00+00:00"
    assert row["prompt_version"]  # set from PROMPT_VERSION
