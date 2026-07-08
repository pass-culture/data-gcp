import pytest
from pydantic import ValidationError

from core import prompts
from core.enrich import build_catalog_context, source_hash
from core.prompts import DashboardSpec


def test_source_hash_is_stable_and_body_sensitive():
    assert source_hash("hello") == source_hash("hello")
    assert source_hash("hello") != source_hash("world")


def test_source_hash_changes_with_prompt_version(monkeypatch):
    before = source_hash("body")
    monkeypatch.setattr(prompts, "PROMPT_VERSION", "vTEST")
    assert source_hash("body") != before


def test_source_hash_handles_none_body():
    assert isinstance(source_hash(None), str)


def test_context_none_catalog():
    assert "no matching dashboard" in build_catalog_context(None, [], lambda cid: "")


def test_context_lists_member_cards_and_sql():
    catalog_row = {
        "member_cards": ["Revenue", "Users"],
        "squad": "data",
        "tier": "1",
        "certified": True,
        "dashboard_markdown": "Some in-metabase doc",
    }
    ctx = build_catalog_context(catalog_row, [42], lambda cid: f"select {cid}")
    assert "Member cards (2)" in ctx
    assert "- Revenue" in ctx
    assert "card 42: select 42" in ctx
    assert "squad=data" in ctx


def test_context_skips_empty_sql():
    ctx = build_catalog_context({"member_cards": ["A"]}, [1, 2], lambda cid: "")
    assert "Sample card SQL" not in ctx


def test_dashboard_spec_normalizes_arrays_and_scalars():
    spec = DashboardSpec.model_validate(
        {
            "summary": "s",
            "questions_answered": ["q1", "q2"],
            "key_metrics": None,  # None → empty array
            "audience": "",  # "" → None
        }
    )
    assert spec.questions_answered == ["q1", "q2"]
    assert spec.key_metrics == []
    assert spec.audience is None
    assert spec.summary == "s"
    assert spec.dimensions == []  # missing → empty array


def test_dashboard_spec_coerces_list_items_to_str():
    spec = DashboardSpec.model_validate({"key_metrics": [1, 2.5]})
    assert spec.key_metrics == ["1", "2.5"]


def test_dashboard_spec_rejects_out_of_enum_values():
    with pytest.raises(ValidationError):
        DashboardSpec.model_validate({"confidence": "very-high"})
