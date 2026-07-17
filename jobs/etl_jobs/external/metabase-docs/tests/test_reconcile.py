from datetime import datetime, timedelta, timezone

from core.reconcile import reconcile

NOW = datetime(2026, 7, 1, tzinfo=timezone.utc)


def _doc(last_edited):
    return {"page_id": "p", "dashboard_id": 881, "last_edited": last_edited}


def test_dashboard_exists_when_live():
    flags = reconcile(_doc(NOW), {"is_archived": False}, NOW)
    assert flags["dashboard_exists"] is True


def test_dashboard_missing_is_orphan():
    flags = reconcile(_doc(NOW), None, None)
    assert flags["dashboard_exists"] is False
    assert flags["doc_is_stale"] is False


def test_archived_dashboard_not_exists():
    flags = reconcile(_doc(NOW), {"is_archived": True}, NOW)
    assert flags["dashboard_exists"] is False


def test_stale_when_doc_older_than_cards():
    flags = reconcile(_doc(NOW - timedelta(days=10)), {"is_archived": False}, NOW)
    assert flags["doc_is_stale"] is True


def test_fresh_when_doc_newer_than_cards():
    flags = reconcile(_doc(NOW), {"is_archived": False}, NOW - timedelta(days=10))
    assert flags["doc_is_stale"] is False


def test_naive_and_iso_string_timestamps_are_handled():
    flags = reconcile(
        {"page_id": "p", "dashboard_id": 1, "last_edited": "2026-06-01T00:00:00Z"},
        {"is_archived": False},
        datetime(2026, 6, 15),  # naive → treated as UTC
    )
    assert flags["doc_is_stale"] is True
