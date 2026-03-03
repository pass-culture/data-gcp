"""Regression tests for ISO8601 datetime parsing with mixed fractional seconds.

These tests call the real BrevoTransactional.parse_to_df method to ensure
it handles dates with and without fractional seconds — the root cause of
the production failure in import_brevo / import_pro_transactional_data_to_tmp.
"""

import sys
from dataclasses import dataclass
from datetime import date
from types import ModuleType
from unittest.mock import MagicMock

import pytest

# Stub out heavy dependencies so we can import brevo_transactional
# without installing brevo_python or google-cloud-bigquery.
for mod_name in (
    "brevo_python",
    "brevo_python.rest",
    "google.cloud",
    "google.cloud.bigquery",
):
    if mod_name not in sys.modules:
        sys.modules[mod_name] = MagicMock()

# Also stub the local `utils` module which requires GCP env vars
utils_stub = ModuleType("utils")
utils_stub.ENV_SHORT_NAME = "test"
utils_stub.rate_limiter = lambda *a, **kw: (lambda f: f)
sys.modules["utils"] = utils_stub

from brevo_transactional import BrevoTransactional  # noqa: E402


@dataclass
class FakeEvent:
    email: str
    event: str
    template_id: int
    _date: str
    tag: str


@pytest.fixture
def brevo():
    return BrevoTransactional(
        gcp_project="test-project",
        tmp_dataset="tmp",
        destination_table_name="test_table",
        api_keys=["fake-key"],
        start_date="2026-03-01",
        end_date="2026-03-02",
    )


def _make_events(date_strings):
    return [
        FakeEvent(
            email=f"user{i}@test.com",
            event="delivered",
            template_id=1,
            _date=d,
            tag="tag",
        )
        for i, d in enumerate(date_strings)
    ]


class TestParseTodfDatetimeParsing:
    """Tests that call the real parse_to_df — will fail if format='ISO8601' is removed."""

    def test_dates_with_fractional_seconds(self, brevo):
        events = _make_events(
            [
                "2026-03-01T08:39:23.000+01:00",
                "2026-03-01T08:39:21.000+01:00",
            ]
        )
        df = brevo.parse_to_df(events)
        assert not df.empty
        assert (df["event_date"] == date(2026, 3, 1)).all()

    def test_dates_without_fractional_seconds(self, brevo):
        events = _make_events(
            [
                "2026-03-01T12:08:03+01:00",
                "2026-03-01T14:00:00+01:00",
            ]
        )
        df = brevo.parse_to_df(events)
        assert not df.empty
        assert (df["event_date"] == date(2026, 3, 1)).all()

    def test_mixed_fractional_and_non_fractional_seconds(self, brevo):
        """The exact case that caused the production bug."""
        events = _make_events(
            [
                "2026-03-01T08:39:23.000+01:00",  # with .000
                "2026-03-01T08:01:16.000+01:00",  # with .000
                "2026-03-01T12:08:03+01:00",  # WITHOUT fractional seconds
                "2026-03-01T12:02:57.069+01:00",  # with .069
                "2026-03-01T11:36:18.978+01:00",  # with .978
                "2026-03-01T10:23:19.911+01:00",  # with .911
            ]
        )
        df = brevo.parse_to_df(events)
        assert len(df) == len(events)
        assert (df["event_date"] == date(2026, 3, 1)).all()

    def test_mixed_with_variable_precision(self, brevo):
        events = _make_events(
            [
                "2026-03-01T10:00:00.123456+01:00",  # microseconds
                "2026-03-01T10:00:01.123+01:00",  # milliseconds
                "2026-03-01T10:00:02+01:00",  # no fractional
            ]
        )
        df = brevo.parse_to_df(events)
        assert len(df) == len(events)
