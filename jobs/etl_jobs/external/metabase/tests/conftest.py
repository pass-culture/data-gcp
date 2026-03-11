"""Shared fixtures for Metabase migration tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def native_card_fixture() -> dict:
    """Load the native SQL card MBQL v5 fixture."""
    with open(FIXTURES_DIR / "card_native_mbql5.json") as f:
        return json.load(f)


@pytest.fixture
def query_card_fixture() -> dict:
    """Load the query builder card MBQL v5 fixture."""
    with open(FIXTURES_DIR / "card_query_mbql5.json") as f:
        return json.load(f)


@pytest.fixture
def field_mapping() -> dict[int, int]:
    """Sample field ID mapping: old → new."""
    return {
        201: 301,  # user_id
        202: 302,  # booking_cnt → total_individual_bookings
        203: 303,  # total_amount
        204: 304,  # user_type
    }


@pytest.fixture
def table_mapping() -> dict[int, int]:
    """Sample table ID mapping: old → new."""
    return {10: 20}


@pytest.fixture
def column_mapping() -> dict[str, str]:
    """Sample column name mapping: old → new."""
    return {
        "booking_cnt": "total_individual_bookings",
        "total_amount": "total_actual_amount_spent",
    }
