"""
Unit tests for filter logic, panachage sorting, and Pydantic validators.

These tests run without any external services (no GCS, LanceDB, HuggingFace, etc.)
by testing the pure functions directly with in-memory Polars DataFrames.
"""

import datetime

import pandas as pd
import polars as pl
import pytest
from pydantic import ValidationError

from app.models.validators import (
    OfferSelection,
    PredictionRequest,
    PredictionResult,
    SearchResult,
)
from app.post_process.panachage import panachage_sort
from app.search.filters import apply_filters

# ── Fixtures ─────────────────────────────────────────────────────────────

SAMPLE_DATA = {
    "item_id": ["1", "2", "3", "4", "5"],
    "offer_id": ["o1", "o2", "o3", "o4", "o5"],
    "offer_category_id": ["LIVRE", "MUSEE", "LIVRE", "CINEMA", "MUSEE"],
    "venue_department_code": ["75", "13", "75", "69", "75"],
    "last_stock_price": [5.0, 15.0, 8.0, 50.0, 3.0],
    "offer_creation_date": [
        datetime.date(2023, 6, 1),
        datetime.date(2024, 1, 15),
        datetime.date(2022, 3, 10),
        datetime.date(2025, 7, 20),
        datetime.date(2024, 11, 5),
    ],
    "stock_beginning_date": [
        datetime.date(2023, 9, 1),
        datetime.date(2024, 3, 1),
        datetime.date(2022, 6, 1),
        datetime.date(2025, 9, 1),
        datetime.date(2025, 1, 1),
    ],
}


@pytest.fixture()
def sample_lf() -> pl.LazyFrame:
    return pl.LazyFrame(SAMPLE_DATA)


# ── apply_filters tests ─────────────────────────────────────────────────


class TestApplyFilters:
    def test_filter_in_operator(self, sample_lf):
        """Filter by offer_category_id IN ['LIVRE', 'MUSEE']."""
        filters = [
            {
                "column": "offer_category_id",
                "operator": "in",
                "value": ["LIVRE", "MUSEE"],
            }
        ]
        result = apply_filters(sample_lf, filters).collect()
        assert len(result) == 4
        assert set(result["offer_category_id"].to_list()) == {"LIVRE", "MUSEE"}

    def test_filter_lte_operator(self, sample_lf):
        """Filter by last_stock_price <= 10.0."""
        filters = [{"column": "last_stock_price", "operator": "<=", "value": 10.0}]
        result = apply_filters(sample_lf, filters).collect()
        assert len(result) == 3
        assert all(p <= 10.0 for p in result["last_stock_price"].to_list())

    def test_filter_lt_operator(self, sample_lf):
        """Filter by last_stock_price < 50.0."""
        filters = [{"column": "last_stock_price", "operator": "<", "value": 50.0}]
        result = apply_filters(sample_lf, filters).collect()
        assert len(result) == 4

    def test_filter_eq_operator(self, sample_lf):
        """Filter by venue_department_code = '75'."""
        filters = [{"column": "venue_department_code", "operator": "=", "value": "75"}]
        result = apply_filters(sample_lf, filters).collect()
        assert len(result) == 3
        assert all(d == "75" for d in result["venue_department_code"].to_list())

    def test_filter_between_offer_creation_date(self, sample_lf):
        """Filter by offer_creation_date between 2023-01-01 and 2026-01-01."""
        filters = [
            {
                "column": "offer_creation_date",
                "operator": "between",
                "value": ["2023-01-01", "2026-01-01"],
            }
        ]
        result = apply_filters(sample_lf, filters).collect()
        assert len(result) == 4  # excludes item 3 (2022-03-10)

    def test_filter_between_stock_beginning_date(self, sample_lf):
        """Filter by stock_beginning_date between 2023-01-01 and 2026-01-01."""
        filters = [
            {
                "column": "stock_beginning_date",
                "operator": "between",
                "value": ["2023-01-01", "2026-01-01"],
            }
        ]
        result = apply_filters(sample_lf, filters).collect()
        assert len(result) == 4  # excludes item 3 (2022-06-01)

    def test_filter_multiple_combined(self, sample_lf):
        """
        Multiple filters: venue_department_code = '75' AND last_stock_price < 50.0.
        """
        filters = [
            {"column": "venue_department_code", "operator": "=", "value": "75"},
            {"column": "last_stock_price", "operator": "<", "value": 50.0},
        ]
        result = apply_filters(sample_lf, filters).collect()
        assert len(result) == 3
        assert all(d == "75" for d in result["venue_department_code"].to_list())
        assert all(p < 50.0 for p in result["last_stock_price"].to_list())

    def test_filter_wrong_operator_raises(self, sample_lf):
        """Unsupported operator should raise ValueError."""
        filters = [
            {"column": "offer_category_id", "operator": "WRONG_OP", "value": "LIVRE"}
        ]
        with pytest.raises(ValueError, match="Unsupported operator"):
            apply_filters(sample_lf, filters).collect()

    def test_filter_between_non_list_raises(self, sample_lf):
        """
        'between' with a non-list value should raise ValueError (no such operator).
        """
        filters = [
            {"column": "offer_category_id", "operator": "between", "value": "LIVRE"}
        ]
        # 'between' with a non-list falls through to OPERATOR_MAP which doesn't have it
        with pytest.raises(ValueError, match="Unsupported operator"):
            apply_filters(sample_lf, filters).collect()

    def test_filter_in_non_list_raises(self, sample_lf):
        """'in' with a non-list value should raise a Polars error."""
        filters = [{"column": "offer_category_id", "operator": "in", "value": "LIVRE"}]
        with pytest.raises(Exception):  # noqa: B017, PT011
            apply_filters(sample_lf, filters).collect()

    def test_filter_unknown_column_skipped(self, sample_lf):
        """Unknown column should be silently skipped."""
        filters = [{"column": "nonexistent_column", "operator": "=", "value": "test"}]
        result = apply_filters(sample_lf, filters).collect()
        assert len(result) == 5  # no filtering applied

    def test_empty_filters(self, sample_lf):
        """Empty filter list returns all rows."""
        result = apply_filters(sample_lf, []).collect()
        assert len(result) == 5


# ── Panachage sort tests ────────────────────────────────────────────────


class TestPanachageSort:
    def test_interleaves_ranks(self):
        """Rows with different ranks should be interleaved."""
        df = pd.DataFrame(
            {
                "offer_id": ["a", "b", "c", "d"],
                "pertinence": ["p1", "p2", "p3", "p4"],
                "rank": [1, 2, 1, 2],
            }
        )
        result = panachage_sort(df)
        assert len(result) == 4
        # First two should be one from each rank
        assert result.iloc[0]["rank"] != result.iloc[1]["rank"]

    def test_single_rank(self):
        """All same rank preserves all rows."""
        df = pd.DataFrame(
            {
                "offer_id": ["a", "b", "c"],
                "pertinence": ["p1", "p2", "p3"],
                "rank": [1, 1, 1],
            }
        )
        result = panachage_sort(df)
        assert len(result) == 3


# ── Pydantic validator tests ────────────────────────────────────────────


class TestValidators:
    def test_prediction_request_valid(self):
        req = PredictionRequest(
            search_query="test",
            filters_list=[{"column": "x", "operator": "=", "value": 1}],
        )
        assert req.search_query == "test"
        assert len(req.filters_list) == 1

    def test_prediction_request_no_filters(self):
        req = PredictionRequest(search_query="test")
        assert req.filters_list is None

    def test_prediction_request_missing_query_raises(self):
        with pytest.raises(ValidationError):
            PredictionRequest()

    def test_search_result_empty(self):
        sr = SearchResult(offers=[])
        assert sr.offers == []

    def test_search_result_with_offers(self):
        sr = SearchResult(
            offers=[OfferSelection(offer_id="o1", pertinence="good match")]
        )
        assert len(sr.offers) == 1
        assert sr.offers[0].offer_id == "o1"

    def test_prediction_result_structure(self):
        pr = PredictionResult(
            predictions=SearchResult(
                offers=[OfferSelection(offer_id="o1", pertinence="relevant")]
            )
        )
        data = pr.model_dump()
        assert "predictions" in data
        assert "offers" in data["predictions"]
        assert data["predictions"]["offers"][0]["offer_id"] == "o1"
