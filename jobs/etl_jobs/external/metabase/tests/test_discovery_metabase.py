from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

from api.client import MetabaseClient
from api.models import CardDependencyInfo, TableDependency
from discovery.metabase import (
    build_card_dependency_cache,
    get_impacted_cards_from_cache,
    load_card_dependency_cache,
)


def _make_client() -> MetabaseClient:
    return MetabaseClient(host="http://fake-metabase:3000", session_token="fake-token")


def _make_native_card(
    card_id: int,
    name: str,
    sql: str,
    *,
    creator_email: str = "user@example.com",
    collection_id: int | None = 1,
) -> dict[str, Any]:
    return {
        "id": card_id,
        "name": name,
        "creator": {"email": creator_email},
        "collection_id": collection_id,
        "dataset_query": {
            "stages": [
                {
                    "lib/type": "mbql.stage/native",
                    "native": sql,
                }
            ]
        },
    }


def _make_qb_card(
    card_id: int,
    name: str,
    source_table: int,
    *,
    creator_email: str = "user@example.com",
    collection_id: int | None = 1,
) -> dict[str, Any]:
    return {
        "id": card_id,
        "name": name,
        "creator": {"email": creator_email},
        "collection_id": collection_id,
        "dataset_query": {
            "stages": [
                {
                    "lib/type": "mbql.stage/mbql",
                    "source-table": source_table,
                }
            ]
        },
    }


SAMPLE_TABLE_CATALOG: dict[str, list[dict[str, Any]]] = {
    "analytics": [
        {"id": 10, "name": "user_stats", "active": True},
        {"id": 20, "name": "bookings", "active": True},
    ],
    "marketing": [
        {"id": 30, "name": "campaigns", "active": True},
    ],
}


class TestBuildCardDependencyCache:
    def test_matches_native_card_to_table(self, tmp_path: Path) -> None:
        # Given
        client = _make_client()
        cards = [
            _make_native_card(100, "User Report", "SELECT * FROM analytics.user_stats"),
        ]
        client.fetch_all_cards = MagicMock(return_value=cards)  # type: ignore[method-assign]
        client.resolve_collection_path = MagicMock(return_value=("Root/Reports", "1/5"))  # type: ignore[method-assign]
        cache_path = tmp_path / "cache.json"

        # When
        result = build_card_dependency_cache(client, SAMPLE_TABLE_CATALOG, cache_path=cache_path)

        # Then
        expected = {
            "user_stats": TableDependency(
                id=10,
                schema="analytics",
                cards_using_table={
                    "100": CardDependencyInfo(
                        card_name="User Report",
                        card_type="native",
                        card_owner="user@example.com",
                        collection_path_names="Root/Reports",
                        collection_path_ids="1/5",
                    ),
                },
            ),
        }
        assert result == expected

    def test_matches_query_builder_card_to_table(self, tmp_path: Path) -> None:
        # Given
        client = _make_client()
        cards = [_make_qb_card(200, "Booking Dashboard", 20)]
        client.fetch_all_cards = MagicMock(return_value=cards)  # type: ignore[method-assign]
        client.resolve_collection_path = MagicMock(return_value=("Root", "1"))  # type: ignore[method-assign]
        cache_path = tmp_path / "cache.json"

        # When
        result = build_card_dependency_cache(client, SAMPLE_TABLE_CATALOG, cache_path=cache_path)

        # Then
        expected = {
            "bookings": TableDependency(
                id=20,
                schema="analytics",
                cards_using_table={
                    "200": CardDependencyInfo(
                        card_name="Booking Dashboard",
                        card_type="query_builder",
                        card_owner="user@example.com",
                        collection_path_names="Root",
                        collection_path_ids="1",
                    ),
                },
            ),
        }
        assert result == expected

    def test_native_card_matches_multiple_tables(self, tmp_path: Path) -> None:
        # Given
        client = _make_client()
        cards = [
            _make_native_card(300, "Join Query", "SELECT * FROM analytics.user_stats JOIN analytics.bookings ON 1=1"),
        ]
        client.fetch_all_cards = MagicMock(return_value=cards)  # type: ignore[method-assign]
        client.resolve_collection_path = MagicMock(return_value=("Root", "1"))  # type: ignore[method-assign]
        cache_path = tmp_path / "cache.json"

        # When
        result = build_card_dependency_cache(client, SAMPLE_TABLE_CATALOG, cache_path=cache_path)

        # Then
        assert "user_stats" in result
        assert "bookings" in result
        assert "300" in result["user_stats"].cards_using_table
        assert "300" in result["bookings"].cards_using_table

    def test_ignores_unknown_card_types(self, tmp_path: Path) -> None:
        # Given
        client = _make_client()
        cards = [
            {
                "id": 400,
                "name": "Unknown Card",
                "creator": {"email": "user@example.com"},
                "collection_id": 1,
                "dataset_query": {"stages": [{"lib/type": "mbql.stage/unknown"}]},
            },
        ]
        client.fetch_all_cards = MagicMock(return_value=cards)  # type: ignore[method-assign]
        cache_path = tmp_path / "cache.json"

        # When
        result = build_card_dependency_cache(client, SAMPLE_TABLE_CATALOG, cache_path=cache_path)

        # Then
        expected: dict[str, TableDependency] = {}
        assert result == expected

    def test_ignores_card_with_no_stages(self, tmp_path: Path) -> None:
        # Given
        client = _make_client()
        cards = [
            {
                "id": 500,
                "name": "No Stages Card",
                "creator": {"email": "user@example.com"},
                "collection_id": 1,
                "dataset_query": {"stages": []},
            },
        ]
        client.fetch_all_cards = MagicMock(return_value=cards)  # type: ignore[method-assign]
        cache_path = tmp_path / "cache.json"

        # When
        result = build_card_dependency_cache(client, SAMPLE_TABLE_CATALOG, cache_path=cache_path)

        # Then
        expected: dict[str, TableDependency] = {}
        assert result == expected

    def test_ignores_card_with_no_dataset_query(self, tmp_path: Path) -> None:
        # Given
        client = _make_client()
        cards = [
            {
                "id": 600,
                "name": "No Query Card",
                "creator": {"email": "user@example.com"},
                "collection_id": 1,
                "dataset_query": None,
            },
        ]
        client.fetch_all_cards = MagicMock(return_value=cards)  # type: ignore[method-assign]
        cache_path = tmp_path / "cache.json"

        # When
        result = build_card_dependency_cache(client, SAMPLE_TABLE_CATALOG, cache_path=cache_path)

        # Then
        expected: dict[str, TableDependency] = {}
        assert result == expected

    def test_writes_json_cache_file(self, tmp_path: Path) -> None:
        # Given
        client = _make_client()
        cards = [_make_qb_card(700, "Test Card", 30, creator_email="admin@test.com")]
        client.fetch_all_cards = MagicMock(return_value=cards)  # type: ignore[method-assign]
        client.resolve_collection_path = MagicMock(return_value=("Marketing", "3"))  # type: ignore[method-assign]
        cache_path = tmp_path / "cache.json"

        # When
        build_card_dependency_cache(client, SAMPLE_TABLE_CATALOG, cache_path=cache_path)

        # Then
        expected = {
            "campaigns": {
                "id": 30,
                "schema": "marketing",
                "cards_using_table": {
                    "700": {
                        "card_name": "Test Card",
                        "card_type": "query_builder",
                        "card_owner": "admin@test.com",
                        "collection_path_names": "Marketing",
                        "collection_path_ids": "3",
                    },
                },
            },
        }
        assert json.loads(cache_path.read_text()) == expected

    def test_handles_card_without_creator(self, tmp_path: Path) -> None:
        # Given
        client = _make_client()
        cards = [
            {
                "id": 800,
                "name": "Orphan Card",
                "collection_id": 1,
                "dataset_query": {
                    "stages": [
                        {
                            "lib/type": "mbql.stage/mbql",
                            "source-table": 10,
                        }
                    ]
                },
            },
        ]
        client.fetch_all_cards = MagicMock(return_value=cards)  # type: ignore[method-assign]
        client.resolve_collection_path = MagicMock(return_value=("Root", "1"))  # type: ignore[method-assign]
        cache_path = tmp_path / "cache.json"

        # When
        result = build_card_dependency_cache(client, SAMPLE_TABLE_CATALOG, cache_path=cache_path)

        # Then
        assert result["user_stats"].cards_using_table["800"].card_owner == ""

    def test_multiple_cards_on_same_table(self, tmp_path: Path) -> None:
        # Given
        client = _make_client()
        cards = [
            _make_qb_card(901, "Card A", 10, creator_email="alice@test.com"),
            _make_qb_card(902, "Card B", 10, creator_email="bob@test.com"),
        ]
        client.fetch_all_cards = MagicMock(return_value=cards)  # type: ignore[method-assign]
        client.resolve_collection_path = MagicMock(return_value=("Root", "1"))  # type: ignore[method-assign]
        cache_path = tmp_path / "cache.json"

        # When
        result = build_card_dependency_cache(client, SAMPLE_TABLE_CATALOG, cache_path=cache_path)

        # Then
        assert len(result["user_stats"].cards_using_table) == 2
        assert "901" in result["user_stats"].cards_using_table
        assert "902" in result["user_stats"].cards_using_table


class TestLoadCardDependencyCache:
    def test_raises_when_file_not_found(self, tmp_path: Path) -> None:
        # Given
        cache_path = tmp_path / "nonexistent.json"

        # When / Then
        import pytest

        with pytest.raises(FileNotFoundError, match="Card dependency cache not found"):
            load_card_dependency_cache(cache_path=cache_path)

    def test_round_trips_with_build(self, tmp_path: Path) -> None:
        # Given
        client = _make_client()
        cards = [
            _make_native_card(100, "Report", "SELECT * FROM analytics.user_stats"),
        ]
        client.fetch_all_cards = MagicMock(return_value=cards)  # type: ignore[method-assign]
        client.resolve_collection_path = MagicMock(return_value=("Root/Reports", "1/5"))  # type: ignore[method-assign]
        cache_path = tmp_path / "cache.json"
        original = build_card_dependency_cache(client, SAMPLE_TABLE_CATALOG, cache_path=cache_path)

        # When
        loaded = load_card_dependency_cache(cache_path=cache_path)

        # Then
        assert loaded == original


class TestGetImpactedCardsFromCache:
    def test_returns_sorted_card_ids(self) -> None:
        # Given
        cache = {
            "my_table": TableDependency(
                id=10,
                schema="analytics",
                cards_using_table={
                    "456": CardDependencyInfo(
                        card_name="Card B",
                        card_type="native",
                        card_owner="user@test.com",
                        collection_path_names="Root",
                        collection_path_ids="1",
                    ),
                    "123": CardDependencyInfo(
                        card_name="Card A",
                        card_type="query_builder",
                        card_owner="user@test.com",
                        collection_path_names="Root",
                        collection_path_ids="1",
                    ),
                },
            ),
        }

        # When
        result = get_impacted_cards_from_cache("my_table", cache)

        # Then
        expected = [123, 456]
        assert result == expected

    def test_returns_empty_for_unknown_table(self) -> None:
        # Given
        cache: dict[str, TableDependency] = {
            "known_table": TableDependency(
                id=10,
                schema="analytics",
                cards_using_table={},
            ),
        }

        # When
        result = get_impacted_cards_from_cache("unknown_table", cache)

        # Then
        expected: list[int] = []
        assert result == expected

    def test_returns_empty_for_empty_cache(self) -> None:
        # Given
        cache: dict[str, TableDependency] = {}

        # When
        result = get_impacted_cards_from_cache("any_table", cache)

        # Then
        expected: list[int] = []
        assert result == expected
