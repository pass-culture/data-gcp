from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

from api.client import MetabaseClient


def _make_client() -> MetabaseClient:
    return MetabaseClient(host="http://fake-metabase:3000", session_token="fake-token")


class TestFetchCollection:
    def test_returns_collection_dict(self) -> None:
        # Given
        client = _make_client()
        mock_response = MagicMock()
        mock_response.json.return_value = {"id": 5, "name": "Analytics", "location": "/1/"}
        mock_response.raise_for_status = MagicMock()
        client.session.get = MagicMock(return_value=mock_response)  # type: ignore[method-assign]

        # When
        result = client.fetch_collection(5)

        # Then
        expected = {"id": 5, "name": "Analytics", "location": "/1/"}
        assert result == expected
        client.session.get.assert_called_once_with("http://fake-metabase:3000/api/collection/5")


class TestResolveCollectionPath:
    def test_returns_empty_for_none_collection(self) -> None:
        # Given
        client = _make_client()
        collection_cache: dict[int, tuple[str, str]] = {}

        # When
        result = client.resolve_collection_path(None, collection_cache)

        # Then
        expected = ("", "")
        assert result == expected

    def test_resolves_root_level_collection(self) -> None:
        # Given
        client = _make_client()
        collection_cache: dict[int, tuple[str, str]] = {}

        def mock_fetch_collection(cid: int) -> dict[str, Any]:
            collections = {
                1: {"id": 1, "name": "Root", "location": "/"},
            }
            return collections[cid]

        client.fetch_collection = MagicMock(side_effect=mock_fetch_collection)  # type: ignore[method-assign]

        # When
        result = client.resolve_collection_path(1, collection_cache)

        # Then
        expected = ("Root", "1")
        assert result == expected

    def test_resolves_nested_collection(self) -> None:
        # Given
        client = _make_client()
        collection_cache: dict[int, tuple[str, str]] = {}

        def mock_fetch_collection(cid: int) -> dict[str, Any]:
            collections = {
                1: {"id": 1, "name": "Root", "location": "/"},
                5: {"id": 5, "name": "Analytics", "location": "/1/"},
                12: {"id": 12, "name": "Dashboards", "location": "/1/5/"},
            }
            return collections[cid]

        client.fetch_collection = MagicMock(side_effect=mock_fetch_collection)  # type: ignore[method-assign]

        # When
        result = client.resolve_collection_path(12, collection_cache)

        # Then
        expected = ("Root/Analytics/Dashboards", "1/5/12")
        assert result == expected

    def test_returns_cached_result(self) -> None:
        # Given
        client = _make_client()
        collection_cache: dict[int, tuple[str, str]] = {
            5: ("Root/Analytics", "1/5"),
        }
        client.fetch_collection = MagicMock()  # type: ignore[method-assign]

        # When
        result = client.resolve_collection_path(5, collection_cache)

        # Then
        expected = ("Root/Analytics", "1/5")
        assert result == expected
        client.fetch_collection.assert_not_called()

    def test_caches_ancestors_opportunistically(self) -> None:
        # Given
        client = _make_client()
        collection_cache: dict[int, tuple[str, str]] = {}

        def mock_fetch_collection(cid: int) -> dict[str, Any]:
            collections = {
                1: {"id": 1, "name": "Root", "location": "/"},
                5: {"id": 5, "name": "Analytics", "location": "/1/"},
                12: {"id": 12, "name": "Dashboards", "location": "/1/5/"},
            }
            return collections[cid]

        client.fetch_collection = MagicMock(side_effect=mock_fetch_collection)  # type: ignore[method-assign]

        # When
        client.resolve_collection_path(12, collection_cache)
        result = client.resolve_collection_path(5, collection_cache)

        # Then
        expected = ("Root/Analytics", "1/5")
        assert result == expected
        # The second call should not make any API calls
        assert client.fetch_collection.call_count == 3  # Only 3 calls from the first resolution


class TestFetchAllCards:
    def test_returns_list_of_card_dicts(self) -> None:
        # Given
        client = _make_client()
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {"id": 1, "name": "Card A"},
            {"id": 2, "name": "Card B"},
        ]
        mock_response.raise_for_status = MagicMock()
        client.session.get = MagicMock(return_value=mock_response)  # type: ignore[method-assign]

        # When
        result = client.fetch_all_cards()

        # Then
        expected = [
            {"id": 1, "name": "Card A"},
            {"id": 2, "name": "Card B"},
        ]
        assert result == expected
        client.session.get.assert_called_once_with("http://fake-metabase:3000/api/card")


class TestBuildTableCatalogReturnValue:
    def test_returns_catalog_dict(self, tmp_path: MagicMock) -> None:
        # Given
        client = _make_client()
        client.fetch_database_tables = MagicMock(  # type: ignore[method-assign]
            return_value=[
                {"id": 100, "name": "table_a", "schema": "schema_1", "active": True},
                {"id": 200, "name": "table_b", "schema": "schema_2", "active": True},
            ]
        )
        cache_path = tmp_path / "cache_database_tables.json"

        # When
        with patch("api.client.DATABASE_TABLES_CACHE_PATH", cache_path):
            result = client.build_table_catalog(database_id=1)

        # Then
        expected = {
            "schema_1": [{"id": 100, "name": "table_a", "active": True}],
            "schema_2": [{"id": 200, "name": "table_b", "active": True}],
        }
        assert result == expected
