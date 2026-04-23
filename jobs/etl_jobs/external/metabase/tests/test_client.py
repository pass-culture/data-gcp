from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from api.client import MetabaseClient


def _make_client() -> MetabaseClient:
    return MetabaseClient(host="http://fake-metabase:3000", session_token="fake-token")


class TestFindDatabaseId:
    def test_returns_id_when_found(self) -> None:
        # Given
        client = _make_client()
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "data": [
                {"id": 1, "name": "Sample Database"},
                {"id": 2, "name": "BigQuery Prod"},
            ]
        }
        mock_response.raise_for_status = MagicMock()
        client.session.get = MagicMock(return_value=mock_response)  # type: ignore[method-assign]

        # When
        result = client.find_database_id("BigQuery Prod")

        # Then
        expected = 2
        assert result == expected

    def test_returns_none_when_not_found(self) -> None:
        # Given
        client = _make_client()
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "data": [
                {"id": 1, "name": "Sample Database"},
            ]
        }
        mock_response.raise_for_status = MagicMock()
        client.session.get = MagicMock(return_value=mock_response)  # type: ignore[method-assign]

        # When
        result = client.find_database_id("Nonexistent DB")

        # Then
        assert result is None


class TestBuildTableCatalog:
    def test_creates_cache_file(self, tmp_path: Path) -> None:
        # Given
        client = _make_client()
        client.fetch_database_tables = MagicMock(  # type: ignore[method-assign]
            return_value=[
                {"id": 100, "name": "table_a", "schema": "schema_1", "active": True},
                {"id": 200, "name": "table_b", "schema": "schema_1", "active": True},
                {"id": 300, "name": "table_c", "schema": "schema_2", "active": True},
            ]
        )
        cache_path = tmp_path / "cache_database_tables.json"

        # When
        with patch("api.client.DATABASE_TABLES_CACHE_PATH", cache_path):
            client.build_table_catalog(database_id=1)

        # Then
        expected = {
            "schema_1": [
                {"id": 100, "name": "table_a", "active": True},
                {"id": 200, "name": "table_b", "active": True},
            ],
            "schema_2": [
                {"id": 300, "name": "table_c", "active": True},
            ],
        }
        assert json.loads(cache_path.read_text()) == expected

    def test_includes_inactive_tables(self, tmp_path: Path) -> None:
        # Given
        client = _make_client()
        client.fetch_database_tables = MagicMock(  # type: ignore[method-assign]
            return_value=[
                {"id": 100, "name": "active_table", "schema": "my_schema", "active": True},
                {"id": 200, "name": "deleted_table", "schema": "my_schema", "active": False},
            ]
        )
        cache_path = tmp_path / "cache_database_tables.json"

        # When
        with patch("api.client.DATABASE_TABLES_CACHE_PATH", cache_path):
            client.build_table_catalog(database_id=1)

        # Then
        expected = {
            "my_schema": [
                {"id": 100, "name": "active_table", "active": True},
                {"id": 200, "name": "deleted_table", "active": False},
            ],
        }
        assert json.loads(cache_path.read_text()) == expected


class TestFindTableId:
    def test_finds_active_table_from_cache(self, tmp_path: Path) -> None:
        # Given
        client = _make_client()
        cache_path = tmp_path / "cache_database_tables.json"
        catalog = {
            "my_schema": [
                {"id": 100, "name": "active_table", "active": True},
                {"id": 200, "name": "other_table", "active": True},
            ],
        }
        cache_path.write_text(json.dumps(catalog))

        # When
        with patch("api.client.DATABASE_TABLES_CACHE_PATH", cache_path):
            result = client.find_table_id("active_table", "my_schema")

        # Then
        expected = 100
        assert result == expected

    def test_finds_inactive_table_from_cache(self, tmp_path: Path) -> None:
        # Given
        client = _make_client()
        cache_path = tmp_path / "cache_database_tables.json"
        catalog = {
            "my_schema": [
                {"id": 100, "name": "active_table", "active": True},
                {"id": 200, "name": "deleted_table", "active": False},
            ],
        }
        cache_path.write_text(json.dumps(catalog))

        # When
        with patch("api.client.DATABASE_TABLES_CACHE_PATH", cache_path):
            result = client.find_table_id("deleted_table", "my_schema")

        # Then
        expected = 200
        assert result == expected

    def test_returns_none_when_not_in_cache(self, tmp_path: Path) -> None:
        # Given
        client = _make_client()
        cache_path = tmp_path / "cache_database_tables.json"
        catalog = {
            "my_schema": [
                {"id": 100, "name": "active_table", "active": True},
            ],
        }
        cache_path.write_text(json.dumps(catalog))

        # When
        with patch("api.client.DATABASE_TABLES_CACHE_PATH", cache_path):
            result = client.find_table_id("nonexistent_table", "my_schema")

        # Then
        assert result is None

    def test_raises_when_no_cache(self, tmp_path: Path) -> None:
        # Given
        client = _make_client()
        cache_path = tmp_path / "nonexistent_cache.json"

        # When / Then
        import pytest

        with patch("api.client.DATABASE_TABLES_CACHE_PATH", cache_path):
            with pytest.raises(FileNotFoundError, match="Table catalog not found"):
                client.find_table_id("any_table", "any_schema")
