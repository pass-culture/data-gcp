import json
from unittest.mock import patch

import pandas as pd

from domain.dependencies import (
    get_card_lists,
    get_native_dependencies,
    get_query_dependencies,
    get_table_infos,
    run_dependencies,
)


class TestGetCardLists:
    def test_separates_native_and_query(self, metabase):
        metabase.get_cards.return_value = [
            {
                "id": 1,
                "query_type": "native",
                "legacy_query": json.dumps({"native": {"query": "SELECT 1"}}),
            },
            {
                "id": 2,
                "query_type": "query",
                "legacy_query": json.dumps({"query": {"source-table": 1}}),
            },
            {
                "id": 3,
                "query_type": "native",
                "legacy_query": None,
            },
        ]

        native, query = get_card_lists(metabase)
        assert len(native) == 2
        assert len(query) == 1
        # First native card should have parsed legacy_query
        assert native[0]["legacy_query"] == {"native": {"query": "SELECT 1"}}
        # Third card has None legacy_query, still classified as native
        assert native[1]["legacy_query"] is None


class TestGetTableInfos:
    def test_returns_dataframe(self, metabase):
        metabase.get_table.return_value = [
            {"id": 1, "schema": "public", "name": "users"},
            {"id": 2, "schema": "public", "name": "orders"},
        ]

        result = get_table_infos(metabase)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert list(result.columns) == ["table_id", "table_schema", "table_name"]


class TestGetQueryDependencies:
    def test_source_table(self):
        tables_df = pd.DataFrame(
            {"table_id": [10], "table_schema": ["public"], "table_name": ["users"]}
        )
        cards = [
            {
                "id": 1,
                "name": "Card 1",
                "creator": {"email": "u@t.com"},
                "query_type": "query",
                "legacy_query": {"query": {"source-table": 10}},
            }
        ]
        result = get_query_dependencies(cards, tables_df)
        assert len(result) == 1
        assert result.iloc[0]["table_id"] == 10

    def test_source_table_with_joins(self):
        tables_df = pd.DataFrame(
            {"table_id": [10, 20], "table_schema": ["p", "p"], "table_name": ["a", "b"]}
        )
        cards = [
            {
                "id": 1,
                "name": "Card 1",
                "creator": {"email": "u@t.com"},
                "query_type": "query",
                "legacy_query": {
                    "query": {
                        "source-table": 10,
                        "joins": [{"source-table": 20}],
                    }
                },
            }
        ]
        result = get_query_dependencies(cards, tables_df)
        assert len(result) == 2

    def test_source_query_with_source_table(self):
        tables_df = pd.DataFrame(
            {"table_id": [30], "table_schema": ["p"], "table_name": ["c"]}
        )
        cards = [
            {
                "id": 1,
                "name": "Card 1",
                "creator": {"email": "u@t.com"},
                "query_type": "query",
                "legacy_query": {
                    "query": {
                        "source-query": {"source-table": 30},
                    }
                },
            }
        ]
        result = get_query_dependencies(cards, tables_df)
        assert len(result) == 1
        assert result.iloc[0]["table_id"] == 30

    def test_source_query_with_joins(self):
        tables_df = pd.DataFrame(
            {"table_id": [30, 40], "table_schema": ["p", "p"], "table_name": ["c", "d"]}
        )
        cards = [
            {
                "id": 1,
                "name": "Card 1",
                "creator": {"email": "u@t.com"},
                "query_type": "query",
                "legacy_query": {
                    "query": {
                        "source-query": {"source-table": 30},
                        "joins": [{"source-table": 40}],
                    }
                },
            }
        ]
        result = get_query_dependencies(cards, tables_df)
        assert len(result) == 2

    def test_no_legacy_query_skipped(self):
        tables_df = pd.DataFrame(
            {"table_id": [10], "table_schema": ["p"], "table_name": ["a"]}
        )
        cards = [
            {
                "id": 1,
                "name": "Card 1",
                "creator": {"email": "u@t.com"},
                "query_type": "query",
                "legacy_query": None,
            },
            {
                "id": 2,
                "name": "Card 2",
                "creator": {"email": "u@t.com"},
                "query_type": "query",
                "legacy_query": {"query": {"source-table": 10}},
            },
        ]
        result = get_query_dependencies(cards, tables_df)
        # Only card 2 should appear (card 1 skipped)
        assert len(result) == 1
        assert result.iloc[0]["card_id"] == 2

    def test_no_source_table_nor_source_query(self):
        tables_df = pd.DataFrame(
            {"table_id": [10], "table_schema": ["p"], "table_name": ["a"]}
        )
        cards = [
            {
                "id": 1,
                "name": "Card 1",
                "creator": {"email": "u@t.com"},
                "query_type": "query",
                "legacy_query": {"query": {"filter": ["=", 1]}},
            }
        ]
        result = get_query_dependencies(cards, tables_df)
        assert len(result) == 1
        # table_id should be empty list exploded
        assert result.iloc[0]["card_id"] == 1


class TestGetNativeDependencies:
    def test_extracts_from_sql(self):
        tables_df = pd.DataFrame(
            {"table_id": [1], "table_schema": ["analytics"], "table_name": ["users"]}
        )
        cards = [
            {
                "id": 1,
                "name": "Card 1",
                "creator": {"email": "u@t.com"},
                "query_type": "native",
                "legacy_query": {"native": {"query": "SELECT * FROM analytics.users"}},
            }
        ]
        result = get_native_dependencies(cards, tables_df)
        assert len(result) == 1
        assert result.iloc[0]["table_name"] == "users"
        assert result.iloc[0]["table_schema"] == "analytics"

    def test_extracts_join(self):
        tables_df = pd.DataFrame(
            {
                "table_id": [1, 2],
                "table_schema": ["a", "b"],
                "table_name": ["users", "orders"],
            }
        )
        cards = [
            {
                "id": 1,
                "name": "Card 1",
                "creator": {"email": "u@t.com"},
                "query_type": "native",
                "legacy_query": {
                    "native": {"query": "SELECT * FROM a.users JOIN b.orders ON 1=1"}
                },
            }
        ]
        result = get_native_dependencies(cards, tables_df)
        assert len(result) == 2

    def test_no_legacy_query_skipped(self):
        tables_df = pd.DataFrame(
            {"table_id": [1], "table_schema": ["a"], "table_name": ["users"]}
        )
        cards = [
            {
                "id": 1,
                "name": "Card 1",
                "creator": {"email": "u@t.com"},
                "query_type": "native",
                "legacy_query": None,
            },
            {
                "id": 2,
                "name": "Card 2",
                "creator": {"email": "u@t.com"},
                "query_type": "native",
                "legacy_query": {"native": {"query": "SELECT * FROM a.users"}},
            },
        ]
        result = get_native_dependencies(cards, tables_df)
        # Only card 2 should appear (card 1 skipped)
        assert len(result) == 1
        assert result.iloc[0]["card_id"] == 2

    def test_removes_backticks(self):
        tables_df = pd.DataFrame(
            {"table_id": [1], "table_schema": ["ds"], "table_name": ["t"]}
        )
        cards = [
            {
                "id": 1,
                "name": "Card 1",
                "creator": {"email": "u@t.com"},
                "query_type": "native",
                "legacy_query": {"native": {"query": "SELECT * FROM `ds`.`t`"}},
            }
        ]
        result = get_native_dependencies(cards, tables_df)
        assert len(result) == 1
        assert result.iloc[0]["table_name"] == "t"


class TestRunDependencies:
    @patch("domain.dependencies.pd.DataFrame.to_gbq")
    def test_run_dependencies(self, mock_to_gbq, metabase):
        metabase.get_table.return_value = [
            {"id": 10, "schema": "public", "name": "users"}
        ]
        metabase.get_cards.return_value = [
            {
                "id": 1,
                "query_type": "native",
                "legacy_query": json.dumps(
                    {"native": {"query": "SELECT * FROM public.users"}}
                ),
                "name": "Card 1",
                "creator": {"email": "u@t.com"},
            },
            {
                "id": 2,
                "query_type": "query",
                "legacy_query": json.dumps({"query": {"source-table": 10}}),
                "name": "Card 2",
                "creator": {"email": "u@t.com"},
            },
        ]

        result = run_dependencies(metabase)
        assert result == "success"
        mock_to_gbq.assert_called_once()
        call_args = mock_to_gbq.call_args
        assert "int_metabase_dev" in call_args[0][0]
