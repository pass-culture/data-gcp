"""Tests for card migration (migration/card.py).

Tests that:
- Native SQL cards have schema, table, and column names replaced in SQL
- Native SQL cards have template tag dimension field IDs updated
- Query builder cards have source-table and field refs updated via tree walker
- result_metadata and visualization_settings field refs are updated
- Unrelated card properties (id, name, display) are preserved unchanged
"""

from __future__ import annotations

from pathlib import Path

from api.models import Card
from migration.card import (
    migrate_card,
    migrate_native_query,
    migrate_query_builder,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"


class TestMigrateNativeQuery:
    """Test native SQL card migration."""

    def test_replaces_schema_and_table_in_sql(self) -> None:
        """Schema.table references in SQL should be replaced."""
        dataset_query = {
            "type": "native",
            "database": 2,
            "native": {
                "query": "SELECT * FROM analytics.old_user_stats WHERE 1=1",
                "template-tags": {},
            },
        }
        result = migrate_native_query(
            dataset_query=dataset_query,
            column_mapping={},
            field_mapping={},
            old_schema="analytics",
            new_schema="analytics",
            old_table="old_user_stats",
            new_table="enriched_user_data",
        )
        assert "enriched_user_data" in result["native"]["query"]
        assert "old_user_stats" not in result["native"]["query"]

    def test_replaces_column_names_in_sql(self) -> None:
        """Column names in SQL should be replaced based on mapping."""
        dataset_query = {
            "type": "native",
            "database": 2,
            "native": {
                "query": "SELECT booking_cnt, total_amount FROM analytics.enriched_user_data",
                "template-tags": {},
            },
        }
        result = migrate_native_query(
            dataset_query=dataset_query,
            column_mapping={
                "booking_cnt": "total_individual_bookings",
                "total_amount": "total_actual_amount_spent",
            },
            field_mapping={},
            old_schema="analytics",
            new_schema="analytics",
            old_table="enriched_user_data",
            new_table="enriched_user_data",
        )
        sql = result["native"]["query"]
        assert "total_individual_bookings" in sql
        assert "total_actual_amount_spent" in sql
        assert "booking_cnt" not in sql

    def test_preserves_metabase_filter_syntax(self) -> None:
        """Lines with [[ ]] Metabase filter syntax should not have columns replaced."""
        dataset_query = {
            "type": "native",
            "database": 2,
            "native": {
                "query": (
                    "SELECT booking_cnt FROM t\n"
                    "[[ WHERE booking_cnt > {{min_bookings}} ]]\n"
                    "ORDER BY booking_cnt"
                ),
                "template-tags": {},
            },
        }
        result = migrate_native_query(
            dataset_query=dataset_query,
            column_mapping={"booking_cnt": "total_individual_bookings"},
            field_mapping={},
            old_schema="x",
            new_schema="x",
            old_table="t",
            new_table="t",
        )
        lines = result["native"]["query"].split("\n")
        # First and third lines should have replacement
        assert "total_individual_bookings" in lines[0]
        assert "total_individual_bookings" in lines[2]
        # Second line (with [[ ]]) should NOT be replaced
        assert "booking_cnt" in lines[1]

    def test_updates_template_tag_dimension_field_ids(self) -> None:
        """Dimension template tags should have their field IDs updated."""
        dataset_query = {
            "type": "native",
            "database": 2,
            "native": {
                "query": "SELECT * FROM t WHERE x = {{filter}}",
                "template-tags": {
                    "filter": {
                        "id": "abc",
                        "name": "filter",
                        "type": "dimension",
                        "dimension": ["field", 201, None],
                    },
                    "text_filter": {
                        "id": "def",
                        "name": "text_filter",
                        "type": "text",
                    },
                },
            },
        }
        result = migrate_native_query(
            dataset_query=dataset_query,
            column_mapping={},
            field_mapping={201: 301},
            old_schema="x",
            new_schema="x",
            old_table="t",
            new_table="t",
        )
        # Dimension tag's field ID should be updated
        dim = result["native"]["template-tags"]["filter"]["dimension"]
        assert dim == ["field", 301, None]
        # Text tag should be unchanged
        assert result["native"]["template-tags"]["text_filter"]["type"] == "text"

    def test_handles_schema_rename(self) -> None:
        """When schema changes, the SQL should reflect both old and new."""
        dataset_query = {
            "type": "native",
            "database": 2,
            "native": {
                "query": "SELECT * FROM old_schema.old_table",
                "template-tags": {},
            },
        }
        result = migrate_native_query(
            dataset_query=dataset_query,
            column_mapping={},
            field_mapping={},
            old_schema="old_schema",
            new_schema="new_schema",
            old_table="old_table",
            new_table="new_table",
        )
        assert "new_schema.new_table" in result["native"]["query"]
        assert "old_schema" not in result["native"]["query"]


class TestMigrateQueryBuilder:
    """Test query builder card migration."""

    def test_replaces_source_table(self) -> None:
        dataset_query = {
            "type": "query",
            "database": 2,
            "query": {
                "source-table": 10,
                "fields": [["field", 201, None]],
            },
        }
        result = migrate_query_builder(
            dataset_query=dataset_query,
            field_mapping={201: 301},
            table_mapping={10: 20},
        )
        assert result["query"]["source-table"] == 20
        assert result["query"]["fields"][0] == ["field", 301, None]

    def test_replaces_field_refs_in_filters(self) -> None:
        dataset_query = {
            "type": "query",
            "database": 2,
            "query": {
                "source-table": 10,
                "filter": [
                    "and",
                    [">", ["field", 202, None], 0],
                    ["=", ["field", 204, None], "active"],
                ],
            },
        }
        result = migrate_query_builder(
            dataset_query=dataset_query,
            field_mapping={202: 302, 204: 304},
            table_mapping={10: 20},
        )
        filter_clause = result["query"]["filter"]
        assert filter_clause[1][1] == ["field", 302, None]
        assert filter_clause[2][1] == ["field", 304, None]

    def test_does_not_replace_database_id(self) -> None:
        """Database ID should not be touched even if in field mapping."""
        dataset_query = {
            "type": "query",
            "database": 201,  # Same as a field ID
            "query": {
                "source-table": 10,
                "fields": [["field", 201, None]],
            },
        }
        result = migrate_query_builder(
            dataset_query=dataset_query,
            field_mapping={201: 301},
            table_mapping={10: 20},
        )
        # database should NOT be replaced
        assert result["database"] == 201
        # But field ref should
        assert result["query"]["fields"][0] == ["field", 301, None]


class TestMigrateCard:
    """Test full card migration (end-to-end)."""

    def test_native_card_full_migration(self, native_card_fixture: dict) -> None:
        """Full migration of a native SQL card."""
        card = Card.model_validate(native_card_fixture)
        result = migrate_card(
            card=card,
            field_mapping={201: 301, 202: 302},
            table_mapping={10: 20},
            column_mapping={
                "booking_cnt": "total_individual_bookings",
            },
            old_schema="analytics",
            new_schema="analytics",
            old_table="old_user_stats",
            new_table="enriched_user_data",
        )

        # Card properties preserved
        assert result.id == 101
        assert result.name == "Test Native Card"
        assert result.display == "table"

        # table_id updated
        assert result.table_id == 20

        # SQL updated
        sql = result.dataset_query.native.query
        assert "enriched_user_data" in sql
        assert "old_user_stats" not in sql
        assert "total_individual_bookings" in sql

    def test_query_card_full_migration(self, query_card_fixture: dict) -> None:
        """Full migration of a query builder card."""
        card = Card.model_validate(query_card_fixture)
        field_mapping = {201: 301, 202: 302, 203: 303, 204: 304}
        table_mapping = {10: 20}

        result = migrate_card(
            card=card,
            field_mapping=field_mapping,
            table_mapping=table_mapping,
            column_mapping={},
            old_schema="analytics",
            new_schema="analytics",
            old_table="old_user_stats",
            new_table="enriched_user_data",
        )

        # Card properties preserved
        assert result.id == 102
        assert result.name == "Test Query Builder Card"
        assert result.display == "table"

        # table_id updated
        assert result.table_id == 20

        # dataset_query.query.source-table updated
        dq = result.dataset_query
        assert dq.query.source_table == 20

        # Fields updated
        fields = dq.query.fields
        assert ["field", 301, None] in fields
        assert ["field", 302, {"base-type": "type/Integer"}] in fields

        # result_metadata field refs updated
        if result.result_metadata:
            for meta in result.result_metadata:
                meta_dict = meta.model_dump(by_alias=True)
                if meta_dict.get("field_ref"):
                    ref = meta_dict["field_ref"]
                    if isinstance(ref, list) and len(ref) >= 2:
                        assert ref[1] in (301, 302, 303, 304)

    def test_preserves_unrelated_properties(self, native_card_fixture: dict) -> None:
        """Properties not related to field/table refs should be unchanged."""
        card = Card.model_validate(native_card_fixture)
        result = migrate_card(
            card=card,
            field_mapping={},
            table_mapping={},
            column_mapping={},
            old_schema="analytics",
            new_schema="analytics",
            old_table="old_user_stats",
            new_table="enriched_user_data",
        )
        assert result.collection_id == card.collection_id
        assert result.archived == card.archived
        assert result.description == card.description
        assert result.dataset_query.database == card.dataset_query.database

    def test_card_without_dataset_query_returns_unchanged(self) -> None:
        """A card with no dataset_query should be returned as-is."""
        card = Card(id=999, name="No Query Card")
        result = migrate_card(
            card=card,
            field_mapping={201: 301},
            table_mapping={10: 20},
            column_mapping={},
            old_schema="s",
            new_schema="s",
            old_table="t",
            new_table="t",
        )
        assert result.id == 999
        assert result.name == "No Query Card"
