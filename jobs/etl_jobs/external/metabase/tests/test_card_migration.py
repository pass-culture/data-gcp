from __future__ import annotations

import logging

import pytest

from api.models import Card
from migration.card import (
    _migrate_result_metadata,
    _migrate_template_tags,
    _migrate_visualization_settings,
    _replace_sql_references,
    migrate_card,
    migrate_native_query,
    migrate_query_builder,
)
from tests.conftest import NATIVE_CARD_API_RESPONSE_DATA, QUERY_CARD_API_RESPONSE_DATA


class TestMigrateNativeQuery:
    def test_replaces_schema_and_table_in_sql(self) -> None:
        # Given
        dataset_query = {
            "lib/type": "mbql/query",
            "database": 2,
            "stages": [
                {
                    "lib/type": "mbql.stage/native",
                    "native": "SELECT * FROM analytics.old_user_stats WHERE 1=1",
                    "template-tags": {},
                }
            ],
        }

        # When
        result = migrate_native_query(
            dataset_query=dataset_query,
            column_mapping={},
            field_mapping={},
            old_schema="analytics",
            new_schema="analytics",
            old_table="old_user_stats",
            new_table="enriched_user_data",
        )

        # Then
        expected = {
            "lib/type": "mbql/query",
            "database": 2,
            "stages": [
                {
                    "lib/type": "mbql.stage/native",
                    "native": "SELECT * FROM analytics.enriched_user_data WHERE 1=1",
                    "template-tags": {},
                }
            ],
        }
        assert result == expected

    def test_replaces_column_names_in_sql(self) -> None:
        # Given
        dataset_query = {
            "lib/type": "mbql/query",
            "database": 2,
            "stages": [
                {
                    "lib/type": "mbql.stage/native",
                    "native": "SELECT booking_cnt, total_amount FROM analytics.enriched_user_data",
                    "template-tags": {},
                }
            ],
        }

        # When
        result = migrate_native_query(
            dataset_query=dataset_query,
            column_mapping={"booking_cnt": "total_individual_bookings", "total_amount": "total_actual_amount_spent"},
            field_mapping={},
            old_schema="analytics",
            new_schema="analytics",
            old_table="enriched_user_data",
            new_table="enriched_user_data",
        )

        # Then
        sql = result["stages"][0]["native"]
        assert "total_individual_bookings" in sql
        assert "total_actual_amount_spent" in sql
        assert "booking_cnt" not in sql

    def test_preserves_metabase_filter_syntax_lines_unchanged(self) -> None:
        # Given
        dataset_query = {
            "lib/type": "mbql/query",
            "database": 2,
            "stages": [
                {
                    "lib/type": "mbql.stage/native",
                    "native": (
                        "SELECT booking_cnt FROM t\n[[ WHERE booking_cnt > {{min_bookings}} ]]\nORDER BY booking_cnt"
                    ),
                    "template-tags": {},
                }
            ],
        }

        # When
        result = migrate_native_query(
            dataset_query=dataset_query,
            column_mapping={"booking_cnt": "total_individual_bookings"},
            field_mapping={},
            old_schema="x",
            new_schema="x",
            old_table="t",
            new_table="t",
        )

        # Then
        lines = result["stages"][0]["native"].split("\n")
        assert "total_individual_bookings" in lines[0]
        assert "total_individual_bookings" in lines[2]
        assert "booking_cnt" in lines[1]

    def test_updates_dimension_template_tag_field_ids(self) -> None:
        # Given
        dataset_query = {
            "lib/type": "mbql/query",
            "database": 2,
            "stages": [
                {
                    "lib/type": "mbql.stage/native",
                    "native": "SELECT * FROM t WHERE x = {{filter}}",
                    "template-tags": {
                        "filter": {
                            "id": "abc",
                            "name": "filter",
                            "type": "dimension",
                            "dimension": ["field", {"lib/uuid": "dim-uuid"}, 201],
                        },
                        "text_filter": {"id": "def", "name": "text_filter", "type": "text"},
                    },
                }
            ],
        }

        # When
        result = migrate_native_query(
            dataset_query=dataset_query,
            column_mapping={},
            field_mapping={201: 301},
            old_schema="x",
            new_schema="x",
            old_table="t",
            new_table="t",
        )

        # Then
        expected = {
            "filter": {
                "id": "abc",
                "name": "filter",
                "type": "dimension",
                "dimension": ["field", {"lib/uuid": "dim-uuid"}, 301],
            },
            "text_filter": {"id": "def", "name": "text_filter", "type": "text"},
        }
        assert result["stages"][0]["template-tags"] == expected

    def test_replaces_both_schema_and_table_when_schema_changes(self) -> None:
        # Given
        dataset_query = {
            "lib/type": "mbql/query",
            "database": 2,
            "stages": [
                {"lib/type": "mbql.stage/native", "native": "SELECT * FROM old_schema.old_table", "template-tags": {}}
            ],
        }

        # When
        result = migrate_native_query(
            dataset_query=dataset_query,
            column_mapping={},
            field_mapping={},
            old_schema="old_schema",
            new_schema="new_schema",
            old_table="old_table",
            new_table="new_table",
        )

        # Then
        expected = {
            "lib/type": "mbql/query",
            "database": 2,
            "stages": [
                {"lib/type": "mbql.stage/native", "native": "SELECT * FROM new_schema.new_table", "template-tags": {}}
            ],
        }
        assert result == expected


class TestMigrateQueryBuilder:
    def test_replaces_source_table(self) -> None:
        # Given
        dataset_query = {
            "lib/type": "mbql/query",
            "database": 2,
            "stages": [
                {"lib/type": "mbql.stage/mbql", "source-table": 10, "fields": [["field", {"lib/uuid": "a"}, 201]]}
            ],
        }

        # When
        result = migrate_query_builder(dataset_query=dataset_query, field_mapping={201: 301}, table_mapping={10: 20})

        # Then
        expected = {
            "lib/type": "mbql/query",
            "database": 2,
            "stages": [
                {"lib/type": "mbql.stage/mbql", "source-table": 20, "fields": [["field", {"lib/uuid": "a"}, 301]]}
            ],
        }
        assert result == expected

    def test_replaces_field_refs_in_filters(self) -> None:
        # Given
        dataset_query = {
            "lib/type": "mbql/query",
            "database": 2,
            "stages": [
                {
                    "lib/type": "mbql.stage/mbql",
                    "source-table": 10,
                    "filter": [
                        "and",
                        [">", ["field", {"lib/uuid": "a"}, 202], 0],
                        ["=", ["field", {"lib/uuid": "b"}, 204], "active"],
                    ],
                }
            ],
        }

        # When
        result = migrate_query_builder(
            dataset_query=dataset_query, field_mapping={202: 302, 204: 304}, table_mapping={10: 20}
        )

        # Then
        expected = {
            "lib/type": "mbql/query",
            "database": 2,
            "stages": [
                {
                    "lib/type": "mbql.stage/mbql",
                    "source-table": 20,
                    "filter": [
                        "and",
                        [">", ["field", {"lib/uuid": "a"}, 302], 0],
                        ["=", ["field", {"lib/uuid": "b"}, 304], "active"],
                    ],
                }
            ],
        }
        assert result == expected

    def test_does_not_replace_database_id(self) -> None:
        # Given
        dataset_query = {
            "lib/type": "mbql/query",
            "database": 201,
            "stages": [
                {"lib/type": "mbql.stage/mbql", "source-table": 10, "fields": [["field", {"lib/uuid": "a"}, 201]]}
            ],
        }

        # When
        result = migrate_query_builder(dataset_query=dataset_query, field_mapping={201: 301}, table_mapping={10: 20})

        # Then
        expected = {
            "lib/type": "mbql/query",
            "database": 201,
            "stages": [
                {"lib/type": "mbql.stage/mbql", "source-table": 20, "fields": [["field", {"lib/uuid": "a"}, 301]]}
            ],
        }
        assert result == expected


class TestMigrateCard:
    def test_native_card_full_migration(self) -> None:
        # Given
        card = Card.model_validate(NATIVE_CARD_API_RESPONSE_DATA)

        # When
        result = migrate_card(
            card=card,
            field_mapping={201: 301, 202: 302},
            table_mapping={10: 20},
            column_mapping={"booking_cnt": "total_individual_bookings"},
            old_schema="analytics",
            new_schema="analytics",
            old_table="old_user_stats",
            new_table="enriched_user_data",
        )

        # Then
        assert result.id == 101
        assert result.name == "Test Native Card"
        assert result.display == "table"
        assert result.table_id == 20
        assert result.dataset_query is not None
        assert result.dataset_query.stages is not None
        stage = result.dataset_query.stages[0]
        sql = stage["native"]
        assert "enriched_user_data" in sql
        assert "old_user_stats" not in sql
        assert "total_individual_bookings" in sql

    def test_query_builder_card_full_migration(self) -> None:
        # Given
        card = Card.model_validate(QUERY_CARD_API_RESPONSE_DATA)
        field_mapping = {201: 301, 202: 302, 203: 303, 204: 304}
        table_mapping = {10: 20}

        # When
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

        # Then
        assert result.id == 102
        assert result.name == "Test Query Builder Card"
        assert result.display == "table"
        assert result.table_id == 20
        dq = result.dataset_query
        assert dq is not None
        assert dq.stages is not None
        stage = dq.stages[0]
        assert stage["source-table"] == 20
        fields = stage["fields"]
        assert any(f[2] == 301 for f in fields if isinstance(f, list) and len(f) >= 3)
        assert any(f[2] == 302 for f in fields if isinstance(f, list) and len(f) >= 3)
        if result.result_metadata:
            for meta in result.result_metadata:
                meta_dict = meta.model_dump(by_alias=True)
                if meta_dict.get("field_ref"):
                    ref = meta_dict["field_ref"]
                    if isinstance(ref, list) and len(ref) >= 3:
                        assert ref[2] in (301, 302, 303, 304)

    def test_preserves_unrelated_card_properties(self) -> None:
        # Given
        card = Card.model_validate(NATIVE_CARD_API_RESPONSE_DATA)

        # When
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

        # Then
        assert result.collection_id == card.collection_id
        assert result.archived == card.archived
        assert result.description == card.description
        assert result.dataset_query is not None
        assert card.dataset_query is not None
        assert result.dataset_query.database == card.dataset_query.database

    def test_card_without_dataset_query_returns_unchanged(self) -> None:
        # Given
        card = Card(id=999, name="No Query Card")

        # When
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

        # Then
        assert result.id == 999
        assert result.name == "No Query Card"

    def test_unknown_stage_type_returns_card_unchanged(self) -> None:
        # Given
        card = Card.model_validate(
            {
                "id": 500,
                "name": "Pivot Card",
                "dataset_query": {
                    "lib/type": "mbql/query",
                    "database": 2,
                    "stages": [{"lib/type": "mbql.stage/pivot"}],
                },
                "table_id": 10,
            }
        )

        # When
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

        # Then
        assert result is card
        assert result.id == 500
        assert result.name == "Pivot Card"
        assert result.table_id == 10

    def test_card_without_stages_returns_unchanged(self) -> None:
        # Given
        card = Card.model_validate(
            {
                "id": 600,
                "name": "Empty Stages Card",
                "dataset_query": {"lib/type": "mbql/query", "database": 2, "stages": []},
                "table_id": 10,
            }
        )

        # When
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

        # Then
        assert result is card
        assert result.id == 600


class TestMigrateNativeQueryEdgeCases:
    def test_returns_unchanged_when_stages_empty(self) -> None:
        # Given
        dataset_query = {"lib/type": "mbql/query", "database": 2, "stages": []}

        # When
        result = migrate_native_query(
            dataset_query=dataset_query,
            column_mapping={"col_a": "col_b"},
            field_mapping={201: 301},
            old_schema="s",
            new_schema="s",
            old_table="t",
            new_table="t",
        )

        # Then
        expected = {"lib/type": "mbql/query", "database": 2, "stages": []}
        assert result == expected

    def test_returns_unchanged_when_native_sql_is_empty(self) -> None:
        # Given
        dataset_query = {"lib/type": "mbql/query", "database": 2, "stages": [{"lib/type": "mbql.stage/native"}]}

        # When
        result = migrate_native_query(
            dataset_query=dataset_query,
            column_mapping={"col_a": "col_b"},
            field_mapping={201: 301},
            old_schema="s",
            new_schema="s",
            old_table="t",
            new_table="t",
        )

        # Then
        assert result["stages"][0].get("native", "") == ""


class TestMigrateTemplateTags:
    def test_skips_non_dict_tag_values(self) -> None:
        # Given
        template_tags = {
            "valid_tag": {
                "id": "abc",
                "name": "valid_tag",
                "type": "dimension",
                "dimension": ["field", {"lib/uuid": "dim-uuid"}, 201],
            },
            "string_tag": "not_a_dict",
            "int_tag": 42,
            "list_tag": [1, 2, 3],
            "none_tag": None,
        }
        field_mapping = {201: 301}

        # When
        result = _migrate_template_tags(template_tags, field_mapping=field_mapping)

        # Then
        expected = {
            "valid_tag": {
                "id": "abc",
                "name": "valid_tag",
                "type": "dimension",
                "dimension": ["field", {"lib/uuid": "dim-uuid"}, 301],
            },
            "string_tag": "not_a_dict",
            "int_tag": 42,
            "list_tag": [1, 2, 3],
            "none_tag": None,
        }
        assert result == expected


class TestMigrateResultMetadata:
    def test_returns_none_when_input_is_none(self) -> None:
        # Given
        metadata = None
        field_mapping = {201: 301}
        table_mapping = {10: 20}

        # When
        result = _migrate_result_metadata(metadata, field_mapping=field_mapping, table_mapping=table_mapping)

        # Then
        assert result is None

    def test_returns_empty_list_when_input_is_empty(self) -> None:
        # Given
        metadata: list = []
        field_mapping = {201: 301}
        table_mapping = {10: 20}

        # When
        result = _migrate_result_metadata(metadata, field_mapping=field_mapping, table_mapping=table_mapping)

        # Then
        expected: list = []
        assert result == expected


class TestMigrateVisualizationSettings:
    def test_returns_none_when_input_is_none(self) -> None:
        # Given
        settings = None
        field_mapping = {201: 301}
        table_mapping = {10: 20}

        # When
        result = _migrate_visualization_settings(settings, field_mapping=field_mapping, table_mapping=table_mapping)

        # Then
        assert result is None

    def test_returns_empty_dict_when_input_is_empty(self) -> None:
        # Given
        settings: dict = {}
        field_mapping = {201: 301}
        table_mapping = {10: 20}

        # When
        result = _migrate_visualization_settings(settings, field_mapping=field_mapping, table_mapping=table_mapping)

        # Then
        expected: dict = {}
        assert result == expected


class TestSchemaAwareSQLReplacement:
    def test_does_not_replace_table_in_different_schema(self) -> None:
        # Given
        sql = """
    SELECT p.* FROM raw_data.venue_provider p
    JOIN analytics_stg.venue_provider a ON p.id = a.venue_id
    """

        # When
        result = _replace_sql_references(
            query=sql,
            column_mapping={},
            old_schema="analytics_stg",
            new_schema="analytics",
            old_table="venue_provider",
            new_table="venue_provider",
        )

        # Then
        expected = """
    SELECT p.* FROM raw_data.venue_provider p
    JOIN analytics.venue_provider a ON p.id = a.venue_id
    """
        assert result.strip() == expected.strip()

    def test_replaces_qualified_table_with_backticks(self) -> None:
        # Given
        sql = "FROM `analytics_stg`.`venue_provider`"

        # When
        result = _replace_sql_references(
            query=sql,
            column_mapping={},
            old_schema="analytics_stg",
            new_schema="analytics",
            old_table="venue_provider",
            new_table="venue_provider",
        )

        # Then
        expected = "FROM analytics.venue_provider"
        assert expected in result

    def test_schema_prefix_exact_match(self) -> None:
        # Given
        sql = "FROM my_analytics_stg.venue_provider"

        # When
        result = _replace_sql_references(
            query=sql,
            column_mapping={},
            old_schema="analytics_stg",
            new_schema="analytics",
            old_table="venue_provider",
            new_table="venue_provider",
        )

        # Then
        assert "my_analytics_stg.venue_provider" in result

    def test_multi_schema_join_preserves_other_schemas(self) -> None:
        # Given
        sql = """
    FROM analytics_stg.venue_provider stg
    JOIN raw_data.venue_provider raw ON stg.id = raw.id
    JOIN analytics_prod.venue_provider prod ON stg.id = prod.id
    """

        # When
        result = _replace_sql_references(
            query=sql,
            column_mapping={},
            old_schema="analytics_stg",
            new_schema="analytics",
            old_table="venue_provider",
            new_table="venue_provider",
        )

        # Then
        assert "analytics.venue_provider stg" in result
        assert "raw_data.venue_provider raw" in result
        assert "analytics_prod.venue_provider prod" in result

    def test_unqualified_table_replaced_with_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        # Given
        sql = "SELECT * FROM venue_provider"

        # When
        with caplog.at_level(logging.WARNING):
            result = _replace_sql_references(
                query=sql,
                column_mapping={},
                old_schema="analytics_stg",
                new_schema="analytics",
                old_table="venue_provider",
                new_table="venue_provider",
            )

        # Then
        assert "FROM venue_provider" in result
        assert "unqualified reference" in caplog.text.lower()

    def test_preserves_backticks_around_full_qualified_name_with_column_reference(self) -> None:
        # Given
        sql = """
    JOIN `analytics_stg.region_department`
    ON `analytics_stg.global_collective_offer`.venue_department_code = `analytics_stg.region_department`.num_dep
    """

        # When
        result = _replace_sql_references(
            query=sql,
            column_mapping={},
            old_schema="analytics_stg",
            new_schema="analytics_stg",
            old_table="region_department",
            new_table="region_department_v2",
        )

        # Then
        expected = """
    JOIN `analytics_stg.region_department_v2`
    ON `analytics_stg.global_collective_offer`.venue_department_code = `analytics_stg.region_department_v2`.num_dep
    """
        assert result.strip() == expected.strip()

    def test_preserves_backticks_around_full_qualified_name_simple(self) -> None:
        # Given
        sql = "FROM `analytics_stg.region_department`"

        # When
        result = _replace_sql_references(
            query=sql,
            column_mapping={},
            old_schema="analytics_stg",
            new_schema="analytics_stg",
            old_table="region_department",
            new_table="region_department_v2",
        )

        # Then
        expected = "FROM `analytics_stg.region_department_v2`"
        assert expected in result

    def test_handles_mixed_backtick_styles(self) -> None:
        # Given
        sql = """
    SELECT *
    FROM `analytics_stg.region_department` t1
    JOIN `analytics_stg`.`global_offer` t2 ON t1.id = t2.dept_id
    """

        # When
        result = _replace_sql_references(
            query=sql,
            column_mapping={},
            old_schema="analytics_stg",
            new_schema="analytics_stg",
            old_table="region_department",
            new_table="region_department_v2",
        )

        # Then
        assert "`analytics_stg.region_department_v2`" in result
        assert "`analytics_stg`.`global_offer`" in result


class TestSchemaAwareDiscovery:
    def test_discovery_distinguishes_schemas(self) -> None:
        # Given
        from discovery.metabase import _sql_references_table

        sql = "FROM raw_data.venue_provider JOIN analytics_stg.venue_provider"

        # When / Then
        assert _sql_references_table(sql, "venue_provider", "analytics_stg")
        assert _sql_references_table(sql, "venue_provider", "raw_data")
        assert not _sql_references_table(sql, "venue_provider", "production")

