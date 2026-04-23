from __future__ import annotations

from migration.field_walker import replace_field_ids


class TestFieldRefReplacement:
    def test_replaces_field_ref_pmbql_with_empty_opts(self) -> None:
        # Given
        node = ["field", {}, 201]
        field_mapping = {201: 301}

        # When
        result = replace_field_ids(node, field_mapping)

        # Then
        expected = ["field", {}, 301]
        assert result == expected

    def test_replaces_field_ref_pmbql_with_uuid(self) -> None:
        # Given
        node = ["field", {"lib/uuid": "abc-123"}, 201]
        field_mapping = {201: 301}

        # When
        result = replace_field_ids(node, field_mapping)

        # Then
        expected = ["field", {"lib/uuid": "abc-123"}, 301]
        assert result == expected

    def test_replaces_field_ref_pmbql_with_extra_options(self) -> None:
        # Given
        node = ["field", {"lib/uuid": "def-456", "base-type": "type/Integer"}, 202]
        field_mapping = {202: 302}

        # When
        result = replace_field_ids(node, field_mapping)

        # Then
        expected = ["field", {"lib/uuid": "def-456", "base-type": "type/Integer"}, 302]
        assert result == expected

    def test_does_not_replace_unmapped_field(self) -> None:
        # Given
        node = ["field", {"lib/uuid": "xxx"}, 999]

        # When
        result = replace_field_ids(node, {201: 301})

        # Then
        expected = ["field", {"lib/uuid": "xxx"}, 999]
        assert result == expected

    def test_preserves_field_options_after_replacement(self) -> None:
        # Given
        node = ["field", {"lib/uuid": "abc", "join-alias": "Products"}, 201]
        field_mapping = {201: 301}

        # When
        result = replace_field_ids(node, field_mapping)

        # Then
        expected = ["field", {"lib/uuid": "abc", "join-alias": "Products"}, 301]
        assert result == expected


class TestSourceTableReplacement:
    def test_replaces_source_table(self) -> None:
        # Given
        node = {"source-table": 10}
        table_mapping = {10: 20}

        # When
        result = replace_field_ids(node, {}, table_mapping)

        # Then
        expected = {"source-table": 20}
        assert result == expected

    def test_does_not_replace_unmapped_source_table(self) -> None:
        # Given
        node = {"source-table": 999}

        # When
        result = replace_field_ids(node, {}, {10: 20})

        # Then
        expected = {"source-table": 999}
        assert result == expected

    def test_replaces_source_table_in_nested_joins(self) -> None:
        # Given
        node = {
            "source-table": 10,
            "joins": [
                {
                    "source-table": 10,
                    "condition": ["=", ["field", {"lib/uuid": "a"}, 201], ["field", {"lib/uuid": "b"}, 203]],
                }
            ],
        }
        field_mapping = {201: 301, 203: 303}
        table_mapping = {10: 20}

        # When
        result = replace_field_ids(node, field_mapping, table_mapping)

        # Then
        expected = {
            "source-table": 20,
            "joins": [
                {
                    "source-table": 20,
                    "condition": ["=", ["field", {"lib/uuid": "a"}, 301], ["field", {"lib/uuid": "b"}, 303]],
                }
            ],
        }
        assert result == expected


class TestForeignKeyRefs:
    def test_replaces_field_ids_inside_fk_refs(self) -> None:
        # Given
        node = ["fk->", ["field", {"lib/uuid": "a"}, 201], ["field", {"lib/uuid": "b"}, 203]]
        field_mapping = {201: 301, 203: 303}

        # When
        result = replace_field_ids(node, field_mapping)

        # Then
        expected = ["fk->", ["field", {"lib/uuid": "a"}, 301], ["field", {"lib/uuid": "b"}, 303]]
        assert result == expected


class TestNonFieldIntegersPreserved:
    def test_does_not_replace_card_id(self) -> None:
        # Given
        node = {
            "id": 201,
            "name": "My Card",
            "dataset_query": {
                "lib/type": "mbql/query",
                "stages": [
                    {"lib/type": "mbql.stage/mbql", "source-table": 10, "fields": [["field", {"lib/uuid": "a"}, 201]]}
                ],
            },
        }
        field_mapping = {201: 301}
        table_mapping = {10: 20}

        # When
        result = replace_field_ids(node, field_mapping, table_mapping)

        # Then
        expected = {
            "id": 201,
            "name": "My Card",
            "dataset_query": {
                "lib/type": "mbql/query",
                "stages": [
                    {"lib/type": "mbql.stage/mbql", "source-table": 20, "fields": [["field", {"lib/uuid": "a"}, 301]]}
                ],
            },
        }
        assert result == expected

    def test_does_not_replace_collection_id(self) -> None:
        # Given
        node = {"collection_id": 201, "table_id": 201}
        field_mapping = {201: 301}

        # When
        result = replace_field_ids(node, field_mapping)

        # Then
        expected = {"collection_id": 201, "table_id": 201}
        assert result == expected

    def test_does_not_replace_database_id(self) -> None:
        # Given
        node = {"database": 201, "lib/type": "mbql/query"}
        field_mapping = {201: 301}

        # When
        result = replace_field_ids(node, field_mapping)

        # Then
        expected = {"database": 201, "lib/type": "mbql/query"}
        assert result == expected

    def test_does_not_replace_visualization_settings_target_id(self) -> None:
        # Given
        node = {
            "click_behavior": {"type": "link", "linkType": "question", "targetId": 201},
            "graph.metrics": [["field", {"lib/uuid": "a"}, 201]],
        }
        field_mapping = {201: 301}

        # When
        result = replace_field_ids(node, field_mapping)

        # Then
        expected = {
            "click_behavior": {"type": "link", "linkType": "question", "targetId": 201},
            "graph.metrics": [["field", {"lib/uuid": "a"}, 301]],
        }
        assert result == expected

    def test_does_not_replace_string_containing_number(self) -> None:
        # Given
        node = {"query": "SELECT 201 FROM table"}
        field_mapping = {201: 301}

        # When
        result = replace_field_ids(node, field_mapping)

        # Then
        expected = {"query": "SELECT 201 FROM table"}
        assert result == expected

    def test_does_not_replace_limit_value(self) -> None:
        # Given
        node = {"limit": 201}
        field_mapping = {201: 301}

        # When
        result = replace_field_ids(node, field_mapping)

        # Then
        expected = {"limit": 201}
        assert result == expected


class TestNestedStructures:
    def test_nested_filter_with_multiple_fields(self) -> None:
        # Given
        node = [
            "and",
            [">", ["field", {"lib/uuid": "a"}, 202], 0],
            ["=", ["field", {"lib/uuid": "b"}, 204], "active"],
            ["or", ["<", ["field", {"lib/uuid": "c"}, 201], 100], ["is-null", ["field", {"lib/uuid": "d"}, 203]]],
        ]
        field_mapping = {201: 301, 202: 302, 203: 303, 204: 304}

        # When
        result = replace_field_ids(node, field_mapping)

        # Then
        expected = [
            "and",
            [">", ["field", {"lib/uuid": "a"}, 302], 0],
            ["=", ["field", {"lib/uuid": "b"}, 304], "active"],
            ["or", ["<", ["field", {"lib/uuid": "c"}, 301], 100], ["is-null", ["field", {"lib/uuid": "d"}, 303]]],
        ]
        assert result == expected

    def test_aggregation_with_field_refs(self) -> None:
        # Given
        node = [["sum", ["field", {"lib/uuid": "a"}, 203]], ["count"]]
        field_mapping = {203: 303}

        # When
        result = replace_field_ids(node, field_mapping)

        # Then
        expected = [["sum", ["field", {"lib/uuid": "a"}, 303]], ["count"]]
        assert result == expected

    def test_order_by_with_field_refs(self) -> None:
        # Given
        node = [["asc", ["field", {"lib/uuid": "a"}, 201]], ["desc", ["field", {"lib/uuid": "b"}, 202]]]
        field_mapping = {201: 301, 202: 302}

        # When
        result = replace_field_ids(node, field_mapping)

        # Then
        expected = [["asc", ["field", {"lib/uuid": "a"}, 301]], ["desc", ["field", {"lib/uuid": "b"}, 302]]]
        assert result == expected

    def test_expressions_with_field_refs(self) -> None:
        # Given
        node = {"expressions": {"double_amount": ["*", ["field", {"lib/uuid": "a"}, 203], 2]}}
        field_mapping = {203: 303}

        # When
        result = replace_field_ids(node, field_mapping)

        # Then
        expected = {"expressions": {"double_amount": ["*", ["field", {"lib/uuid": "a"}, 303], 2]}}
        assert result == expected

    def test_replaces_field_refs_in_result_metadata_but_not_plain_id(self) -> None:
        # Given
        node = [
            {"name": "user_id", "field_ref": ["field", {"lib/uuid": "a"}, 201], "id": 201},
            {"name": "booking_cnt", "field_ref": ["field", {"lib/uuid": "b"}, 202], "id": 202},
        ]
        field_mapping = {201: 301, 202: 302}

        # When
        result = replace_field_ids(node, field_mapping)

        # Then
        expected = [
            {"name": "user_id", "field_ref": ["field", {"lib/uuid": "a"}, 301], "id": 201},
            {"name": "booking_cnt", "field_ref": ["field", {"lib/uuid": "b"}, 302], "id": 202},
        ]
        assert result == expected


class TestEdgeCases:
    def test_empty_mapping_returns_input_unchanged(self) -> None:
        # Given
        node = {"source-table": 10, "fields": [["field", {"lib/uuid": "a"}, 201]]}

        # When
        result = replace_field_ids(node, {}, {})

        # Then
        expected = {"source-table": 10, "fields": [["field", {"lib/uuid": "a"}, 201]]}
        assert result == expected

    def test_none_input_returns_none(self) -> None:
        # Given
        node = None

        # When
        result = replace_field_ids(node, {201: 301})

        # Then
        assert result is None

    def test_string_input_returns_unchanged(self) -> None:
        # Given
        node = "some string"

        # When
        result = replace_field_ids(node, {201: 301})

        # Then
        expected = "some string"
        assert result == expected

    def test_boolean_input_returns_unchanged(self) -> None:
        # Given
        node = True

        # When
        result = replace_field_ids(node, {1: 2})

        # Then
        assert result is True

    def test_empty_list(self) -> None:
        # Given
        node: list = []

        # When
        result = replace_field_ids(node, {201: 301})

        # Then
        expected: list = []
        assert result == expected

    def test_empty_dict(self) -> None:
        # Given
        node: dict = {}

        # When
        result = replace_field_ids(node, {201: 301})

        # Then
        expected: dict = {}
        assert result == expected

    def test_does_not_mutate_input(self) -> None:
        # Given
        original = {"source-table": 10, "fields": [["field", {"lib/uuid": "a"}, 201]]}
        field_mapping = {201: 301}
        table_mapping = {10: 20}

        # When
        replace_field_ids(original, field_mapping, table_mapping)

        # Then
        expected = {"source-table": 10, "fields": [["field", {"lib/uuid": "a"}, 201]]}
        assert original == expected
