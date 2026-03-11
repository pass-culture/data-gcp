"""Tests for the MBQL tree walker (migration/field_walker.py).

This is the most critical test module. The tree walker must:
1. Correctly replace ["field", id, ...] refs in nested MBQL structures
2. Correctly replace "source-table" values
3. NOT replace card.id, visualization_settings.card_id, or other non-field integers
4. Handle ["fk->", ...] refs
5. Handle MBQL v5 format (["field", id, null] with 3-element arrays)
"""

from __future__ import annotations

from migration.field_walker import replace_field_ids


class TestFieldRefReplacement:
    """Test replacement of ["field", id, ...] references."""

    def test_replaces_field_ref_v5_with_null(
        self, field_mapping: dict[int, int]
    ) -> None:
        """MBQL v5: ["field", 201, null] → ["field", 301, null]."""
        node = ["field", 201, None]
        result = replace_field_ids(node, field_mapping)
        assert result == ["field", 301, None]

    def test_replaces_field_ref_v5_with_options(
        self, field_mapping: dict[int, int]
    ) -> None:
        """MBQL v5: ["field", 202, {"base-type": "..."}] → ["field", 302, {...}]."""
        node = ["field", 202, {"base-type": "type/Integer"}]
        result = replace_field_ids(node, field_mapping)
        assert result == ["field", 302, {"base-type": "type/Integer"}]

    def test_replaces_field_ref_v4_two_elements(
        self, field_mapping: dict[int, int]
    ) -> None:
        """MBQL v4 compatibility: ["field", 201] → ["field", 301]."""
        node = ["field", 201]
        result = replace_field_ids(node, field_mapping)
        assert result == ["field", 301]

    def test_does_not_replace_unmapped_field(self) -> None:
        """Fields not in the mapping should remain unchanged."""
        node = ["field", 999, None]
        result = replace_field_ids(node, {201: 301})
        assert result == ["field", 999, None]

    def test_preserves_field_options(self, field_mapping: dict[int, int]) -> None:
        """Field options dict should be preserved after replacement."""
        node = [
            "field",
            201,
            {"source-field": 202, "join-alias": "Products"},
        ]
        result = replace_field_ids(node, field_mapping)
        # source-field inside options is NOT a top-level source-table
        # but if it's a nested field ref structure, the walker should handle it
        assert result[0] == "field"
        assert result[1] == 301  # Field ID replaced


class TestSourceTableReplacement:
    """Test replacement of {"source-table": id} references."""

    def test_replaces_source_table(self, table_mapping: dict[int, int]) -> None:
        node = {"source-table": 10}
        result = replace_field_ids(node, {}, table_mapping)
        assert result == {"source-table": 20}

    def test_does_not_replace_unmapped_source_table(self) -> None:
        node = {"source-table": 999}
        result = replace_field_ids(node, {}, {10: 20})
        assert result == {"source-table": 999}

    def test_replaces_source_table_in_nested_query(
        self,
        field_mapping: dict[int, int],
        table_mapping: dict[int, int],
    ) -> None:
        """source-table deep inside a joins structure should be replaced."""
        node = {
            "source-table": 10,
            "joins": [
                {
                    "source-table": 10,
                    "condition": [
                        "=",
                        ["field", 201, None],
                        ["field", 203, None],
                    ],
                }
            ],
        }
        result = replace_field_ids(node, field_mapping, table_mapping)
        assert result["source-table"] == 20
        assert result["joins"][0]["source-table"] == 20
        assert result["joins"][0]["condition"][1] == ["field", 301, None]
        assert result["joins"][0]["condition"][2] == ["field", 303, None]


class TestForeignKeyRefs:
    """Test replacement in ["fk->", ...] references."""

    def test_replaces_fk_ref_field_ids(self, field_mapping: dict[int, int]) -> None:
        """FK refs contain nested field refs that should be replaced."""
        node = ["fk->", ["field", 201, None], ["field", 203, None]]
        result = replace_field_ids(node, field_mapping)
        assert result == [
            "fk->",
            ["field", 301, None],
            ["field", 303, None],
        ]


class TestNonFieldIntegersPreserved:
    """Critical business rule: non-field integers must NOT be replaced.

    This is what distinguishes the tree walker from the broken regex approach.
    The old code did re.sub(r'\\b42\\b', '99') on the entire JSON, which would
    silently corrupt card_id, dashboard_id, etc.
    """

    def test_card_id_not_replaced(
        self, field_mapping: dict[int, int], table_mapping: dict[int, int]
    ) -> None:
        """Top-level 'id' key should never be touched."""
        node = {
            "id": 201,  # Same value as a field ID, but is a card ID
            "name": "My Card",
            "dataset_query": {
                "type": "query",
                "query": {
                    "source-table": 10,
                    "fields": [["field", 201, None]],
                },
            },
        }
        result = replace_field_ids(node, field_mapping, table_mapping)
        assert result["id"] == 201  # NOT replaced!
        assert result["name"] == "My Card"  # Unchanged
        assert result["dataset_query"]["query"]["source-table"] == 20
        assert result["dataset_query"]["query"]["fields"][0] == [
            "field",
            301,
            None,
        ]

    def test_collection_id_not_replaced(self, field_mapping: dict[int, int]) -> None:
        """collection_id should never be touched even if same value as field ID."""
        node = {"collection_id": 201, "table_id": 201}
        result = replace_field_ids(node, field_mapping)
        assert result["collection_id"] == 201
        assert result["table_id"] == 201  # Not in table_mapping

    def test_database_id_not_replaced(self, field_mapping: dict[int, int]) -> None:
        """database key should not be touched."""
        node = {"database": 201, "type": "query"}
        result = replace_field_ids(node, field_mapping)
        assert result["database"] == 201

    def test_visualization_settings_card_id_not_replaced(
        self, field_mapping: dict[int, int]
    ) -> None:
        """Card ID in visualization_settings click behavior should not be replaced."""
        node = {
            "click_behavior": {
                "type": "link",
                "linkType": "question",
                "targetId": 201,
            },
            "graph.metrics": [["field", 201, None]],
        }
        result = replace_field_ids(node, field_mapping)
        # targetId is a plain int, not in MBQL context — NOT replaced
        assert result["click_behavior"]["targetId"] == 201
        # But field ref IS replaced
        assert result["graph.metrics"][0] == ["field", 301, None]

    def test_string_number_not_replaced(self, field_mapping: dict[int, int]) -> None:
        """String values should never be modified."""
        node = {"query": "SELECT 201 FROM table"}
        result = replace_field_ids(node, field_mapping)
        assert result["query"] == "SELECT 201 FROM table"

    def test_limit_not_replaced(self, field_mapping: dict[int, int]) -> None:
        """Numeric values like 'limit' should not be touched."""
        node = {"limit": 201}
        result = replace_field_ids(node, field_mapping)
        assert result["limit"] == 201


class TestNestedStructures:
    """Test replacement in deeply nested structures."""

    def test_nested_filter_with_multiple_fields(
        self, field_mapping: dict[int, int]
    ) -> None:
        node = [
            "and",
            [">", ["field", 202, None], 0],
            ["=", ["field", 204, None], "active"],
            [
                "or",
                ["<", ["field", 201, None], 100],
                ["is-null", ["field", 203, None]],
            ],
        ]
        result = replace_field_ids(node, field_mapping)
        assert result == [
            "and",
            [">", ["field", 302, None], 0],
            ["=", ["field", 304, None], "active"],
            [
                "or",
                ["<", ["field", 301, None], 100],
                ["is-null", ["field", 303, None]],
            ],
        ]

    def test_aggregation_with_field_refs(self, field_mapping: dict[int, int]) -> None:
        node = [["sum", ["field", 203, None]], ["count"]]
        result = replace_field_ids(node, field_mapping)
        assert result == [["sum", ["field", 303, None]], ["count"]]

    def test_order_by_with_field_refs(self, field_mapping: dict[int, int]) -> None:
        node = [
            ["asc", ["field", 201, None]],
            ["desc", ["field", 202, None]],
        ]
        result = replace_field_ids(node, field_mapping)
        assert result == [
            ["asc", ["field", 301, None]],
            ["desc", ["field", 302, None]],
        ]

    def test_expressions_with_field_refs(self, field_mapping: dict[int, int]) -> None:
        node = {
            "expressions": {
                "double_amount": [
                    "*",
                    ["field", 203, None],
                    2,
                ]
            }
        }
        result = replace_field_ids(node, field_mapping)
        assert result["expressions"]["double_amount"] == [
            "*",
            ["field", 303, None],
            2,
        ]

    def test_result_metadata_field_refs(self, field_mapping: dict[int, int]) -> None:
        """Field refs in result_metadata should be replaced."""
        node = [
            {
                "name": "user_id",
                "field_ref": ["field", 201, None],
                "id": 201,
            },
            {
                "name": "booking_cnt",
                "field_ref": ["field", 202, None],
                "id": 202,
            },
        ]
        result = replace_field_ids(node, field_mapping)
        # field_ref is replaced
        assert result[0]["field_ref"] == ["field", 301, None]
        assert result[1]["field_ref"] == ["field", 302, None]
        # But id (plain int) is NOT replaced
        assert result[0]["id"] == 201
        assert result[1]["id"] == 202


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_empty_mapping(self) -> None:
        """Empty mapping should return input unchanged."""
        node = {"source-table": 10, "fields": [["field", 201, None]]}
        result = replace_field_ids(node, {}, {})
        assert result == node

    def test_none_input(self) -> None:
        """None input should return None."""
        result = replace_field_ids(None, {201: 301})
        assert result is None

    def test_string_input(self) -> None:
        """String input should be returned as-is."""
        result = replace_field_ids("some string", {201: 301})
        assert result == "some string"

    def test_integer_input(self) -> None:
        """Integer input should be returned as-is."""
        result = replace_field_ids(42, {42: 99})
        assert result == 42

    def test_boolean_input(self) -> None:
        """Boolean input should be returned as-is."""
        result = replace_field_ids(True, {1: 2})
        assert result is True

    def test_empty_list(self) -> None:
        result = replace_field_ids([], {201: 301})
        assert result == []

    def test_empty_dict(self) -> None:
        result = replace_field_ids({}, {201: 301})
        assert result == {}

    def test_no_mutation_of_input(
        self, field_mapping: dict[int, int], table_mapping: dict[int, int]
    ) -> None:
        """The walker should never mutate its input."""
        original = {
            "source-table": 10,
            "fields": [["field", 201, None]],
        }
        import copy

        frozen = copy.deepcopy(original)
        replace_field_ids(original, field_mapping, table_mapping)
        assert original == frozen  # Not mutated

    def test_list_starting_with_non_string(self) -> None:
        """A list like [42, "foo"] should NOT be treated as a field ref."""
        node = [42, "foo", "bar"]
        result = replace_field_ids(node, {42: 99})
        # 42 is not in a field ref context, so it stays
        assert result == [42, "foo", "bar"]

    def test_field_keyword_with_string_id(self) -> None:
        """["field", "column_name", ...] should not be replaced (string ID)."""
        node = ["field", "user_id", {"base-type": "type/Integer"}]
        result = replace_field_ids(node, {201: 301})
        assert result == ["field", "user_id", {"base-type": "type/Integer"}]
