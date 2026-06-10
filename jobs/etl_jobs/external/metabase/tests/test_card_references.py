from __future__ import annotations

from api.models import Card, DatasetQuery
from main import _card_references_legacy_table


class TestNativeSqlCard:
    def test_detects_legacy_table_in_sql(self) -> None:
        # Given
        card = Card(
            id=1,
            name="test",
            dataset_query=DatasetQuery(
                stages=[{"lib/type": "mbql.stage/native", "native": "SELECT * FROM schema.legacy_table"}]
            ),
        )

        # When
        result = _card_references_legacy_table(card, "legacy_table", "schema", legacy_table_id=10)

        # Then
        expected = True
        assert result == expected

    def test_does_not_match_when_sql_references_new_table(self) -> None:
        # Given
        card = Card(
            id=2,
            name="test",
            dataset_query=DatasetQuery(
                stages=[{"lib/type": "mbql.stage/native", "native": "SELECT * FROM schema.new_table"}]
            ),
        )

        # When
        result = _card_references_legacy_table(card, "legacy_table", "schema", legacy_table_id=10)

        # Then
        expected = False
        assert result == expected

    def test_does_not_match_when_sql_is_empty(self) -> None:
        # Given
        card = Card(
            id=3,
            name="test",
            dataset_query=DatasetQuery(stages=[{"lib/type": "mbql.stage/native", "native": ""}]),
        )

        # When
        result = _card_references_legacy_table(card, "legacy_table", "schema", legacy_table_id=10)

        # Then
        expected = False
        assert result == expected


class TestQueryBuilderCard:
    def test_detects_legacy_table_by_source_table_id(self) -> None:
        # Given
        card = Card(
            id=10,
            name="test",
            dataset_query=DatasetQuery(stages=[{"lib/type": "mbql.stage/mbql", "source-table": 42}]),
        )

        # When
        result = _card_references_legacy_table(card, "legacy_table", "schema", legacy_table_id=42)

        # Then
        expected = True
        assert result == expected

    def test_does_not_match_when_source_table_differs(self) -> None:
        # Given
        card = Card(
            id=11,
            name="test",
            dataset_query=DatasetQuery(stages=[{"lib/type": "mbql.stage/mbql", "source-table": 99}]),
        )

        # When
        result = _card_references_legacy_table(card, "legacy_table", "schema", legacy_table_id=42)

        # Then
        expected = False
        assert result == expected

    def test_does_not_match_when_legacy_table_id_is_none(self) -> None:
        # Given
        card = Card(
            id=12,
            name="test",
            dataset_query=DatasetQuery(stages=[{"lib/type": "mbql.stage/mbql", "source-table": 42}]),
        )

        # When
        result = _card_references_legacy_table(card, "legacy_table", "schema", legacy_table_id=None)

        # Then
        expected = False
        assert result == expected


class TestEdgeCases:
    def test_returns_false_when_no_dataset_query(self) -> None:
        # Given
        card = Card(id=20, name="test", dataset_query=None)

        # When
        result = _card_references_legacy_table(card, "legacy_table", "schema", legacy_table_id=10)

        # Then
        expected = False
        assert result == expected

    def test_returns_false_when_no_stages(self) -> None:
        # Given
        card = Card(id=21, name="test", dataset_query=DatasetQuery(stages=None))

        # When
        result = _card_references_legacy_table(card, "legacy_table", "schema", legacy_table_id=10)

        # Then
        expected = False
        assert result == expected

    def test_returns_false_when_stages_empty(self) -> None:
        # Given
        card = Card(id=22, name="test", dataset_query=DatasetQuery(stages=[]))

        # When
        result = _card_references_legacy_table(card, "legacy_table", "schema", legacy_table_id=10)

        # Then
        expected = False
        assert result == expected

    def test_returns_false_for_unknown_stage_type(self) -> None:
        # Given
        card = Card(
            id=23,
            name="test",
            dataset_query=DatasetQuery(stages=[{"lib/type": "mbql.stage/unknown"}]),
        )

        # When
        result = _card_references_legacy_table(card, "legacy_table", "schema", legacy_table_id=10)

        # Then
        expected = False
        assert result == expected
