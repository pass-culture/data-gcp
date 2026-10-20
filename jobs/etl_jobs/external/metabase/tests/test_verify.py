from __future__ import annotations

from api.models import CardDependencyInfo, TableDependency, TablesToMigrate
from migration.verify import verify_migration


def _dependency(table_id: int, schema: str, card_ids: list[int]) -> TableDependency:
    info = CardDependencyInfo(
        card_name="card", card_type="native", card_owner="", collection_path_names="", collection_path_ids=""
    )
    return TableDependency(id=table_id, schema=schema, cards_using_table={str(cid): info for cid in card_ids})


TABLES = TablesToMigrate.model_validate({"analytics.old_table": {"target_table": "mart.new_table"}})


class TestVerifyMigration:
    def test_reports_cards_still_on_the_legacy_table(self) -> None:
        # Given
        cache = {"analytics.old_table": _dependency(1, "analytics", [7]), "mart.new_table": _dependency(2, "mart", [])}

        # When
        report = verify_migration(TABLES, cache, run_card=lambda _cid: None)

        # Then
        assert report.cards_on_legacy_tables == {"analytics.old_table": [7]}
        assert not report.ok

    def test_runs_every_card_on_the_new_table_and_reports_failures(self) -> None:
        # Given
        cache = {"mart.new_table": _dependency(2, "mart", [8, 9])}
        errors = {9: "Unrecognized name: nb_visits"}

        # When
        report = verify_migration(TABLES, cache, run_card=errors.get)

        # Then
        assert report.cards_checked == [8, 9]
        assert report.failed_cards == {9: "Unrecognized name: nb_visits"}
        assert not report.ok

    def test_is_ok_when_nothing_is_left_and_every_card_runs(self) -> None:
        # Given
        cache = {"mart.new_table": _dependency(2, "mart", [8])}

        # When
        report = verify_migration(TABLES, cache, run_card=lambda _cid: None)

        # Then
        assert report.ok

    def test_columns_only_migration_has_no_legacy_check(self) -> None:
        # Given
        tables = TablesToMigrate.model_validate({"analytics.t": {"columns_to_migrate": {"a": "b"}}})
        cache = {"analytics.t": _dependency(1, "analytics", [5])}

        # When
        report = verify_migration(tables, cache, run_card=lambda _cid: None)

        # Then
        assert report.cards_on_legacy_tables == {}
        assert report.cards_checked == [5]

    def test_card_ids_restrict_the_cards_run(self) -> None:
        # Given
        cache = {"mart.new_table": _dependency(2, "mart", [8, 9])}
        ran: list[int] = []

        def run_card(card_id: int) -> str | None:
            ran.append(card_id)
            return None

        # When
        verify_migration(TABLES, cache, run_card=run_card, card_ids={9})

        # Then
        assert ran == [9]
