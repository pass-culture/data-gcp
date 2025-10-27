"""Unit tests for Metabase job replacement utilities.

tests: they patch the MetabaseAPI constructor and methods so no network calls
are performed and assert the behavior of `NativeCard` and `QueryCard`.
"""

from unittest.mock import patch

from native import NativeCard
from query import QueryCard


class TestMetabaseReplacements:
    """Tests using patch-style mocking similar to titelive tests."""

    @patch("metabase_api.MetabaseAPI.__init__", return_value=None)
    @patch("metabase_api.MetabaseAPI.get_cards")
    def test_native_replace_table_name_when_legacy_in_new_name(
        self, mock_get_cards, mock_init
    ):
        """Ensure NativeCard replacements respect token boundaries.

        We patch MetabaseAPI.__init__ so no network call happens during
        construction. We patch get_cards to return a card payload that
        contains:
        - a standalone reference to schema.legacy_table (should be replaced)
        - a table name which contains the legacy table name as a substring
          (should NOT be replaced)
        """

        legacy_table_name = "users"
        new_table_name = "users_new"
        other_table_name = "monthly_users_archive"

        legacy_schema_name = "analytics"
        new_schema_name = "analytics_new"
        other_schema_name = "intermediate"

        sql = f"""
        SELECT *
        FROM {legacy_schema_name}.{legacy_table_name}
        JOIN {other_schema_name}.{other_table_name}
        ON {legacy_table_name}.id = {other_table_name}.{legacy_table_name}_id
        """

        card_payload = {
            "dataset_query": {"native": {"query": sql, "template-tags": {}}}
        }
        mock_get_cards.return_value = card_payload

        # construct a MetabaseAPI instance (no-op __init__), then NativeCard
        from metabase_api import MetabaseAPI

        api = MetabaseAPI(username="x", password="y", host="h", client_id="c")
        native = NativeCard(card_id=1, metabase_api=api)

        # perform replacements
        native.replace_schema_name(
            legacy_schema_name, new_schema_name, legacy_table_name, new_table_name
        )
        native.replace_table_name(legacy_table_name, new_table_name)

        updated = native.card_info["dataset_query"]["native"]["query"]

        # standalone schema.table should be replaced
        assert f"{new_schema_name}.{new_table_name}" in updated

        # other table that contains legacy substring must remain unchanged
        assert f"{other_schema_name}.{other_table_name}" in updated
        assert (
            f"{other_table_name.replace(legacy_table_name, new_table_name)}"
            not in updated
        )

    @patch("metabase_api.MetabaseAPI.__init__", return_value=None)
    @patch("metabase_api.MetabaseAPI.get_cards")
    def test_query_update_dataset_query_avoids_replacing_other_table_names(
        self, mock_get_cards, mock_init
    ):
        """Ensure QueryCard.update_dataset_query updates numeric ids and preserves field names.

        Patch style mirrors other tests: stub constructor and get_cards so we can
        instantiate a real QueryCard object without network.
        """

        legacy_id = 42
        new_id = 99
        legacy = "users"

        card_dict = {
            "dataset_query": {"query": "irrelevant"},
            "source-table": legacy_id,
            "table_id": legacy_id,
            "fields": [
                {"name": f"{legacy}_some_field", "id": "100"},
                {"name": "other", "id": "100"},
            ],
        }

        mock_get_cards.return_value = card_dict

        from metabase_api import MetabaseAPI

        api = MetabaseAPI(username="x", password="y", host="h", client_id="c")
        qc = QueryCard(card_id=2, metabase_api=api)

        # mapping that would be dangerous if applied too broadly
        mapped_fields = {"100": "200"}

        qc.update_dataset_query(
            mapped_fields, legacy_table_id=legacy_id, new_table_id=new_id
        )

        # numeric source-table must be replaced
        assert qc.card_info["source-table"] == new_id

        # fields ids should not contains legacy ids
        field_ids = [f["id"] for f in qc.card_info.get("fields", [])]
        assert not any(x in field_ids for x in list(mapped_fields.keys()))
