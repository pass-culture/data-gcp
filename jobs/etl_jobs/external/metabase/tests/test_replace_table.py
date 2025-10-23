import os
import sys

from native import NativeCard

# ensure the metabase package directory is importable when running tests from repo root
TEST_DIR = os.path.dirname(__file__)
PROJECT_DIR = os.path.abspath(os.path.join(TEST_DIR, ".."))
sys.path.insert(0, PROJECT_DIR)


class DummyMetabaseAPI:
    def __init__(self, card_dict=None):
        self._card = card_dict or {}
        self.updated = None

    def get_cards(self, card_id):
        return self._card

    def put_card(self, card_id=None, card_dict=None):
        # capture what would be sent to the API
        self.updated = card_dict
        return {"status": "ok"}


def make_native_card_query(
    legacy_table_name, legacy_schema_name, other_table_name, other_schema_name
):
    # query references legacy table as separate token and also appears inside another table name
    q = f"""
    SELECT *
    FROM {legacy_schema_name}.{legacy_table_name}
    JOIN {other_schema_name}.{other_table_name}
    ON {legacy_table_name}.id = {other_table_name}.{legacy_table_name}_id
    """

    card = {"dataset_query": {"native": {"query": q, "template-tags": {}}}}
    return card


def test_native_replace_table_name_when_legacy_in_new_name():
    legacy_table_name = "users"
    new_table_name = "users_new"
    other_table_name = "monthly_users_archive"

    legacy_schema_name = "analytics"
    new_schema_name = "analytics_new"
    other_schema_name = "intermediate"

    card = make_native_card_query(
        legacy_table_name, legacy_schema_name, other_table_name, other_schema_name
    )
    api = DummyMetabaseAPI(card_dict=card)

    native = NativeCard(card_id=1, metabase_api=api)

    # run replacement
    native.replace_schema_name(
        legacy_schema_name, new_schema_name, legacy_table_name, new_table_name
    )
    native.replace_table_name(legacy_table_name, new_table_name)
    updated_query = api._card["dataset_query"]["native"]["query"]
    # legacy should be replaced by new as a separate token
    assert f"{new_schema_name}.{new_table_name}" in updated_query

    # but occurrences where legacy is a substring of another table name should not be replaced
    assert f"{other_schema_name}.{other_table_name}" in updated_query
    assert (
        f"{other_table_name.replace(legacy_table_name, new_table_name)}"
        not in updated_query
    )
