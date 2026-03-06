from unittest.mock import patch

import pandas as pd

from domain.archiving import (
    ListArchive,
    MoveToArchive,
    _is_collection_empty,
    _is_dashboard_dead,
    archive_dead_dashboards,
    archive_empty_collections,
)


class TestListArchive:
    def test_get_data_archiving_no_rule(self):
        la = ListArchive(metabase_folder="test", rule_sql=None)
        la.get_data_archiving()
        assert la.archive_df.empty

    def test_get_data_archiving_empty_string(self):
        la = ListArchive(metabase_folder="test", rule_sql="")
        la.get_data_archiving()
        assert la.archive_df.empty

    @patch("domain.archiving.pd.read_gbq")
    def test_get_data_archiving_with_rule(self, mock_read_gbq):
        mock_read_gbq.return_value = pd.DataFrame({"id": [1, 2]})
        la = ListArchive(metabase_folder="thematic", rule_sql="WHERE x > 1")
        la.get_data_archiving()
        assert len(la.archive_df) == 2
        mock_read_gbq.assert_called_once()
        assert "int_metabase_dev" in mock_read_gbq.call_args[0][0]

    def test_preprocess_empty_df(self):
        la = ListArchive(metabase_folder="test", rule_sql=None)
        la.archive_df = pd.DataFrame()
        result = la.preprocess_data_archiving()
        assert result == []

    def test_preprocess_data_archiving(self):
        la = ListArchive(metabase_folder="test", rule_sql="WHERE 1=1")
        la.archive_df = pd.DataFrame(
            {
                "card_id": [1],
                "card_name": ["My Card"],
                "card_collection_id": [10],
                "collection_id": [5],
                "archive_location_level_2": ["path/to/99"],
                "total_users": [3],
                "total_views": [10],
                "nbr_dashboards": [1],
                "last_execution_date": ["2024-01-01"],
                "last_execution_context": ["api"],
                "total_errors": [0],
                "parent_folder": ["thematic"],
                "days_since_last_execution": [100],
            }
        )
        result = la.preprocess_data_archiving()
        assert len(result) == 1
        assert result[0]["id"] == 1
        assert result[0]["name"] == "My Card"
        assert result[0]["destination_collection_id"] == 99
        assert result[0]["object_type"] == "card"

    def test_preprocess_data_archiving_exception_fallback(self):
        la = ListArchive(metabase_folder="test", rule_sql="WHERE 1=1")
        # Missing archive_location_level_2 column will trigger exception path
        la.archive_df = pd.DataFrame(
            {
                "card_id": [1],
                "card_name": ["My Card"],
                "card_collection_id": [10],
                "collection_id": [5],
                "total_users": [3],
                "total_views": [10],
                "nbr_dashboards": [1],
                "last_execution_date": ["2024-01-01"],
                "last_execution_context": ["api"],
                "total_errors": [0],
                "parent_folder": ["thematic"],
                "days_since_last_execution": [100],
            }
        )
        result = la.preprocess_data_archiving()
        assert len(result) == 1
        assert result[0]["destination_collection_id"] == 29

    def test_preprocess_filters_nan_archive_location(self):
        la = ListArchive(metabase_folder="test", rule_sql="WHERE 1=1")
        la.archive_df = pd.DataFrame(
            {
                "card_id": [1, 2],
                "card_name": ["Card A", "Card B"],
                "card_collection_id": [10, 20],
                "collection_id": [5, 6],
                "archive_location_level_2": ["path/to/99", None],
                "total_users": [3, 4],
                "total_views": [10, 20],
                "nbr_dashboards": [1, 2],
                "last_execution_date": ["2024-01-01", "2024-01-02"],
                "last_execution_context": ["api", "dashboard"],
                "total_errors": [0, 1],
                "parent_folder": ["thematic", "thematic"],
                "days_since_last_execution": [100, 200],
            }
        )
        result = la.preprocess_data_archiving()
        assert len(result) == 1
        assert result[0]["id"] == 1


class TestMoveToArchive:
    def _make_movement(self, name="My Card"):
        return {
            "id": 42,
            "name": name,
            "destination_collection_id": 99,
            "collection_id": 10,
            "last_execution_date": "2024-01-01",
            "last_execution_context": "api",
            "parent_folder": "thematic",
        }

    def test_rename_archive_object_adds_prefix(self, metabase):
        m = MoveToArchive(self._make_movement("My Card"), metabase)
        m.rename_archive_object()
        metabase.put_card.assert_called_once_with(42, {"name": "[Archive] - My Card"})

    def test_rename_archive_object_already_has_prefix(self, metabase):
        m = MoveToArchive(self._make_movement("[Archive] - My Card"), metabase)
        m.rename_archive_object()
        metabase.put_card.assert_called_once_with(42, {"name": "[Archive] - My Card"})

    def test_rename_archive_object_case_insensitive(self, metabase):
        m = MoveToArchive(self._make_movement("Old ARCHIVE card"), metabase)
        m.rename_archive_object()
        metabase.put_card.assert_called_once_with(42, {"name": "Old ARCHIVE card"})

    def test_move_object_success(self, metabase):
        metabase.update_card_collections.return_value = {"status": "ok"}
        m = MoveToArchive(self._make_movement(), metabase)
        log_entry = m.move_object()
        assert log_entry["status"] == "success"
        assert log_entry["id"] == 42
        assert log_entry["new_collection_id"] == 99
        assert log_entry["previous_collection_id"] == 10

    def test_move_object_failure(self, metabase):
        metabase.update_card_collections.return_value = {"status": "error"}
        m = MoveToArchive(self._make_movement(), metabase)
        log_entry = m.move_object()
        assert log_entry["status"] == "error"

    @patch("domain.archiving.pd.DataFrame.to_gbq")
    def test_save_logs_bq(self, mock_to_gbq, metabase):
        m = MoveToArchive(self._make_movement(), metabase)
        log_entry = {"id": 42, "status": "success"}
        m.save_logs_bq(log_entry)
        mock_to_gbq.assert_called_once()

    def test_move_object_optional_fields(self, metabase):
        movement = {
            "id": 1,
            "name": "Card",
            "destination_collection_id": 50,
            "collection_id": 10,
        }
        metabase.update_card_collections.return_value = {"status": "ok"}
        m = MoveToArchive(movement, metabase)
        log_entry = m.move_object()
        assert log_entry["last_execution_date"] is None
        assert log_entry["last_execution_context"] is None
        assert log_entry["parent_folder"] is None


class TestArchiveDeadDashboards:
    def test_no_dead_dashboards(self, metabase):
        metabase.get_collection_children.return_value = {"data": []}
        result = archive_dead_dashboards(metabase, [100])
        assert result == []

    def test_archives_dead_dashboard(self, metabase):
        metabase.get_collection_children.return_value = {
            "data": [{"model": "dashboard", "id": 5}]
        }
        metabase.get_dashboards.return_value = {"ordered_cards": []}

        result = archive_dead_dashboards(metabase, [100])
        assert result == [5]
        metabase.put_dashboard.assert_called_once_with(5, {"archived": True})

    def test_skips_live_dashboard(self, metabase):
        metabase.get_collection_children.return_value = {
            "data": [{"model": "dashboard", "id": 5}]
        }
        metabase.get_dashboards.return_value = {
            "ordered_cards": [{"card": {"archived": False}}]
        }

        result = archive_dead_dashboards(metabase, [100])
        assert result == []

    def test_recursive_scan(self, metabase):
        def side_effect(collection_id, **kwargs):
            if collection_id == 100:
                return {
                    "data": [
                        {"model": "collection", "id": 200},
                        {"model": "dashboard", "id": 5},
                    ]
                }
            elif collection_id == 200:
                return {"data": [{"model": "dashboard", "id": 6}]}
            return {"data": []}

        metabase.get_collection_children.side_effect = side_effect
        metabase.get_dashboards.return_value = {"ordered_cards": []}

        result = archive_dead_dashboards(metabase, [100])
        assert set(result) == {5, 6}

    def test_multiple_roots(self, metabase):
        metabase.get_collection_children.return_value = {"data": []}
        result = archive_dead_dashboards(metabase, [100, 200, 300])
        assert result == []
        assert metabase.get_collection_children.call_count == 3


class TestIsDashboardDead:
    def test_empty_dashboard_is_dead(self, metabase):
        metabase.get_dashboards.return_value = {"ordered_cards": []}
        assert _is_dashboard_dead(metabase, 1) is True

    def test_all_archived_cards_is_dead(self, metabase):
        metabase.get_dashboards.return_value = {
            "ordered_cards": [
                {"card": {"archived": True}},
                {"card": {"archived": True}},
            ]
        }
        assert _is_dashboard_dead(metabase, 1) is True

    def test_some_active_cards_not_dead(self, metabase):
        metabase.get_dashboards.return_value = {
            "ordered_cards": [
                {"card": {"archived": True}},
                {"card": {"archived": False}},
            ]
        }
        assert _is_dashboard_dead(metabase, 1) is False

    def test_card_is_none_ignored(self, metabase):
        metabase.get_dashboards.return_value = {
            "ordered_cards": [
                {"card": None},
                {"card": {"archived": True}},
            ]
        }
        assert _is_dashboard_dead(metabase, 1) is True


class TestIsCollectionEmpty:
    def test_empty(self, metabase):
        metabase.get_collection_children.return_value = {"data": []}
        assert _is_collection_empty(metabase, 1) is True

    def test_not_empty(self, metabase):
        metabase.get_collection_children.return_value = {
            "data": [{"model": "card", "id": 1}]
        }
        assert _is_collection_empty(metabase, 1) is False


class TestArchiveEmptyCollections:
    def test_no_empty_collections(self, metabase):
        metabase.get_collection_children.return_value = {"data": []}
        result = archive_empty_collections(metabase, [100])
        assert result == []

    def test_archives_empty_leaf(self, metabase):
        call_count = 0

        def side_effect(collection_id, **kwargs):
            nonlocal call_count
            models = kwargs.get("models")
            if collection_id == 100 and models == ["collection"]:
                return {"data": [{"model": "collection", "id": 200}]}
            if collection_id == 200 and models == ["collection"]:
                return {"data": []}
            # _is_collection_empty calls without models
            if collection_id == 200:
                return {"data": []}
            return {"data": [{"model": "card", "id": 1}]}

        metabase.get_collection_children.side_effect = side_effect
        result = archive_empty_collections(metabase, [100])
        assert result == [200]
        metabase.put_collection.assert_called_once_with(200, {"archived": True})

    def test_excludes_root_ids(self, metabase):
        metabase.get_collection_children.return_value = {"data": []}
        archive_empty_collections(metabase, [100])
        # Root 100 itself should never be archived even if empty
        metabase.put_collection.assert_not_called()

    def test_custom_exclude_ids(self, metabase):
        def side_effect(collection_id, **kwargs):
            if kwargs.get("models") == ["collection"]:
                if collection_id == 100:
                    return {"data": [{"model": "collection", "id": 200}]}
                return {"data": []}
            # Both empty
            return {"data": []}

        metabase.get_collection_children.side_effect = side_effect
        result = archive_empty_collections(metabase, [100], exclude_ids={100, 200})
        assert result == []

    def test_non_collection_children_skipped(self, metabase):
        def side_effect(collection_id, **kwargs):
            if kwargs.get("models") == ["collection"]:
                if collection_id == 100:
                    return {"data": [{"model": "card", "id": 999}]}
                return {"data": []}
            return {"data": []}

        metabase.get_collection_children.side_effect = side_effect
        result = archive_empty_collections(metabase, [100])
        assert result == []
