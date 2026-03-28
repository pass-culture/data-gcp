from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture(autouse=True)
def mock_metabase_client():
    with patch("main._get_metabase_client") as mock:
        mock.return_value = MagicMock()
        yield mock


class TestArchiveCommand:
    @patch("main.load_archiving_config")
    @patch("main.time.sleep")
    def test_archive_with_cards(
        self, mock_sleep, mock_config, runner, mock_metabase_client
    ):
        mock_config.return_value = {
            "rules": [{"name": "thematic_archiving", "sql": "WHERE 1=1"}],
            "folders": ["thematic"],
            "max_cards_to_archive": 2,
            "empty_collection_cleanup": {},
        }

        with (
            patch("main.ListArchive") as MockListArchive,
            patch("main.MoveToArchive") as MockMoveToArchive,
        ):
            mock_la = MagicMock()
            mock_la.preprocess_data_archiving.return_value = [
                {
                    "id": 1,
                    "name": "Card 1",
                    "destination_collection_id": 99,
                    "collection_id": 10,
                }
            ]
            MockListArchive.return_value = mock_la

            mock_move = MagicMock()
            mock_move.move_object.return_value = {"id": 1, "status": "success"}
            MockMoveToArchive.return_value = mock_move

            from main import app

            result = runner.invoke(app, ["archive"])

        assert result.exit_code == 0
        mock_move.move_object.assert_called_once()
        mock_move.rename_archive_object.assert_called_once()
        mock_move.save_logs_bq.assert_called_once()

    @patch("main.load_archiving_config")
    def test_archive_no_rule_for_folder(self, mock_config, runner):
        mock_config.return_value = {
            "rules": [],
            "folders": ["unknown_folder"],
            "max_cards_to_archive": 10,
            "empty_collection_cleanup": {},
        }

        from main import app

        result = runner.invoke(app, ["archive"])
        assert result.exit_code == 0

    @patch("main.archive_empty_collections")
    @patch("main.archive_dead_dashboards")
    @patch("main.load_archiving_config")
    def test_archive_with_cleanup(
        self, mock_config, mock_dead, mock_empty, runner, mock_metabase_client
    ):
        mock_config.return_value = {
            "rules": [],
            "folders": [],
            "max_cards_to_archive": 10,
            "empty_collection_cleanup": {"root_collection_ids": [608, 607]},
        }

        from main import app

        result = runner.invoke(app, ["archive"])
        assert result.exit_code == 0
        mock_dead.assert_called_once()
        mock_empty.assert_called_once()

    @patch("main.load_archiving_config")
    def test_archive_no_cleanup_config(self, mock_config, runner):
        mock_config.return_value = {
            "rules": [],
            "folders": [],
            "max_cards_to_archive": 10,
        }

        from main import app

        result = runner.invoke(app, ["archive"])
        assert result.exit_code == 0

    @patch("main.load_archiving_config")
    def test_archive_empty_root_ids(self, mock_config, runner):
        mock_config.return_value = {
            "rules": [],
            "folders": [],
            "max_cards_to_archive": 10,
            "empty_collection_cleanup": {"root_collection_ids": []},
        }

        from main import app

        result = runner.invoke(app, ["archive"])
        assert result.exit_code == 0


class TestPermissionsCommand:
    @patch("main.sync_permissions")
    def test_permissions(self, mock_sync, runner, mock_metabase_client):
        mock_sync.return_value = {"changes": [{"a": 1}], "applied": True}

        from main import app

        result = runner.invoke(app, ["permissions"])
        assert result.exit_code == 0
        mock_sync.assert_called_once()

    @patch("main.sync_permissions")
    def test_permissions_dry_run(self, mock_sync, runner, mock_metabase_client):
        mock_sync.return_value = {"changes": [], "applied": False}

        from main import app

        result = runner.invoke(app, ["permissions", "--dry-run"])
        assert result.exit_code == 0
        mock_sync.assert_called_once()


class TestDependenciesCommand:
    @patch("main.run_dependencies")
    def test_dependencies(self, mock_run, runner, mock_metabase_client):
        from main import app

        result = runner.invoke(app, ["dependencies"])
        assert result.exit_code == 0
        mock_run.assert_called_once_with(mock_metabase_client.return_value)
