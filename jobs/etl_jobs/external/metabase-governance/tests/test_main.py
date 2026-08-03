from unittest.mock import MagicMock, patch

import pandas as pd
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


def _base_config(**overrides):
    config = {
        "archive_collection_id": 99,
        "max_cards_to_archive": 10,
        "soft_archive_rules": [],
    }
    config.update(overrides)
    return config


class TestArchiveCommand:
    @pytest.fixture(autouse=True)
    def _mock_offline(self):
        # Pre-flight stats and the failure-cooldown lookup both hit BigQuery;
        # mock them so tests stay offline.
        with (
            patch("main.compute_archive_stats", return_value={}) as compute,
            patch("main.log_archive_stats") as log,
            patch("main.load_recently_failed_card_ids", return_value=set()) as failed,
        ):
            yield {"compute": compute, "log": log, "failed": failed}

    @patch("main.append_archiving_logs")
    @patch("main.load_activity_dataframe")
    @patch("main.select_soft_archive_candidates")
    @patch("main.load_archiving_config")
    @patch("main.time.sleep")
    def test_archive_skips_rename_on_move_failure(
        self,
        mock_sleep,
        mock_config,
        mock_select,
        mock_load_df,
        mock_append_logs,
        runner,
        mock_metabase_client,
    ):
        mock_config.return_value = _base_config(
            soft_archive_rules=[
                {
                    "name": "adhoc",
                    "ancestor_title_pattern": "%adhoc%",
                    "min_age_days": 15,
                    "views_window_months": 3,
                    "max_views": 5,
                }
            ],
            max_cards_to_archive=2,
        )
        mock_load_df.return_value = pd.DataFrame()
        mock_select.return_value = [
            {"id": 1, "name": "Card 1", "collection_id": 10, "rule_name": "adhoc"},
            {"id": 2, "name": "Card 2", "collection_id": 10, "rule_name": "adhoc"},
        ]

        with patch("main.MoveToArchive") as MockMoveToArchive:
            ok_move = MagicMock()
            ok_move.move_object.return_value = {"id": 1, "status": "success"}
            failed_move = MagicMock()
            failed_move.move_object.return_value = {"id": 2, "status": "error"}
            MockMoveToArchive.side_effect = [ok_move, failed_move]

            from main import app

            result = runner.invoke(app, ["archive"])

        assert result.exit_code == 0
        # Successful move → rename called.
        ok_move.rename_archive_object.assert_called_once()
        # Failed move → rename NOT called (otherwise we'd leave a "[Archive] - ..."
        # prefix on a card still sitting in its original collection).
        failed_move.rename_archive_object.assert_not_called()

    @patch("main.append_archiving_logs")
    @patch("main.load_activity_dataframe")
    @patch("main.select_soft_archive_candidates")
    @patch("main.load_archiving_config")
    @patch("main.time.sleep")
    def test_archive_with_cards(
        self,
        mock_sleep,
        mock_config,
        mock_select,
        mock_load_df,
        mock_append_logs,
        runner,
        mock_metabase_client,
    ):
        mock_config.return_value = _base_config(
            soft_archive_rules=[
                {
                    "name": "adhoc",
                    "ancestor_title_pattern": "%adhoc%",
                    "min_age_days": 15,
                    "views_window_months": 3,
                    "max_views": 5,
                }
            ],
            max_cards_to_archive=2,
        )
        mock_load_df.return_value = pd.DataFrame()
        mock_select.return_value = [
            {"id": 1, "name": "Card 1", "collection_id": 10, "rule_name": "adhoc"},
            {"id": 2, "name": "Card 2", "collection_id": 10, "rule_name": "adhoc"},
        ]

        with patch("main.MoveToArchive") as MockMoveToArchive:
            mock_move = MagicMock()
            mock_move.move_object.return_value = {"id": 1, "status": "success"}
            MockMoveToArchive.return_value = mock_move

            from main import app

            result = runner.invoke(app, ["archive"])

        assert result.exit_code == 0
        assert mock_move.move_object.call_count == 2
        assert mock_move.rename_archive_object.call_count == 2
        # One batched BQ write at the end, not per card.
        mock_append_logs.assert_called_once()
        assert len(mock_append_logs.call_args[0][0]) == 2

    @patch("main.load_archiving_config")
    def test_archive_no_rules(self, mock_config, runner):
        mock_config.return_value = _base_config()
        from main import app

        result = runner.invoke(app, ["archive"])
        assert result.exit_code == 0

    @patch("main.archive_empty_collections")
    @patch("main.archive_dead_dashboards")
    @patch("main.load_archiving_config")
    def test_archive_with_cleanup(
        self, mock_config, mock_dead, mock_empty, runner, mock_metabase_client
    ):
        mock_config.return_value = _base_config(
            empty_cleanup={
                "min_age_days": 15,
                "scan_root_collection_ids": [608, 607],
            }
        )

        from main import app

        result = runner.invoke(app, ["archive"])
        assert result.exit_code == 0
        mock_dead.assert_called_once()
        mock_empty.assert_called_once()
        # Both should be called with min_age_days=15
        assert mock_dead.call_args.kwargs["min_age_days"] == 15
        assert mock_empty.call_args.kwargs["min_age_days"] == 15

    @patch("main.load_archiving_config")
    def test_archive_no_cleanup_config(self, mock_config, runner):
        mock_config.return_value = _base_config()
        from main import app

        result = runner.invoke(app, ["archive"])
        assert result.exit_code == 0

    @patch("main.load_archiving_config")
    def test_archive_empty_root_ids(self, mock_config, runner):
        mock_config.return_value = _base_config(
            empty_cleanup={"min_age_days": 15, "scan_root_collection_ids": []}
        )

        from main import app

        result = runner.invoke(app, ["archive"])
        assert result.exit_code == 0

    @patch("main.hard_archive_stale_cards")
    @patch("main.load_archiving_config")
    def test_archive_calls_hard_archive(
        self, mock_config, mock_hard_archive, runner, mock_metabase_client
    ):
        mock_config.return_value = _base_config(
            hard_archive_cards={
                "name_pattern": r"^\[Archive\]",
                "min_days_since_update": 60,
                "max_cards": 50,
            }
        )

        from main import app

        result = runner.invoke(app, ["archive"])
        assert result.exit_code == 0
        mock_hard_archive.assert_called_once_with(
            mock_metabase_client.return_value,
            name_pattern=r"^\[Archive\]",
            min_days_since_update=60,
            max_cards=50,
            # Absent from config → default cooldown applied.
            failure_cooldown_days=30,
            dry_run=False,
        )

    @patch("main.hard_archive_stale_cards")
    @patch("main.load_archiving_config")
    def test_archive_skips_hard_archive_when_absent(
        self, mock_config, mock_hard_archive, runner
    ):
        mock_config.return_value = _base_config()

        from main import app

        result = runner.invoke(app, ["archive"])
        assert result.exit_code == 0
        mock_hard_archive.assert_not_called()

    @patch("main.append_archiving_logs")
    @patch("main.load_activity_dataframe")
    @patch("main.select_soft_archive_candidates")
    @patch("main.hard_archive_stale_cards")
    @patch("main.archive_dead_dashboards")
    @patch("main.archive_empty_collections")
    @patch("main.load_archiving_config")
    @patch("main.time.sleep")
    def test_dry_run_propagates_everywhere(
        self,
        mock_sleep,
        mock_config,
        mock_empty,
        mock_dead,
        mock_hard,
        mock_select,
        mock_load_df,
        mock_append_logs,
        runner,
        mock_metabase_client,
    ):
        mock_config.return_value = _base_config(
            soft_archive_rules=[
                {
                    "name": "adhoc",
                    "ancestor_title_pattern": "%adhoc%",
                    "min_age_days": 15,
                    "views_window_months": 3,
                    "max_views": 5,
                }
            ],
            hard_archive_cards={
                "name_pattern": r"^\[Archive\]",
                "min_days_since_update": 60,
                "max_cards": 50,
            },
            empty_cleanup={
                "min_age_days": 15,
                "scan_root_collection_ids": [608],
            },
        )
        mock_load_df.return_value = pd.DataFrame()
        mock_select.return_value = [
            {"id": 1, "name": "Card 1", "collection_id": 10, "rule_name": "adhoc"}
        ]

        with patch("main.MoveToArchive") as MockMoveToArchive:
            mock_move = MagicMock()
            mock_move.move_object.return_value = None  # dry-run path
            MockMoveToArchive.return_value = mock_move

            from main import app

            result = runner.invoke(app, ["archive", "--dry-run"])

        assert result.exit_code == 0
        # MoveToArchive constructed with dry_run=True
        assert MockMoveToArchive.call_args.kwargs["dry_run"] is True
        # No batched BQ write in dry-run
        mock_append_logs.assert_not_called()
        # Other helpers receive dry_run=True
        assert mock_hard.call_args.kwargs["dry_run"] is True
        assert mock_dead.call_args.kwargs["dry_run"] is True
        assert mock_empty.call_args.kwargs["dry_run"] is True

    @patch("main.load_activity_dataframe")
    @patch("main.select_soft_archive_candidates")
    @patch("main.load_archiving_config")
    def test_dataset_and_table_overrides(
        self,
        mock_config,
        mock_select,
        mock_load_df,
        runner,
        mock_metabase_client,
    ):
        mock_config.return_value = _base_config(
            soft_archive_rules=[
                {
                    "name": "adhoc",
                    "ancestor_title_pattern": "%adhoc%",
                    "min_age_days": 15,
                    "views_window_months": 3,
                    "max_views": 5,
                }
            ],
        )
        mock_load_df.return_value = pd.DataFrame()
        mock_select.return_value = []

        from main import app

        result = runner.invoke(
            app,
            [
                "archive",
                "--dry-run",
                "--dataset-name",
                "sandbox_xyz",
                "--table-name",
                "activity_test",
            ],
        )
        assert result.exit_code == 0
        mock_load_df.assert_called_once_with(
            dataset="sandbox_xyz", table="activity_test"
        )


class TestStatsCommand:
    @pytest.fixture(autouse=True)
    def _mock_failed(self):
        with patch("main.load_recently_failed_card_ids", return_value=set()) as failed:
            yield failed

    @patch("main.log_archive_stats")
    @patch("main.compute_archive_stats")
    @patch("main.load_activity_dataframe")
    @patch("main.load_archiving_config")
    def test_stats_runs_with_soft_rules(
        self,
        mock_config,
        mock_load_df,
        mock_compute,
        mock_log,
        runner,
        mock_metabase_client,
    ):
        mock_config.return_value = _base_config(
            soft_archive_rules=[
                {
                    "name": "adhoc",
                    "ancestor_title_pattern": "%adhoc%",
                    "min_age_days": 15,
                    "views_window_months": 3,
                    "max_views": 5,
                }
            ],
        )
        mock_load_df.return_value = pd.DataFrame()
        mock_compute.return_value = {"stub": True}

        from main import app

        result = runner.invoke(app, ["stats"])
        assert result.exit_code == 0
        mock_load_df.assert_called_once()
        mock_compute.assert_called_once()
        mock_log.assert_called_once_with({"stub": True})

    @patch("main.log_archive_stats")
    @patch("main.compute_archive_stats")
    @patch("main.load_activity_dataframe")
    @patch("main.load_archiving_config")
    def test_stats_skips_activity_load_without_soft_rules(
        self,
        mock_config,
        mock_load_df,
        mock_compute,
        mock_log,
        runner,
        mock_metabase_client,
    ):
        mock_config.return_value = _base_config()
        mock_compute.return_value = {}
        from main import app

        result = runner.invoke(app, ["stats"])
        assert result.exit_code == 0
        mock_load_df.assert_not_called()
        mock_compute.assert_called_once()


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
