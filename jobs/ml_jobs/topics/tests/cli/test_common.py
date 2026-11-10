import contextlib
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from cli import common as common_cli

runner = CliRunner()


def test_start_opens_run_sets_tags_and_writes_run_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connect = MagicMock()
    monkeypatch.setattr(common_cli, "connect_to_mlflow", connect)

    experiment = MagicMock(experiment_id="42", name="topics_v1.0_dev")
    monkeypatch.setattr(common_cli, "get_mlflow_experiment", lambda name: experiment)

    run = MagicMock()
    run.info.run_id = "run-123"
    monkeypatch.setattr(
        common_cli.mlflow, "start_run", lambda **kwargs: contextlib.nullcontext(run)
    )
    set_tags = MagicMock()
    log_param = MagicMock()
    monkeypatch.setattr(common_cli.mlflow, "set_tags", set_tags)
    monkeypatch.setattr(common_cli.mlflow, "log_param", log_param)

    write_run_id = MagicMock()
    monkeypatch.setattr(common_cli, "write_run_id", write_run_id)

    result = runner.invoke(
        common_cli.app,
        [
            "--config-file-name",
            "default",
            "--split-level",
            "1",
            "--airflow-run-id",
            "manual__x",
        ],
    )

    assert result.exit_code == 0, result.output
    connect.assert_called_once()
    write_run_id.assert_called_once_with("run-123")
    set_tags.assert_called_once_with(
        {
            "env": common_cli.ENV_SHORT_NAME,
            "airflow_run_id": "manual__x",
            "split_level": 1,
        }
    )
    log_param.assert_called_once_with("config_file_name", "default")


def test_start_defaults_run_name_to_none_without_airflow_run_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(common_cli, "connect_to_mlflow", MagicMock())
    experiment = MagicMock(experiment_id="42", name="topics_v1.0_dev")
    monkeypatch.setattr(common_cli, "get_mlflow_experiment", lambda name: experiment)
    run = MagicMock()
    run.info.run_id = "run-123"
    start_run = MagicMock(return_value=contextlib.nullcontext(run))
    monkeypatch.setattr(common_cli.mlflow, "start_run", start_run)
    monkeypatch.setattr(common_cli.mlflow, "set_tags", MagicMock())
    monkeypatch.setattr(common_cli.mlflow, "log_param", MagicMock())
    monkeypatch.setattr(common_cli, "write_run_id", MagicMock())

    result = runner.invoke(common_cli.app, [])

    assert result.exit_code == 0, result.output
    start_run.assert_called_once_with(experiment_id="42", run_name=None)
