import contextlib
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.common import mlflow as mlflow_module
from src.common.config import load_config


def test_config_to_params_flattens_one_level() -> None:
    config = {
        "spacy_model": "fr_core_news_sm",
        "pos": ["NOUN", "VERB"],
        "boilerplate": {"min_segment_chars": 40},
        "empty_mapping": {},
    }
    params = mlflow_module._config_to_params(config, "vocabulary")

    assert params["vocabulary.spacy_model"] == "fr_core_news_sm"
    assert params["vocabulary.pos"] == "NOUN, VERB"
    assert params["vocabulary.boilerplate"] == '{"min_segment_chars": 40}'
    assert "vocabulary.empty_mapping" not in params


def test_log_config_to_run_is_noop_when_run_id_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connect = MagicMock()
    monkeypatch.setattr(mlflow_module, "connect_to_mlflow", connect)

    mlflow_module.log_config_to_run(
        run_id="",
        domain="vocabulary",
        config=load_config("default").vocabulary,
        config_path="configs/default.yaml",
    )

    connect.assert_not_called()


def test_log_config_to_run_logs_artifact_params_and_metrics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(mlflow_module, "connect_to_mlflow", MagicMock())
    start_run = MagicMock(return_value=contextlib.nullcontext())
    monkeypatch.setattr(mlflow_module.mlflow, "start_run", start_run)
    log_artifact = MagicMock()
    log_params = MagicMock()
    log_metrics = MagicMock()
    set_tag = MagicMock()
    monkeypatch.setattr(mlflow_module.mlflow, "log_artifact", log_artifact)
    monkeypatch.setattr(mlflow_module.mlflow, "log_params", log_params)
    monkeypatch.setattr(mlflow_module.mlflow, "log_metrics", log_metrics)
    monkeypatch.setattr(mlflow_module.mlflow, "set_tag", set_tag)

    mlflow_module.log_config_to_run(
        run_id="abc123",
        domain="vocabulary",
        config=load_config("default").vocabulary,
        config_path="configs/default.yaml",
        metrics={"n_items": 10},
    )

    start_run.assert_called_once_with(run_id="abc123")
    log_artifact.assert_called_once_with(
        "configs/default.yaml", artifact_path="configs"
    )
    (params,), _ = log_params.call_args
    assert params["vocabulary.spacy_model"] == "fr_core_news_sm"
    log_metrics.assert_called_once_with({"vocabulary.n_items": 10})
    set_tag.assert_called_once_with("vocabulary.done", "true")


def test_connect_to_mlflow_sets_token_and_tracking_uri(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        mlflow_module.google.auth, "default", lambda: (MagicMock(), "project")
    )
    signed = MagicMock(signed_jwt="jwt-token")
    iam_client = MagicMock()
    iam_client.service_account_path.return_value = "sa-path"
    iam_client.sign_jwt.return_value = signed
    monkeypatch.setattr(
        mlflow_module.iam_credentials_v1,
        "IAMCredentialsClient",
        lambda credentials: iam_client,
    )
    set_tracking_uri = MagicMock()
    monkeypatch.setattr(mlflow_module.mlflow, "set_tracking_uri", set_tracking_uri)

    mlflow_module.connect_to_mlflow()

    assert mlflow_module.os.environ["MLFLOW_TRACKING_TOKEN"] == "jwt-token"
    set_tracking_uri.assert_called_once_with(mlflow_module.MLFLOW_URI)


def test_get_mlflow_experiment_returns_existing_active_experiment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    existing = SimpleNamespace(lifecycle_stage="active", experiment_id="1")
    client = MagicMock()
    client.get_experiment_by_name.return_value = existing
    monkeypatch.setattr(mlflow_module.mlflow, "MlflowClient", lambda: client)

    assert mlflow_module.get_mlflow_experiment("topics_v1.0_dev") is existing
    client.create_experiment.assert_not_called()
    client.restore_experiment.assert_not_called()


def test_get_mlflow_experiment_creates_when_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created = SimpleNamespace(lifecycle_stage="active", experiment_id="2")
    client = MagicMock()
    client.get_experiment_by_name.return_value = None
    client.create_experiment.return_value = "2"
    client.get_experiment.return_value = created
    monkeypatch.setattr(mlflow_module.mlflow, "MlflowClient", lambda: client)

    assert mlflow_module.get_mlflow_experiment("topics_v1.0_dev") is created
    client.create_experiment.assert_called_once_with(name="topics_v1.0_dev")


def test_get_mlflow_experiment_reactivates_deleted_experiment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deleted = SimpleNamespace(lifecycle_stage="deleted", experiment_id="3")
    reactivated = SimpleNamespace(lifecycle_stage="active", experiment_id="3")
    client = MagicMock()
    client.get_experiment_by_name.return_value = deleted
    client.get_experiment.return_value = reactivated
    monkeypatch.setattr(mlflow_module.mlflow, "MlflowClient", lambda: client)

    assert mlflow_module.get_mlflow_experiment("topics_v1.0_dev") is reactivated
    client.restore_experiment.assert_called_once_with("3")
