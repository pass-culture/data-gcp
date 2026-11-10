import pathlib
from unittest.mock import MagicMock

import numpy as np
import polars as pl
import pytest
from typer.testing import CliRunner

from cli import clustering as clustering_cli

runner = CliRunner()


def _synthetic_vectors(n_per_blob: int = 100, n_background: int = 800) -> np.ndarray:
    """Two blobs plus scattered background, so each blob stays under the
    default ``max_cluster_share`` (20%) cap -- see
    ``tests/clustering/test_hdbscan_models.py::_synthetic_blobs``.
    """
    rng = np.random.default_rng(0)
    blob1 = rng.normal(loc=0, scale=0.3, size=(n_per_blob, 5))
    blob2 = rng.normal(loc=8, scale=0.3, size=(n_per_blob, 5))
    background = rng.uniform(low=-10, high=18, size=(n_background, 5))
    return np.vstack([blob1, blob2, background]).astype(np.float32)


def test_fit_command_writes_clusters_and_saves_model(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    vectors = _synthetic_vectors()
    item_ids = [str(i) for i in range(len(vectors))]
    data = pl.DataFrame({"item_id": item_ids, "vector": vectors.tolist()})
    input_path = tmp_path / "vectors.parquet"
    data.write_parquet(input_path)
    output_path = tmp_path / "clusters.parquet"

    save_model_mock = MagicMock()
    write_latest_pointer_mock = MagicMock()
    monkeypatch.setattr(clustering_cli.gcs, "save_model", save_model_mock)
    monkeypatch.setattr(
        clustering_cli.gcs, "write_latest_pointer", write_latest_pointer_mock
    )

    result = runner.invoke(
        clustering_cli.app,
        [
            "fit",
            "--input-file-path",
            str(input_path),
            "--output-file-path",
            str(output_path),
            "--split-level",
            "1",
            "--corpus-id",
            "01000000",
            "--run-id",
            "abc123",
            "--device",
            "cpu",
        ],
    )

    assert result.exit_code == 0, result.output
    save_model_mock.assert_called_once()
    write_latest_pointer_mock.assert_called_once_with(1, "01000000", "abc123")

    clusters = pl.read_parquet(output_path)
    assert clusters.height == len(vectors)
    assert set(clusters["assignment"].unique().to_list()) == {"fit"}
    assert set(clusters["corpus_id"].unique().to_list()) == {"01000000"}


def test_fit_command_carries_over_uuid_for_unchanged_items(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(clustering_cli.gcs, "save_model", MagicMock())
    monkeypatch.setattr(clustering_cli.gcs, "write_latest_pointer", MagicMock())

    vectors = _synthetic_vectors()
    item_ids = [str(i) for i in range(len(vectors))]
    data = pl.DataFrame({"item_id": item_ids, "vector": vectors.tolist()})
    input_path = tmp_path / "vectors.parquet"
    data.write_parquet(input_path)

    first_output = tmp_path / "clusters_1.parquet"
    runner.invoke(
        clustering_cli.app,
        [
            "fit",
            "--input-file-path",
            str(input_path),
            "--output-file-path",
            str(first_output),
            "--split-level",
            "1",
            "--corpus-id",
            "01000000",
            "--run-id",
            "run1",
            "--device",
            "cpu",
        ],
    )

    second_output = tmp_path / "clusters_2.parquet"
    result = runner.invoke(
        clustering_cli.app,
        [
            "fit",
            "--input-file-path",
            str(input_path),
            "--output-file-path",
            str(second_output),
            "--split-level",
            "1",
            "--corpus-id",
            "01000000",
            "--run-id",
            "run2",
            "--previous-clusters-file-path",
            str(first_output),
            "--device",
            "cpu",
        ],
    )

    assert result.exit_code == 0, result.output
    first = pl.read_parquet(first_output).filter(~pl.col("is_outlier"))
    second = pl.read_parquet(second_output).filter(~pl.col("is_outlier"))
    first_uuid_by_item = dict(
        zip(first["item_id"].to_list(), first["topic_uuid"].to_list(), strict=True)
    )
    second_uuid_by_item = dict(
        zip(second["item_id"].to_list(), second["topic_uuid"].to_list(), strict=True)
    )
    common_items = set(first_uuid_by_item) & set(second_uuid_by_item)
    assert common_items
    for item_id in common_items:
        assert first_uuid_by_item[item_id] == second_uuid_by_item[item_id]


def test_predict_command_assigns_training_points_to_own_cluster(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    vectors = _synthetic_vectors()
    item_ids = [str(i) for i in range(len(vectors))]
    data = pl.DataFrame({"item_id": item_ids, "vector": vectors.tolist()})
    input_path = tmp_path / "vectors.parquet"
    data.write_parquet(input_path)
    fit_output_path = tmp_path / "clusters.parquet"

    save_model_mock = MagicMock()
    monkeypatch.setattr(clustering_cli.gcs, "save_model", save_model_mock)
    monkeypatch.setattr(clustering_cli.gcs, "write_latest_pointer", MagicMock())

    runner.invoke(
        clustering_cli.app,
        [
            "fit",
            "--input-file-path",
            str(input_path),
            "--output-file-path",
            str(fit_output_path),
            "--split-level",
            "1",
            "--corpus-id",
            "01000000",
            "--run-id",
            "run1",
            "--device",
            "cpu",
        ],
    )

    saved_model = save_model_mock.call_args[0][0]
    monkeypatch.setattr(clustering_cli.gcs, "load_model", lambda path: saved_model)

    predict_output_path = tmp_path / "predicted.parquet"
    result = runner.invoke(
        clustering_cli.app,
        [
            "predict",
            "--input-file-path",
            str(input_path),
            "--output-file-path",
            str(predict_output_path),
            "--cluster-mapping-file-path",
            str(fit_output_path),
            "--split-level",
            "1",
            "--corpus-id",
            "01000000",
            "--run-id",
            "run1",
            "--device",
            "cpu",
        ],
    )

    assert result.exit_code == 0, result.output
    fitted = pl.read_parquet(fit_output_path).sort("item_id")
    predicted = pl.read_parquet(predict_output_path).sort("item_id")
    agreement = (
        predicted["cluster_id"].to_numpy() == fitted["cluster_id"].to_numpy()
    ).mean()
    assert agreement >= 0.95
