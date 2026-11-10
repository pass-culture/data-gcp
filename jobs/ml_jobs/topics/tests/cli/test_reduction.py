import pathlib
from unittest.mock import MagicMock

import numpy as np
import polars as pl
import pytest
import umap
from typer.testing import CliRunner

from cli import reduction as reduction_cli

runner = CliRunner()


def test_fetch_embeddings_command_joins_in_documents_order(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    documents = pl.DataFrame(
        {"item_id": ["1", "2"], "gtl_id": ["01000000", "01000000"]}
    )
    documents_path = tmp_path / "documents.parquet"
    documents.write_parquet(documents_path)

    fake_embeddings = pl.DataFrame(
        {
            "item_id": ["2", "1"],
            "embedding": [[1.0, 0.0], [0.0, 1.0]],
            "zero_norm": [False, False],
        }
    )
    monkeypatch.setattr(
        reduction_cli, "fetch_embeddings", lambda dim, gtl_ids: fake_embeddings
    )

    output_path = tmp_path / "embeddings.parquet"
    result = runner.invoke(
        reduction_cli.app,
        [
            "fetch-embeddings",
            "--documents-file-path",
            str(documents_path),
            "--output-file-path",
            str(output_path),
            "--gtl-ids",
            "01000000",
        ],
    )

    assert result.exit_code == 0, result.output
    joined = pl.read_parquet(output_path)
    assert joined["item_id"].to_list() == ["1", "2"]


def test_fit_command_saves_model_and_writes_vectors(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rng = np.random.default_rng(0)
    n_items = 60
    embeddings = pl.DataFrame(
        {
            "item_id": [str(i) for i in range(n_items)],
            "embedding": rng.normal(size=(n_items, 16)).astype(np.float32).tolist(),
        }
    )
    input_path = tmp_path / "embeddings.parquet"
    embeddings.write_parquet(input_path)
    output_path = tmp_path / "vectors.parquet"

    save_model_mock = MagicMock()
    write_latest_pointer_mock = MagicMock()
    monkeypatch.setattr(reduction_cli.gcs, "save_model", save_model_mock)
    monkeypatch.setattr(
        reduction_cli.gcs, "write_latest_pointer", write_latest_pointer_mock
    )

    result = runner.invoke(
        reduction_cli.app,
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
    vectors = pl.read_parquet(output_path)
    assert vectors.height == n_items
    assert len(vectors["vector"][0]) == 5


def test_transform_command_loads_model_and_projects(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rng = np.random.default_rng(0)
    train_vectors = rng.normal(size=(60, 16)).astype(np.float32)
    model = umap.UMAP(
        n_neighbors=15,
        n_components=5,
        min_dist=0.0,
        metric="cosine",
        random_state=42,
        compatibility_layout=True,
    )
    model.fit(train_vectors)
    monkeypatch.setattr(reduction_cli.gcs, "load_model", lambda path: model)

    new_items = pl.DataFrame(
        {
            "item_id": ["a", "b"],
            "embedding": rng.normal(size=(2, 16)).astype(np.float32).tolist(),
        }
    )
    input_path = tmp_path / "new_embeddings.parquet"
    new_items.write_parquet(input_path)
    output_path = tmp_path / "new_vectors.parquet"

    result = runner.invoke(
        reduction_cli.app,
        [
            "transform",
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
        ],
    )

    assert result.exit_code == 0, result.output
    vectors = pl.read_parquet(output_path)
    assert vectors.height == 2
    assert len(vectors["vector"][0]) == 5
