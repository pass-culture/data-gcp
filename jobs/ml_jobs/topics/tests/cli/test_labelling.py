import pathlib
from unittest.mock import MagicMock

import polars as pl
import pytest
from typer.testing import CliRunner

from cli import labelling as labelling_cli
from src.common.config import load_config as real_load_config

runner = CliRunner()

THEMES = {
    "topic-0": ["recette", "cuisine", "plat", "ingredient"],
    "topic-1": ["voyage", "pays", "avion", "valise"],
    "topic-2": ["sport", "match", "equipe", "ballon"],
}
FILLER = ["livre", "auteur"]


def _vocabulary_table() -> pl.DataFrame:
    words = [w for words in THEMES.values() for w in words] + FILLER
    return pl.DataFrame(
        {
            "term": words,
            "df_total": [30] * len(words),
            "is_stop_word": [False] * len(words),
        }
    )


def _synthetic_corpus(
    tmp_path: pathlib.Path,
) -> tuple[pathlib.Path, pathlib.Path]:
    """3 themed topics x 20 items, plus a 10-item outlier group."""
    item_ids: list[str] = []
    gtl_ids: list[str] = []
    documents: list[str] = []
    topic_uuids: list[str] = []
    is_outliers: list[bool] = []
    cluster_ids: list[int] = []
    assignments: list[str] = []

    for topic_index, (topic_uuid, words) in enumerate(THEMES.items()):
        for i in range(20):
            item_ids.append(f"{topic_uuid}-{i}")
            gtl_ids.append("01000000")
            documents.append(" ".join(words + FILLER))
            topic_uuids.append(topic_uuid)
            is_outliers.append(False)
            cluster_ids.append(topic_index)
            assignments.append("fit")

    for i in range(10):
        item_ids.append(f"outlier-{i}")
        gtl_ids.append("01000000")
        documents.append(f"mot_rare_{i}")
        topic_uuids.append("outlier-uuid")
        is_outliers.append(True)
        cluster_ids.append(-1)
        assignments.append("fit")

    documents_df = pl.DataFrame(
        {"item_id": item_ids, "gtl_id": gtl_ids, "document": documents}
    )
    clusters_df = pl.DataFrame(
        {
            "item_id": item_ids,
            "topic_uuid": topic_uuids,
            "is_outlier": is_outliers,
            "cluster_id": cluster_ids,
            "assignment": assignments,
        }
    )

    documents_path = tmp_path / "documents.parquet"
    clusters_path = tmp_path / "clusters.parquet"
    documents_df.write_parquet(documents_path)
    clusters_df.write_parquet(clusters_path)
    return documents_path, clusters_path


def test_ctfidf_command_writes_topics_and_item_topic(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    documents_path, clusters_path = _synthetic_corpus(tmp_path)
    monkeypatch.setattr(
        labelling_cli, "read_dataframe", lambda sql: _vocabulary_table()
    )

    topics_output = tmp_path / "topics.parquet"
    item_topic_output = tmp_path / "item_topic.parquet"

    result = runner.invoke(
        labelling_cli.app,
        [
            "ctfidf",
            "--documents-file-path",
            str(documents_path),
            "--clusters-file-path",
            str(clusters_path),
            "--topics-output-file-path",
            str(topics_output),
            "--item-topic-output-file-path",
            str(item_topic_output),
            "--split-level",
            "1",
            "--corpus-id",
            "01000000",
            "--corpus-label",
            "Littérature",
        ],
    )

    assert result.exit_code == 0, result.output
    topics = pl.read_parquet(topics_output)
    assert topics.height == 4
    outlier_row = topics.filter(pl.col("is_outlier")).row(0, named=True)
    assert outlier_row["label"] == "Non clusterisés"

    item_topic = pl.read_parquet(item_topic_output)
    assert item_topic.height == 70
    assert set(item_topic["corpus_id"].unique().to_list()) == {"01000000"}


def test_reassign_command_passthrough_when_disabled(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    documents_path, clusters_path = _synthetic_corpus(tmp_path)
    monkeypatch.setattr(
        labelling_cli, "read_dataframe", lambda sql: _vocabulary_table()
    )

    item_topic_path = tmp_path / "item_topic.parquet"
    pl.DataFrame(
        {
            "item_id": ["a"],
            "topic_uuid": ["t"],
            "is_outlier": [True],
            "assignment": ["fit"],
            "split_level": [1],
            "corpus_id": ["01000000"],
            "original_is_outlier": [True],
        }
    ).write_parquet(item_topic_path)

    output_path = tmp_path / "reassigned.parquet"
    result = runner.invoke(
        labelling_cli.app,
        [
            "reassign",
            "--documents-file-path",
            str(documents_path),
            "--clusters-file-path",
            str(clusters_path),
            "--item-topic-file-path",
            str(item_topic_path),
            "--output-file-path",
            str(output_path),
        ],
    )

    assert result.exit_code == 0, result.output
    result_df = pl.read_parquet(output_path)
    assert result_df["topic_uuid"].to_list() == ["t"]
    assert result_df["is_outlier"].to_list() == [True]


def test_reassign_command_moves_qualifying_outlier(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def _config_with_reassignment(name: str) -> object:
        config = real_load_config(name)
        config.labelling = config.labelling.model_copy(
            update={"reassign_outliers": True, "min_shared_words": 2}
        )
        return config

    monkeypatch.setattr(labelling_cli, "load_config", _config_with_reassignment)
    monkeypatch.setattr(
        labelling_cli, "read_dataframe", lambda sql: _vocabulary_table()
    )

    documents_path, clusters_path = _synthetic_corpus(tmp_path)
    documents = pl.read_parquet(documents_path)
    clusters = pl.read_parquet(clusters_path)

    documents = documents.with_columns(
        pl.when(pl.col("item_id") == "outlier-0")
        .then(pl.lit("recette cuisine plat ingredient"))
        .otherwise(pl.col("document"))
        .alias("document")
    )
    documents.write_parquet(documents_path)

    item_topic_path = tmp_path / "item_topic.parquet"
    clusters.select("item_id", "topic_uuid", "is_outlier", "assignment").with_columns(
        pl.lit(1).alias("split_level"),
        pl.lit("01000000").alias("corpus_id"),
        pl.col("is_outlier").alias("original_is_outlier"),
    ).write_parquet(item_topic_path)

    output_path = tmp_path / "reassigned.parquet"
    result = runner.invoke(
        labelling_cli.app,
        [
            "reassign",
            "--documents-file-path",
            str(documents_path),
            "--clusters-file-path",
            str(clusters_path),
            "--item-topic-file-path",
            str(item_topic_path),
            "--output-file-path",
            str(output_path),
        ],
    )

    assert result.exit_code == 0, result.output
    result_df = pl.read_parquet(output_path)
    row = result_df.filter(pl.col("item_id") == "outlier-0").row(0, named=True)
    assert row["assignment"] == "reassigned"
    assert row["is_outlier"] is False
    assert row["original_is_outlier"] is True
    assert row["topic_uuid"] == "topic-0"


def test_export_command_full_refit_truncates(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    topics_path = tmp_path / "topics.parquet"
    item_topic_path = tmp_path / "item_topic.parquet"
    pl.DataFrame({"topic_uuid": ["t0"], "label": ["x"]}).write_parquet(topics_path)
    pl.DataFrame({"item_id": ["1"], "topic_uuid": ["t0"]}).write_parquet(
        item_topic_path
    )

    write_dataframe_mock = MagicMock()
    merge_upsert_mock = MagicMock()
    log_config_mock = MagicMock()
    monkeypatch.setattr(labelling_cli, "write_dataframe", write_dataframe_mock)
    monkeypatch.setattr(labelling_cli, "merge_upsert", merge_upsert_mock)
    monkeypatch.setattr(labelling_cli, "log_config_to_run", log_config_mock)

    result = runner.invoke(
        labelling_cli.app,
        [
            "export",
            "--topics-file-path",
            str(topics_path),
            "--item-topic-file-path",
            str(item_topic_path),
            "--full-refit",
            "--run-id",
            "abc123",
        ],
    )

    assert result.exit_code == 0, result.output
    assert write_dataframe_mock.call_count == 2
    merge_upsert_mock.assert_not_called()
    log_config_mock.assert_called_once()


def test_export_command_incremental_merges(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    topics_path = tmp_path / "topics.parquet"
    item_topic_path = tmp_path / "item_topic.parquet"
    pl.DataFrame({"topic_uuid": ["t0"], "label": ["x"]}).write_parquet(topics_path)
    pl.DataFrame({"item_id": ["1"], "topic_uuid": ["t0"]}).write_parquet(
        item_topic_path
    )

    write_dataframe_mock = MagicMock()
    merge_upsert_mock = MagicMock()
    monkeypatch.setattr(labelling_cli, "write_dataframe", write_dataframe_mock)
    monkeypatch.setattr(labelling_cli, "merge_upsert", merge_upsert_mock)
    monkeypatch.setattr(labelling_cli, "log_config_to_run", MagicMock())

    result = runner.invoke(
        labelling_cli.app,
        [
            "export",
            "--topics-file-path",
            str(topics_path),
            "--item-topic-file-path",
            str(item_topic_path),
            "--run-id",
            "abc123",
        ],
    )

    assert result.exit_code == 0, result.output
    write_dataframe_mock.assert_not_called()
    assert merge_upsert_mock.call_count == 2
