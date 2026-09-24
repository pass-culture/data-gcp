"""Tests for src/utils/wikidata_checkpoint.py — no network calls needed."""

import pandas as pd

from src.utils.wikidata_checkpoint import (
    checkpoint_dir_for,
    clear_checkpoint,
    load_batch_checkpoint,
    load_discovery_checkpoint,
    load_dropped_ids,
    load_processed_batches,
    mark_batch_processed,
    save_batch_checkpoint,
    save_discovery_checkpoint,
    save_dropped_ids,
)


def _make_df(ids: list[str]) -> pd.DataFrame:
    return pd.DataFrame({"wikidata_id": ids, "value": [f"v{qid}" for qid in ids]})


class TestCheckpointDirFor:
    def test_scopes_by_query_name(self):
        assert checkpoint_dir_for("gkg") != checkpoint_dir_for("movie")
        assert "gkg" in checkpoint_dir_for("gkg")


class TestDiscoveryCheckpoint:
    def test_absent_by_default(self, tmp_path):
        assert load_discovery_checkpoint(str(tmp_path)) is None

    def test_roundtrip(self, tmp_path):
        checkpoint_dir = str(tmp_path)
        df = _make_df(["Q1", "Q2"])
        save_discovery_checkpoint(checkpoint_dir, df)
        loaded = load_discovery_checkpoint(checkpoint_dir)
        assert loaded is not None
        assert list(loaded["wikidata_id"]) == ["Q1", "Q2"]

    def test_creates_checkpoint_dir_if_missing(self, tmp_path):
        checkpoint_dir = str(tmp_path / "not_yet_created")
        save_discovery_checkpoint(checkpoint_dir, _make_df(["Q1"]))
        assert load_discovery_checkpoint(checkpoint_dir) is not None


class TestBatchCheckpoint:
    def test_absent_by_default(self, tmp_path):
        assert load_batch_checkpoint(str(tmp_path), 0) is None

    def test_roundtrip(self, tmp_path):
        checkpoint_dir = str(tmp_path)
        df = _make_df(["Q1", "Q2"])
        save_batch_checkpoint(checkpoint_dir, 0, df)
        loaded = load_batch_checkpoint(checkpoint_dir, 0)
        assert loaded is not None
        assert list(loaded["wikidata_id"]) == ["Q1", "Q2"]

    def test_different_indexes_do_not_collide(self, tmp_path):
        checkpoint_dir = str(tmp_path)
        save_batch_checkpoint(checkpoint_dir, 0, _make_df(["Q1"]))
        save_batch_checkpoint(checkpoint_dir, 1, _make_df(["Q2"]))
        assert list(load_batch_checkpoint(checkpoint_dir, 0)["wikidata_id"]) == ["Q1"]
        assert list(load_batch_checkpoint(checkpoint_dir, 1)["wikidata_id"]) == ["Q2"]


class TestProcessedBatchesLog:
    def test_empty_by_default(self, tmp_path):
        assert load_processed_batches(str(tmp_path)) == set()

    def test_roundtrip(self, tmp_path):
        checkpoint_dir = str(tmp_path)
        mark_batch_processed(checkpoint_dir, 0)
        mark_batch_processed(checkpoint_dir, 2)
        assert load_processed_batches(checkpoint_dir) == {0, 2}

    def test_appends_without_losing_prior_entries(self, tmp_path):
        checkpoint_dir = str(tmp_path)
        mark_batch_processed(checkpoint_dir, 0)
        assert load_processed_batches(checkpoint_dir) == {0}
        mark_batch_processed(checkpoint_dir, 1)
        assert load_processed_batches(checkpoint_dir) == {0, 1}


class TestDroppedIds:
    def test_empty_by_default(self, tmp_path):
        assert load_dropped_ids(str(tmp_path)) == []

    def test_roundtrip(self, tmp_path):
        checkpoint_dir = str(tmp_path)
        save_dropped_ids(checkpoint_dir, ["Q1", "Q2"])
        assert load_dropped_ids(checkpoint_dir) == ["Q1", "Q2"]

    def test_overwrites_previous_value(self, tmp_path):
        checkpoint_dir = str(tmp_path)
        save_dropped_ids(checkpoint_dir, ["Q1"])
        save_dropped_ids(checkpoint_dir, ["Q1", "Q2"])
        assert load_dropped_ids(checkpoint_dir) == ["Q1", "Q2"]


class TestClearCheckpoint:
    def test_removes_checkpoint_dir(self, tmp_path):
        checkpoint_dir = str(tmp_path / "gkg")
        save_discovery_checkpoint(checkpoint_dir, _make_df(["Q1"]))
        assert load_discovery_checkpoint(checkpoint_dir) is not None

        clear_checkpoint(checkpoint_dir)

        assert load_discovery_checkpoint(checkpoint_dir) is None

    def test_noop_if_checkpoint_dir_missing(self, tmp_path):
        clear_checkpoint(str(tmp_path / "never_created"))  # must not raise
