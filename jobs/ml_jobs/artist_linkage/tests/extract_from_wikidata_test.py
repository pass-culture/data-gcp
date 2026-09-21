"""Tests for cli/extract_from_wikidata.py — no network calls needed.

`extract`/`merge`'s own orchestration only: checkpoint resume, target
validation, wiring fetch_discovery/hydrate_batch together, and the generalized
`QueryConfig.optional` handling (a missing/empty result is expected for an
optional target, a hard failure for any other). The raw QLever HTTP fetch +
retry client lives in src/utils/qlever.py (tests in tests/utils/qlever_test.py);
the two-pass discovery+hydration logic built on top of it lives in
src/utils/wikidata_extraction.py (tests in
tests/utils/wikidata_extraction_test.py); `merge`'s own merge_data/
postprocess_data logic lives in src/utils/wikidata_merge.py (tests in
tests/utils/wikidata_merge_test.py); checkpoint file I/O lives in
src/utils/wikidata_checkpoint.py (tests in
tests/utils/wikidata_checkpoint_test.py).
"""

import os
from unittest.mock import patch

import pandas as pd
import pytest
import requests

import cli.extract_from_wikidata as wikidata_cli
from cli.extract_from_wikidata import extract, merge
from src.utils import wikidata_checkpoint
from src.wikidata_config import QUERY_CONFIGS, QueryConfig


def _make_discovery_df(ids: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "wikidata_id": [f"https://www.wikidata.org/entity/{qid}" for qid in ids],
            "gkg_id": [f"/g/{qid}" for qid in ids],
            "matching_score": [1] * len(ids),
        }
    )


def _make_hydration_df(ids: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "wikidata_id": [f"https://www.wikidata.org/entity/{qid}" for qid in ids],
            "artist_name_fr": [f"Name {qid}" for qid in ids],
        }
    )


def _make_stripped_raw_df(ids: list[str]) -> pd.DataFrame:
    """A minimal raw dataframe as `extract` would have saved it (wikidata_id
    already stripped of its URI prefix), with every column merge_data/
    postprocess_data need."""
    return pd.DataFrame(
        {
            "wikidata_id": ids,
            "artist_name_fr": [f"Nom {qid}" for qid in ids],
            "artist_name_en": [f"Name {qid}" for qid in ids],
            "artist_description": ["desc"] * len(ids),
            "wikipedia_url": [None] * len(ids),
            "img": [None] * len(ids),
            "aliases_fr": [""] * len(ids),
            "aliases_en": [""] * len(ids),
        }
    )


class TestExtractTwoPassCheckpointing:
    """End-to-end: a mid-run failure on one Pass 2 batch must leave the other,
    already-hydrated batch checkpointed, and a subsequent `extract` call for the
    same target must resume from it instead of redoing Pass 1 or that batch."""

    QUERY_NAME = "test_two_pass"

    @pytest.fixture(autouse=True)
    def _register_test_config(self, monkeypatch):
        monkeypatch.setitem(
            QUERY_CONFIGS,
            self.QUERY_NAME,
            QueryConfig(
                template="extract_discovery.rq.j2",
                entity_types=["wd:Q5"],
                id_properties=[],
                hydration_batch_size=2,
            ),
        )

    def test_resumes_after_a_mid_run_failure(self, tmp_path, monkeypatch):
        checkpoint_root = str(tmp_path / "checkpoints")
        monkeypatch.setattr(wikidata_checkpoint, "CHECKPOINT_ROOT_DIR", checkpoint_root)
        monkeypatch.setattr(wikidata_cli, "clear_qlever_cache", lambda: None)

        discovery_df = _make_discovery_df(["Q1", "Q2", "Q3", "Q4"])
        # hydration_batch_size=2 -> two batches: [Q1, Q2] and [Q3, Q4]

        def hydrate_batch_fails_on_second_batch(query_name, batch, dropped_ids):
            if batch == ["Q3", "Q4"]:
                raise requests.RequestException("simulated QLever outage")
            return [_make_hydration_df(batch)]

        output_path = str(tmp_path / "out.parquet")

        with (
            patch.object(
                wikidata_cli, "fetch_discovery", return_value=discovery_df
            ) as mock_fetch_discovery,
            patch.object(
                wikidata_cli,
                "hydrate_batch",
                side_effect=hydrate_batch_fails_on_second_batch,
            ) as mock_hydrate,
            pytest.raises(requests.RequestException),
        ):
            extract(query_name=self.QUERY_NAME, output_file_path=output_path)

        assert mock_fetch_discovery.call_count == 1
        assert mock_hydrate.call_count == 2

        checkpoint_dir = os.path.join(checkpoint_root, self.QUERY_NAME)
        assert wikidata_checkpoint.load_processed_batches(checkpoint_dir) == {0}
        assert wikidata_checkpoint.load_batch_checkpoint(checkpoint_dir, 0) is not None
        assert wikidata_checkpoint.load_batch_checkpoint(checkpoint_dir, 1) is None

        with (
            patch.object(
                wikidata_cli, "fetch_discovery", return_value=discovery_df
            ) as mock_fetch_discovery_2,
            patch.object(
                wikidata_cli,
                "hydrate_batch",
                return_value=[_make_hydration_df(["Q3", "Q4"])],
            ) as mock_hydrate_2,
        ):
            extract(query_name=self.QUERY_NAME, output_file_path=output_path)

        assert mock_fetch_discovery_2.call_count == 0  # resumed from checkpoint
        mock_hydrate_2.assert_called_once_with(self.QUERY_NAME, ["Q3", "Q4"], [])

        result = pd.read_parquet(output_path)
        assert sorted(result["wikidata_id"]) == ["Q1", "Q2", "Q3", "Q4"]

        # Cleared only after a fully successful run.
        assert not os.path.isdir(checkpoint_dir)


class TestExtractOptionalTarget:
    """QueryConfig.optional generalizes the "this target may legitimately come
    back empty" case beyond music_ids specifically — extract must skip saving a
    raw file (not raise) for any target configured that way."""

    QUERY_NAME = "test_optional"

    @pytest.fixture(autouse=True)
    def _register_test_config(self, monkeypatch):
        monkeypatch.setitem(
            QUERY_CONFIGS,
            self.QUERY_NAME,
            QueryConfig(
                template="extract_artists.rq.j2",
                entity_types=["wd:Q5"],
                optional=True,
            ),
        )
        monkeypatch.setattr(wikidata_cli, "clear_qlever_cache", lambda: None)

    def test_empty_result_is_skipped_not_raised(self, tmp_path, monkeypatch):
        # A real QLever CSV response always has a string-typed wikidata_id column
        # even with zero matching rows — a bare pd.DataFrame({"wikidata_id": []})
        # would infer float64 and break extract_wikidata_id's .str accessor before
        # the emptiness check even runs.
        monkeypatch.setattr(
            wikidata_cli,
            "fetch_wikidata_qlever_csv",
            lambda _query: pd.DataFrame({"wikidata_id": pd.Series([], dtype="object")}),
        )
        output_path = str(tmp_path / "out.parquet")

        extract(
            query_name=self.QUERY_NAME, output_file_path=output_path
        )  # must not raise

        assert not os.path.exists(output_path)

    def test_non_optional_target_still_raises_on_empty(self, tmp_path, monkeypatch):
        monkeypatch.setitem(
            QUERY_CONFIGS,
            "test_required",
            QueryConfig(
                template="extract_artists.rq.j2", entity_types=["wd:Q5"], optional=False
            ),
        )
        monkeypatch.setattr(
            wikidata_cli,
            "fetch_wikidata_qlever_csv",
            lambda _query: pd.DataFrame({"wikidata_id": pd.Series([], dtype="object")}),
        )
        output_path = str(tmp_path / "out.parquet")

        with pytest.raises(ValueError, match="No data retrieved for test_required"):
            extract(query_name="test_required", output_file_path=output_path)


class TestMergeOptionalTargets:
    """merge's own FileNotFoundError handling must key off QueryConfig.optional
    for whichever targets are configured that way, not a hardcoded target name."""

    def test_missing_optional_target_file_is_skipped(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            wikidata_cli,
            "QUERY_CONFIGS",
            {
                "required_target": QueryConfig(
                    template="extract_artists.rq.j2", entity_types=["wd:Q5"]
                ),
                "optional_target": QueryConfig(
                    template="extract_artists.rq.j2",
                    entity_types=["wd:Q5"],
                    optional=True,
                ),
            },
        )
        input_dir = tmp_path / "raw"
        input_dir.mkdir()
        _make_stripped_raw_df(["Q1"]).to_parquet(
            str(input_dir / "required_target.parquet")
        )
        output_path = str(tmp_path / "out.parquet")

        merge(input_dir_path=str(input_dir), output_file_path=output_path)

        assert os.path.exists(output_path)

    def test_missing_non_optional_target_file_raises(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            wikidata_cli,
            "QUERY_CONFIGS",
            {
                "required_target": QueryConfig(
                    template="extract_artists.rq.j2", entity_types=["wd:Q5"]
                ),
            },
        )
        input_dir = tmp_path / "raw"
        input_dir.mkdir()
        output_path = str(tmp_path / "out.parquet")

        with pytest.raises(
            ValueError, match="Missing raw extraction for required_target"
        ):
            merge(input_dir_path=str(input_dir), output_file_path=output_path)
