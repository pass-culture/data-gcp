"""Tests for cli/extraction.py — no network calls needed.

`extract`/`merge`'s own orchestration only: the generalized
`QueryConfig.optional` handling (a missing/empty result is expected for an
optional target, a hard failure for any other). The raw QLever HTTP fetch +
retry client lives in src/extraction/qlever.py (tests in tests/utils/qlever_test.py);
the extraction logic built on top of it lives in
src/extraction/wikidata_extraction.py (tests in
tests/utils/wikidata_extraction_test.py); `merge`'s own merge_data/
postprocess_data logic lives in src/extraction/wikidata_merge.py (tests in
tests/utils/wikidata_merge_test.py).
"""

import os

import pandas as pd
import pytest

import cli.extraction as wikidata_cli
from cli.extraction import extract, merge
from src.extraction import wikidata_extraction
from src.extraction.wikidata_config import QUERY_CONFIGS, QueryConfig


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

    def test_empty_result_is_skipped_not_raised(self, tmp_path, monkeypatch):
        # A real QLever CSV response always has a string-typed wikidata_id column
        # even with zero matching rows — a bare pd.DataFrame({"wikidata_id": []})
        # would infer float64 and break extract_wikidata_id's .str accessor before
        # the emptiness check even runs.
        monkeypatch.setattr(
            wikidata_extraction,
            "fetch_wikidata_qlever_csv",
            lambda _query, **_kwargs: pd.DataFrame(
                {"wikidata_id": pd.Series([], dtype="object")}
            ),
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
            wikidata_extraction,
            "fetch_wikidata_qlever_csv",
            lambda _query, **_kwargs: pd.DataFrame(
                {"wikidata_id": pd.Series([], dtype="object")}
            ),
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
