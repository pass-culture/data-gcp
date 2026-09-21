"""Tests for cli/extract_from_wikidata.py — no network calls needed."""

from unittest.mock import Mock, patch

import pandas as pd
import pytest
import requests

from cli.extract_from_wikidata import (
    MUSIC_IDS_KEY,
    QLeverQueryTooExpensive,
    _clear_qlever_cache_once,
    _is_cost_rejection,
    clear_qlever_cache,
    extract_wikidata_id,
    fetch_wikidata_qlever_csv,
    fetch_wikidata_qlever_csv_batch,
    merge_data,
    postprocess_data,
)

NEW_ID_COLUMNS = [
    "spotify_id",
    "isni_id",
    "apple_music_id",
    "deezer_id",
    "genius_id",
    "soundcloud_id",
]


def _make_raw_df(**kwargs) -> pd.DataFrame:
    """Build a minimal raw DataFrame as returned by fetch_wikidata_qlever_csv (main queries)."""
    defaults = {
        "wikidata_id": [
            "https://www.wikidata.org/entity/Q1",
            "https://www.wikidata.org/entity/Q2",
        ],
        "artist_name_fr": ["Artiste Un", "Artiste Deux"],
        "artist_name_en": ["Artist One", "Artist Two"],
        "artist_description": ["desc1", "desc2"],
        "wikipedia_url": ["https://fr.wikipedia.org/wiki/Un", None],
        "img": ["https://commons.wikimedia.org/img1.jpg", None],
        "gkg_id": ["/g/1234", None],
        "aliases_fr": ["Un|Alias FR", ""],
        "aliases_en": ["One|Alias EN", "Two"],
        "professions": ["musicien", "chanteur"],
        "genres": ["rock", "pop"],
        "languages_spoken": ["français", "anglais"],
        "birth_date_val": ["1980-01-01", "1990-06-15"],
    }
    defaults.update(kwargs)
    return pd.DataFrame(defaults)


def _make_ids_df(**kwargs) -> pd.DataFrame:
    """Build a minimal IDs DataFrame as returned by the music_artist_ids query."""
    defaults = {
        "wikidata_id": [
            "https://www.wikidata.org/entity/Q1",
            "https://www.wikidata.org/entity/Q2",
        ],
        "spotify_id": ["3TVXtAsR1Inumwj472S9r4", None],
        "isni_id": ["0000000121239645", "0000000121239646"],
        "apple_music_id": ["12345", None],
        "deezer_id": ["56789", None],
        "genius_id": ["genius-q1", None],
        "soundcloud_id": ["soundcloud-q1", None],
        "matching_score": [4, 1],
    }
    defaults.update(kwargs)
    return pd.DataFrame(defaults)


class TestExtractWikidataId:
    def test_strips_wikidata_uri_prefix(self):
        df = pd.DataFrame(
            {
                "wikidata_id": [
                    "https://www.wikidata.org/entity/Q42",
                    "https://www.wikidata.org/entity/Q123",
                ]
            }
        )
        result = extract_wikidata_id(df)
        assert list(result["wikidata_id"]) == ["Q42", "Q123"]

    def test_does_not_alter_other_columns(self):
        df = pd.DataFrame(
            {
                "wikidata_id": ["https://www.wikidata.org/entity/Q1"],
                "artist_name_fr": ["Test"],
            }
        )
        result = extract_wikidata_id(df)
        assert result["artist_name_fr"].iloc[0] == "Test"


class TestMergeData:
    def test_adds_boolean_source_columns(self):
        df = _make_raw_df().pipe(extract_wikidata_id)
        merged = merge_data({"music": df})

        assert "music" in merged.columns
        assert merged["music"].dtype == bool or merged["music"].dtype == object

    def test_deduplicates_on_wikidata_id(self):
        df = _make_raw_df().pipe(extract_wikidata_id)
        # Duplicate the dataframe — should be deduplicated
        merged = merge_data({"music": pd.concat([df, df])})
        assert len(merged) == len(df)

    def test_merges_music_ids_when_present(self):
        df = _make_raw_df().pipe(extract_wikidata_id)
        ids_df = _make_ids_df().pipe(extract_wikidata_id)
        merged = merge_data({"music": df, MUSIC_IDS_KEY: ids_df})

        assert "spotify_id" in merged.columns
        assert len(merged) == len(df)

    def test_music_ids_matching_score_renamed_to_avoid_conflict(self):
        """matching_score from music_ids must be merged into a single matching_score column."""
        df = _make_raw_df().pipe(extract_wikidata_id)
        ids_df = _make_ids_df().pipe(extract_wikidata_id)
        merged = merge_data({"music": df, MUSIC_IDS_KEY: ids_df})

        assert "matching_score" in merged.columns
        assert "music_ids_matching_score" not in merged.columns
        assert "matching_score_x" not in merged.columns
        assert "matching_score_y" not in merged.columns

    def test_skips_music_ids_merge_when_absent(self):
        df = _make_raw_df().pipe(extract_wikidata_id)
        merged = merge_data({"music": df})

        assert "spotify_id" not in merged.columns


class TestPostprocessData:
    @pytest.fixture
    def processed(self) -> pd.DataFrame:
        # Simulate the full pipeline: main query + IDs merge
        df = _make_raw_df().pipe(extract_wikidata_id)
        ids_df = _make_ids_df().pipe(extract_wikidata_id)
        return postprocess_data(merge_data({"music": df, MUSIC_IDS_KEY: ids_df}))

    def test_has_alias_column(self, processed):
        assert "alias" in processed.columns

    def test_has_raw_alias_column(self, processed):
        assert "raw_alias" in processed.columns

    def test_new_id_columns_present(self, processed):
        """All new external ID columns must survive the postprocess pipeline."""
        for col in NEW_ID_COLUMNS:
            assert col in processed.columns, f"Column '{col}' missing after postprocess"

    def test_no_empty_aliases(self, processed):
        assert (processed["alias"] != "").all()
        assert processed["alias"].notna().all()

    def test_img_uses_https(self, processed):
        imgs = processed["img"].dropna()
        assert imgs.str.startswith("https://").all()

    def test_spotify_id_value_preserved(self, processed):
        """The Spotify ID of Q1 must be present in the output rows for that wikidata_id."""
        q1_rows = processed[processed["wikidata_id"] == "Q1"]
        assert not q1_rows.empty
        assert (q1_rows["spotify_id"] == "3TVXtAsR1Inumwj472S9r4").all()

    def test_isni_id_value_preserved(self, processed):
        q2_rows = processed[processed["wikidata_id"] == "Q2"]
        assert not q2_rows.empty
        assert (q2_rows["isni_id"] == "0000000121239646").all()

    def test_null_ids_remain_null(self, processed):
        """Optional IDs that are None in input must remain NaN (not become strings)."""
        q2_rows = processed[processed["wikidata_id"] == "Q2"]

        assert q2_rows["spotify_id"].isna().all()
        assert q2_rows["deezer_id"].isna().all()
        assert q2_rows["genius_id"].isna().all()
        assert q2_rows["soundcloud_id"].isna().all()


def _mock_response(status_code: int, text: str = "", json_data=None) -> Mock:
    response = Mock(spec=requests.Response)
    response.status_code = status_code
    response.text = text
    if json_data is not None:
        response.json.return_value = json_data
    else:
        response.json.side_effect = ValueError("no JSON body")
    return response


@pytest.fixture(autouse=True)
def _no_real_sleep():
    """Tenacity's default sleep strategy is a real time.sleep — patch it out so
    tests exercising the retry decorator (stop_after_attempt(3), exponential
    backoff up to 40s) don't actually block for tens of seconds."""
    with patch("tenacity.nap.time.sleep"):
        yield


class TestQleverRetry:
    """cli/extract_from_wikidata.py's qlever_retry (tenacity) decorates every
    QLever HTTP call: 3 attempts, exponential backoff, retrying only on
    requests.RequestException — see qlever_retry's docstring/comment."""

    def test_clear_cache_once_raises_after_exhausting_retries(self):
        with (
            patch(
                "cli.extract_from_wikidata.requests.get",
                return_value=_mock_response(500, "boom"),
            ) as mock_get,
            pytest.raises(requests.RequestException),
        ):
            _clear_qlever_cache_once()
        assert mock_get.call_count == 3

    def test_clear_cache_succeeds_without_retry(self):
        with patch(
            "cli.extract_from_wikidata.requests.get",
            return_value=_mock_response(200),
        ) as mock_get:
            _clear_qlever_cache_once()
        assert mock_get.call_count == 1

    def test_clear_qlever_cache_swallows_failure_after_retries(self):
        """The outer clear_qlever_cache must NOT raise even after all retries are
        exhausted — cache clearing is best-effort, extraction should proceed."""
        with patch(
            "cli.extract_from_wikidata.requests.get",
            return_value=_mock_response(500, "boom"),
        ) as mock_get:
            clear_qlever_cache()  # must not raise
        assert mock_get.call_count == 3

    def test_fetch_csv_retries_then_succeeds(self):
        responses = [
            _mock_response(500, "transient"),
            _mock_response(500, "transient"),
            _mock_response(200, "wikidata_id\nQ1\n"),
        ]
        with patch(
            "cli.extract_from_wikidata.requests.post", side_effect=responses
        ) as mock_post:
            df = fetch_wikidata_qlever_csv("SELECT ...")
        assert mock_post.call_count == 3
        assert list(df["wikidata_id"]) == ["Q1"]

    def test_fetch_csv_raises_after_exhausting_retries(self):
        with (
            patch(
                "cli.extract_from_wikidata.requests.post",
                return_value=_mock_response(500, "boom"),
            ) as mock_post,
            pytest.raises(requests.RequestException),
        ):
            fetch_wikidata_qlever_csv("SELECT ...")
        assert mock_post.call_count == 3

    def test_fetch_csv_batch_raises_cost_rejection_without_retrying(self):
        """A genuine cost rejection (QLeverQueryTooExpensive) must propagate
        immediately, on the first attempt — retrying an identical too-expensive
        query would just fail the same way every time; the caller bisects
        instead (see hydrate_batch)."""
        response = _mock_response(429, json_data={"exception": "Query timed out"})
        with (
            patch(
                "cli.extract_from_wikidata.requests.post", return_value=response
            ) as mock_post,
            pytest.raises(QLeverQueryTooExpensive),
        ):
            fetch_wikidata_qlever_csv_batch("SELECT ...")
        assert mock_post.call_count == 1

    def test_fetch_csv_batch_retries_transient_errors(self):
        responses = [
            _mock_response(503, "transient"),
            _mock_response(200, "wikidata_id\nQ1\n"),
        ]
        with patch(
            "cli.extract_from_wikidata.requests.post", side_effect=responses
        ) as mock_post:
            df = fetch_wikidata_qlever_csv_batch("SELECT ...")
        assert mock_post.call_count == 2
        assert list(df["wikidata_id"]) == ["Q1"]


class TestIsCostRejection:
    def test_true_for_429_with_timeout_phrase(self):
        response = _mock_response(429, json_data={"exception": "Query timed out"})
        assert _is_cost_rejection(response) is True

    def test_false_for_429_without_recognized_phrase(self):
        response = _mock_response(429, json_data={"exception": "rate limited"})
        assert _is_cost_rejection(response) is False

    def test_false_for_non_429_status(self):
        response = _mock_response(500, json_data={"exception": "timed out"})
        assert _is_cost_rejection(response) is False

    def test_false_when_body_is_not_json(self):
        response = _mock_response(429, text="not json")
        assert _is_cost_rejection(response) is False
