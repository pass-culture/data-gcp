"""Tests for cli/extract_from_wikidata.py — no network calls needed."""

import pandas as pd
import pytest

from cli.extract_from_wikidata import (
    extract_wikidata_id,
    merge_data,
    merge_music_artist_ids,
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
        wiki_ids = df["wikidata_id"].unique()
        merged = merge_data([df], {"music": wiki_ids})

        assert "music" in merged.columns
        assert merged["music"].dtype == bool or merged["music"].dtype == object

    def test_deduplicates_on_wikidata_id(self):
        df = _make_raw_df().pipe(extract_wikidata_id)
        # Duplicate the dataframe — should be deduplicated
        merged = merge_data([df, df], {"music": df["wikidata_id"].unique()})
        assert len(merged) == len(df)


class TestMergeMusicArtistIds:
    def test_adds_id_columns(self):
        df = _make_raw_df().pipe(extract_wikidata_id)
        ids_df = _make_ids_df().pipe(extract_wikidata_id)
        result = merge_music_artist_ids(df, ids_df)

        for col in NEW_ID_COLUMNS:
            assert col in result.columns, f"Column '{col}' missing after merge"

    def test_left_join_preserves_all_main_rows(self):
        """Artists without IDs in ids_df must still appear in the result."""
        df = _make_raw_df().pipe(extract_wikidata_id)
        # ids_df only has Q1
        ids_df = _make_ids_df(
            **{
                "wikidata_id": ["https://www.wikidata.org/entity/Q1"],
                "spotify_id": ["abc"],
                "isni_id": ["0000"],
                "apple_music_id": [None],
                "deezer_id": [None],
                "genius_id": [None],
                "soundcloud_id": [None],
                "matching_score": [2],
            }
        ).pipe(extract_wikidata_id)

        result = merge_music_artist_ids(df, ids_df)
        assert len(result) == len(df)
        q2_row = result[result["wikidata_id"] == "Q2"].iloc[0]
        assert pd.isna(q2_row["spotify_id"])

    def test_id_values_correctly_joined(self):
        df = _make_raw_df().pipe(extract_wikidata_id)
        ids_df = _make_ids_df().pipe(extract_wikidata_id)
        result = merge_music_artist_ids(df, ids_df)

        q1 = result[result["wikidata_id"] == "Q1"].iloc[0]
        assert q1["spotify_id"] == "3TVXtAsR1Inumwj472S9r4"
        assert q1["isni_id"] == "0000000121239645"

        q2 = result[result["wikidata_id"] == "Q2"].iloc[0]
        assert q2["isni_id"] == "0000000121239646"
        assert pd.isna(q2["spotify_id"])


class TestPostprocessData:
    @pytest.fixture()
    def processed(self) -> pd.DataFrame:
        # Simulate the full pipeline: main query + IDs merge
        df = _make_raw_df().pipe(extract_wikidata_id)
        ids_df = _make_ids_df().pipe(extract_wikidata_id)
        df_with_ids = merge_music_artist_ids(df, ids_df)
        return postprocess_data(df_with_ids)

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
