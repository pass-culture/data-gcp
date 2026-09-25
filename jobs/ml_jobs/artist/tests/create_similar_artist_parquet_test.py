import numpy as np
import pandas as pd
import pytest

from cli.create_similar_artist_parquet import (
    ARTIST_ID_KEY,
    ARTIST_NAME_KEY,
    COMBINED_SCORE_KEY,
    RANK_ALPHA_CONSTANT,
    RANK_ITEM_KEY,
    RANK_KEY,
    RANK_SEMANTIC_KEY,
    RAW_COMBINED_SCORE_KEY,
    TT_COEFFICIENT,
    format_results_df,
    merge_search_results,
)


def _make_search_df(artist_ids: list, artist_names: list, ranks: list) -> pd.DataFrame:
    """Helper to build a minimal search results DataFrame."""
    return pd.DataFrame(
        {
            ARTIST_ID_KEY: artist_ids,
            ARTIST_NAME_KEY: artist_names,
            RANK_KEY: ranks,
        }
    )


class TestMergeSearchResults:
    def test_basic_merge_both_present(self):
        """Both search methods return the same artists — no NaN ranks expected."""
        semantic_df = _make_search_df(["a1", "a2", "a3"], ["A", "B", "C"], [0, 1, 2])
        item_df = _make_search_df(["a1", "a2", "a3"], ["A", "B", "C"], [2, 0, 1])

        result = merge_search_results(semantic_df, item_df)

        assert set(result.columns) >= {
            ARTIST_ID_KEY,
            ARTIST_NAME_KEY,
            RANK_SEMANTIC_KEY,
            RANK_ITEM_KEY,
            "semantic_rank_score",
            "item_rank_score",
            RAW_COMBINED_SCORE_KEY,
            COMBINED_SCORE_KEY,
        }
        assert len(result) == 3
        # No NaN values when both sides have data
        assert result[RANK_SEMANTIC_KEY].notna().all()
        assert result[RANK_ITEM_KEY].notna().all()

    def test_outer_join_fills_missing_item_ranks(self):
        """Artists only in semantic results get item_rank_score = 0."""
        semantic_df = _make_search_df(["a1", "a2"], ["A", "B"], [0, 1])
        item_df = _make_search_df(["a1"], ["A"], [0])

        result = merge_search_results(semantic_df, item_df)

        assert len(result) == 2
        a2_row = result.loc[result[ARTIST_ID_KEY] == "a2"].iloc[0]
        assert np.isclose(a2_row["item_rank_score"], 0.0)

    def test_outer_join_fills_missing_semantic_ranks(self):
        """Artists only in item results get semantic_rank_score = 0."""
        semantic_df = _make_search_df(["a1"], ["A"], [0])
        item_df = _make_search_df(["a1", "a3"], ["A", "C"], [0, 1])

        result = merge_search_results(semantic_df, item_df)

        assert len(result) == 2
        a3_row = result.loc[result[ARTIST_ID_KEY] == "a3"].iloc[0]
        assert np.isclose(a3_row["semantic_rank_score"], 0.0)

    def test_combined_score_formula(self):
        """Verify the combined score formula against manual calculation."""
        semantic_df = _make_search_df(["a1"], ["A"], [0])
        item_df = _make_search_df(["a1"], ["A"], [0])

        result = merge_search_results(semantic_df, item_df)
        row = result.iloc[0]

        expected_semantic = 1.0 / (RANK_ALPHA_CONSTANT + 0)
        expected_item = 1.0 / (RANK_ALPHA_CONSTANT + 0)
        expected_raw = expected_semantic + TT_COEFFICIENT * expected_item
        expected_combined = expected_raw / ((1 + TT_COEFFICIENT) / RANK_ALPHA_CONSTANT)

        assert row["semantic_rank_score"] == pytest.approx(expected_semantic)
        assert row["item_rank_score"] == pytest.approx(expected_item)
        assert row[RAW_COMBINED_SCORE_KEY] == pytest.approx(expected_raw)
        assert row[COMBINED_SCORE_KEY] == pytest.approx(expected_combined)

    def test_normalized_score_is_at_most_one(self):
        """The best possible rank (0, 0) should yield a normalized score of 1.0."""
        semantic_df = _make_search_df(["a1"], ["A"], [0])
        item_df = _make_search_df(["a1"], ["A"], [0])

        result = merge_search_results(semantic_df, item_df)

        assert result[COMBINED_SCORE_KEY].iloc[0] == pytest.approx(1.0)

    def test_normalized_score_decreases_with_worse_rank(self):
        """Higher rank values should produce lower combined scores."""
        semantic_df = _make_search_df(["a1", "a2"], ["A", "B"], [0, 100])
        item_df = _make_search_df(["a1", "a2"], ["A", "B"], [0, 100])

        result = merge_search_results(semantic_df, item_df)

        scores = result.set_index(ARTIST_ID_KEY)[COMBINED_SCORE_KEY]
        assert scores["a1"] > scores["a2"]

    def test_empty_item_df(self):
        """When item_df is empty, all item_rank_scores should be 0."""
        semantic_df = _make_search_df(["a1", "a2"], ["A", "B"], [0, 1])
        item_df = pd.DataFrame(columns=[ARTIST_ID_KEY, ARTIST_NAME_KEY, RANK_KEY])

        result = merge_search_results(semantic_df, item_df)

        assert len(result) == 2
        assert (np.isclose(result["item_rank_score"], 0.0)).all()
        # Semantic-only scores should still be positive
        assert (result["semantic_rank_score"] > 0).all()

    def test_tt_coefficient_weighting(self):
        """Verify that item_rank_score is weighted by TT_COEFFICIENT in raw score."""
        semantic_df = _make_search_df(["a1"], ["A"], [10])
        item_df = _make_search_df(["a1"], ["A"], [10])

        result = merge_search_results(semantic_df, item_df)
        row = result.iloc[0]

        np.testing.assert_approx_equal(
            row[RAW_COMBINED_SCORE_KEY],
            row["semantic_rank_score"] + TT_COEFFICIENT * row["item_rank_score"],
        )


class TestFormatResultsDf:
    artist_0_name = "Artist 0"

    @pytest.fixture()
    def merged_df(self) -> pd.DataFrame:
        """A realistic merged result DataFrame for artist 'query_id'."""
        ids = [f"a{i}" for i in range(12)]
        names = [f"Artist {i}" for i in range(12)]
        scores = list(np.linspace(1.0, 0.1, 12))
        return pd.DataFrame(
            {
                ARTIST_ID_KEY: ids,
                ARTIST_NAME_KEY: names,
                COMBINED_SCORE_KEY: scores,
            }
        )

    def test_excludes_query_artist(self, merged_df: pd.DataFrame):
        """The query artist itself should not appear in the results."""
        result = format_results_df(merged_df, "a0", self.artist_0_name)
        assert "a0" not in result[f"{ARTIST_ID_KEY}_match"].values

    def test_returns_at_most_10(self, merged_df: pd.DataFrame):
        """Output should contain at most 10 rows."""
        result = format_results_df(merged_df, "a0", self.artist_0_name)
        assert len(result) <= 10

    def test_rank_combined_is_one_based(self, merged_df: pd.DataFrame):
        """The rank_combined column should start at 1."""
        result = format_results_df(merged_df, "a0", self.artist_0_name)
        assert result[f"{RANK_KEY}_combined"].iloc[0] == 1
        assert result[f"{RANK_KEY}_combined"].iloc[-1] == len(result)

    def test_sorted_by_combined_score_descending(self, merged_df: pd.DataFrame):
        """Rows should be ordered by combined_score from highest to lowest."""
        result = format_results_df(merged_df, "a0", self.artist_0_name)
        scores = result[COMBINED_SCORE_KEY].values
        assert list(scores) == sorted(scores, reverse=True)

    def test_columns_renamed(self, merged_df: pd.DataFrame):
        """Original ID/name columns should be renamed with _match suffix."""
        result = format_results_df(merged_df, "a0", self.artist_0_name)
        assert f"{ARTIST_ID_KEY}_match" in result.columns
        assert f"{ARTIST_NAME_KEY}_match" in result.columns

    def test_query_artist_columns_added(self, merged_df: pd.DataFrame):
        """The query artist's ID and name should be present on every row."""
        result = format_results_df(merged_df, "a0", self.artist_0_name)
        assert (result[ARTIST_ID_KEY] == "a0").all()
        assert (result[ARTIST_NAME_KEY] == self.artist_0_name).all()

    def test_fewer_than_10_candidates(self):
        """When fewer than 10 candidates exist, return all of them."""
        df = pd.DataFrame(
            {
                ARTIST_ID_KEY: ["query", "a1", "a2"],
                ARTIST_NAME_KEY: ["Q", "A", "B"],
                COMBINED_SCORE_KEY: [0.9, 0.8, 0.7],
            }
        )
        result = format_results_df(df, "query", "Q")
        assert len(result) == 2

    def test_all_same_id_as_query_returns_empty(self):
        """If every row is the query artist, the result should be empty."""
        df = pd.DataFrame(
            {
                ARTIST_ID_KEY: ["query", "query"],
                ARTIST_NAME_KEY: ["Q", "Q"],
                COMBINED_SCORE_KEY: [0.9, 0.8],
            }
        )
        result = format_results_df(df, "query", "Q")
        assert len(result) == 0

    def test_empty_input(self):
        """An empty DataFrame should produce an empty result."""
        df = pd.DataFrame(columns=[ARTIST_ID_KEY, ARTIST_NAME_KEY, COMBINED_SCORE_KEY])
        result = format_results_df(df, "query", "Q")
        assert len(result) == 0
