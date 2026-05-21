import pandas as pd
import pytest

from src.clustering import (
    clusterize_offers,
    extract_cluster_metadata,
    get_cluster_metadata_representant,
    should_match_on_offer_names,
)
from src.interfaces import ClusterRepresentantMethod


class TestShouldMatchOnOfferNames:
    def test_non_spectacle_subcategory_matches_on_names(self):
        df = pd.DataFrame(
            {
                "description_match": [False, True, False],
                "name_match": [True, False, False],
                "image_match": [False, False, True],
            }
        )
        result = should_match_on_offer_names(df, "CONCERT")
        expected = pd.Series([True, True, True])
        pd.testing.assert_series_equal(result, expected)

    def test_spectacle_subcategory_ignores_name_match(self):
        df = pd.DataFrame(
            {
                "description_match": [False, True, False],
                "name_match": [True, True, True],
                "image_match": [False, False, True],
            }
        )
        result = should_match_on_offer_names(df, "SPECTACLE_REPRESENTATION")
        # name_match is ignored, only description_match and image_match matter
        expected = pd.Series([False, True, True])
        pd.testing.assert_series_equal(result, expected)

    def test_spectacle_subcategory_all_false(self):
        df = pd.DataFrame(
            {
                "description_match": [False],
                "name_match": [True],
                "image_match": [False],
            }
        )
        result = should_match_on_offer_names(df, "SPECTACLE_REPRESENTATION")
        expected = pd.Series([False])
        pd.testing.assert_series_equal(result, expected)


class TestClusterizeOffers:
    def _make_cross_df(self, rows):
        return pd.DataFrame(
            rows,
            columns=[
                "offer_id_1",
                "offer_id_2",
                "offer_subcategory_id_1",
                "partial_name_similarity",
                "description_similarity",
                "name_similarity",
                "image_similarity",
            ],
        )

    def test_basic_clustering(self):
        cross_df = self._make_cross_df(
            [
                # offer A and B match on description
                ["A", "B", "CONCERT", 80, 96, 50, 0.5],
                # offer B and C match on name
                ["B", "C", "CONCERT", 80, 50, 95, 0.5],
            ]
        )
        result = clusterize_offers(cross_df, "CONCERT")
        # A-B-C should form one cluster
        assert len(result) == 1
        assert result.iloc[0]["cluster"] == {"A", "B", "C"}
        assert result.iloc[0]["cluster_length"] == 3
        assert result.iloc[0]["subcategory_id"] == "CONCERT"

    def test_no_match_produces_empty(self):
        cross_df = self._make_cross_df(
            [
                # Below all thresholds
                ["A", "B", "CONCERT", 80, 50, 50, 0.5],
            ]
        )
        result = clusterize_offers(cross_df, "CONCERT")
        assert len(result) == 0

    def test_filters_by_subcategory(self):
        cross_df = self._make_cross_df(
            [
                ["A", "B", "CONCERT", 80, 96, 50, 0.5],
                ["C", "D", "CINEMA", 80, 96, 50, 0.5],
            ]
        )
        result = clusterize_offers(cross_df, "CONCERT")
        assert len(result) == 1
        assert result.iloc[0]["cluster"] == {"A", "B"}

    def test_filters_by_partial_name_threshold(self):
        cross_df = self._make_cross_df(
            [
                # partial_name_similarity below threshold (60)
                ["A", "B", "CONCERT", 50, 96, 95, 0.9],
            ]
        )
        result = clusterize_offers(cross_df, "CONCERT")
        assert len(result) == 0

    def test_two_separate_clusters(self):
        cross_df = self._make_cross_df(
            [
                ["A", "B", "CONCERT", 80, 96, 50, 0.5],
                ["C", "D", "CONCERT", 80, 96, 50, 0.5],
            ]
        )
        result = clusterize_offers(cross_df, "CONCERT")
        assert len(result) == 2
        clusters = list(result["cluster"])
        assert {"A", "B"} in clusters
        assert {"C", "D"} in clusters

    def test_image_match_creates_cluster(self):
        cross_df = self._make_cross_df(
            [
                ["A", "B", "CONCERT", 80, 50, 50, 0.9],
            ]
        )
        result = clusterize_offers(cross_df, "CONCERT")
        assert len(result) == 1
        assert result.iloc[0]["cluster"] == {"A", "B"}


class TestGetClusterMetadataRepresentant:
    def test_max_similarity_returns_highest(self):
        df = pd.DataFrame(
            {
                "offer_id": ["A", "B", "C"],
                "metric": [0.8, 0.95, 0.7],
            }
        )
        result = get_cluster_metadata_representant(
            df, "metric", ClusterRepresentantMethod.MAX_SIMILARITY
        )
        assert result == "B"

    def test_max_exact_similarity_returns_most_common_highest(self):
        # Two offers share metric=0.9; one has metric=0.95
        # Most common is 0.9 (count=2), so among those pick the first
        df = pd.DataFrame(
            {
                "offer_id": ["A", "B", "C"],
                "metric": [0.9, 0.95, 0.9],
            }
        )
        result = get_cluster_metadata_representant(
            df, "metric", ClusterRepresentantMethod.MAX_EXACT_SIMILARITY
        )
        assert result == "A"

    def test_max_exact_similarity_tie_in_counts(self):
        # All counts are 1 (all unique values), so most_common is all of them
        # biggest_similarity should be the max index value
        df = pd.DataFrame(
            {
                "offer_id": ["A", "B", "C"],
                "metric": [0.7, 0.9, 0.8],
            }
        )
        result = get_cluster_metadata_representant(
            df, "metric", ClusterRepresentantMethod.MAX_EXACT_SIMILARITY
        )
        assert result == "B"

    def test_returns_none_when_all_zero(self):
        df = pd.DataFrame(
            {
                "offer_id": ["A", "B"],
                "metric": [0.0, 0.0],
            }
        )
        result = get_cluster_metadata_representant(
            df, "metric", ClusterRepresentantMethod.MAX_SIMILARITY
        )
        assert result is None

    def test_returns_none_when_empty(self):
        df = pd.DataFrame({"offer_id": [], "metric": []})
        result = get_cluster_metadata_representant(
            df, "metric", ClusterRepresentantMethod.MAX_SIMILARITY
        )
        assert result is None

    def test_raises_on_unknown_method(self):
        df = pd.DataFrame({"offer_id": ["A"], "metric": [0.5]})
        with pytest.raises(ValueError, match="Unknown method"):
            get_cluster_metadata_representant(df, "metric", "unknown_method")


class TestExtractClusterMetadata:
    def test_extracts_metadata_from_cluster(self):
        raw_data_df = pd.DataFrame(
            {
                "offer_id": ["A", "B", "C"],
                "offer_name": ["Concert X", "Concert Y", "Concert Z"],
                "offer_description": ["Desc A", "Desc B", "Desc C"],
                "image_url": ["url_a", "url_b", "url_c"],
            }
        )
        cross_df = pd.DataFrame(
            {
                "offer_id_1": ["A", "A", "B"],
                "offer_id_2": ["B", "C", "C"],
                "full_name_similarity": [0.9, 0.8, 0.7],
                "full_description_similarity": [0.85, 0.95, 0.6],
                "image_similarity": [0.9, 0.9, 0.9],
            }
        )
        cluster_row = pd.Series({"cluster": {"A", "B", "C"}})

        result = extract_cluster_metadata(cluster_row, cross_df, raw_data_df)

        assert result["event_name"] is not None
        assert result["event_name"] in ["Concert X", "Concert Y", "Concert Z"]
        assert result["event_description"] is not None
        assert result["event_description"] in ["Desc A", "Desc B", "Desc C"]
        assert result["event_image_url"] is not None
        assert result["event_image_url"] in ["url_a", "url_b", "url_c"]

    def test_returns_none_when_no_similarities(self):
        raw_data_df = pd.DataFrame(
            {
                "offer_id": ["A", "B"],
                "offer_name": ["Name A", "Name B"],
                "offer_description": ["Desc A", "Desc B"],
                "image_url": ["url_a", "url_b"],
            }
        )
        cross_df = pd.DataFrame(
            {
                "offer_id_1": ["A"],
                "offer_id_2": ["B"],
                "full_name_similarity": [0.0],
                "full_description_similarity": [0.0],
                "image_similarity": [0.0],
            }
        )
        cluster_row = pd.Series({"cluster": {"A", "B"}})

        result = extract_cluster_metadata(cluster_row, cross_df, raw_data_df)

        assert result["event_name"] is None
        assert result["event_description"] is None
        assert result["event_image_url"] is None
