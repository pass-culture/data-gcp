import numpy as np
import pandas as pd
import pytest
import rapidfuzz
from rapidfuzz import fuzz

from utils import clustering_utils
from utils.clustering_utils import (
    cluster_with_distance_matrices,
    get_cluster_to_nickname_dict,
)


class TestGetClusterToNicknameDict:
    def test_empty_dataframe(self):
        df = pd.DataFrame(
            {
                "cluster_id": [],
                "offer_number": [],
                "artist_name": [],
            }
        )
        result = get_cluster_to_nickname_dict(df)
        assert result == {}

    def test_single_cluster(self):
        df = pd.DataFrame(
            {
                "cluster_id": ["cluster1", "cluster1"],
                "offer_number": [1, 2],
                "artist_name": ["artist1", "artist2"],
            }
        )
        result = get_cluster_to_nickname_dict(df)
        assert result == {"cluster1": "artist2"}

    def test_multiple_clusters(self):
        df = pd.DataFrame(
            {
                "cluster_id": ["cluster1", "cluster1", "cluster2", "cluster2"],
                "offer_number": [1, 2, 3, 4],
                "artist_name": ["artist1", "artist2", "artist3", "artist4"],
            }
        )
        result = get_cluster_to_nickname_dict(df)
        assert result == {"cluster1": "artist2", "cluster2": "artist4"}


class TestFormatClusterMatrix:
    def test_format_cluster_matrix_valid_input(self):
        df = pd.DataFrame(
            {"preprocessed_artist_name": [{"artist1", "artist2"}, {"artist3"}]},
            index=[0, 1],
        )
        result = clustering_utils.format_cluster_matrix(df, "offer1", "type1")
        assert list(result.columns) == [
            "preprocessed_artist_name",
            "num_artists",
            "offer_category_id",
            "artist_type",
            "group_cluster_id",
            "cluster_id",
        ]
        assert result["num_artists"].tolist() == [2, 1]
        assert result["offer_category_id"].tolist() == ["offer1", "offer1"]
        assert result["artist_type"].tolist() == ["type1", "type1"]
        assert result["group_cluster_id"].tolist() == [0, 1]
        assert result["cluster_id"].tolist() == ["offer1_type1_0", "offer1_type1_1"]

    def test_format_cluster_matrix_empty_input(self):
        df = pd.DataFrame({"preprocessed_artist_name": []})
        result = clustering_utils.format_cluster_matrix(df, "offer1", "type1")
        assert list(result.columns) == [
            "preprocessed_artist_name",
            "num_artists",
            "offer_category_id",
            "artist_type",
            "group_cluster_id",
            "cluster_id",
        ]
        assert result.empty

    class TestClusterWithDistanceMatrices:
        def test_normal_conditions(self):
            df = pd.DataFrame(
                {
                    "preprocessed_artist_name": [
                        "Taylor Swift",
                        "Taylor Swiftt",
                        "Taylor Swifttt",
                        "Adele",
                        "Adelee",
                        "Adeleee",
                        "Ed Sheeran",
                        "Ed Sheerann",
                        "Ed Sheerannn",
                        "Beyonce",
                    ]
                }
            )
            result = cluster_with_distance_matrices(
                df,
                num_chunks=2,
                clustering_threshold=0.2,
                dtype_distance_matrix=np.uint8,
                distance_metric=rapidfuzz.distance.OSA.normalized_distance,
                sparse_filter_threshold=0.3,
            ).reset_index()

            assert len(result) == 4
            assert result["cluster"].nunique() == 4
            assert result["cluster"].min() == 0
            assert len(result.explode("preprocessed_artist_name")) == len(
                df.drop_duplicates()
            )

        def test_empty_dataframe(self):
            df = pd.DataFrame({"preprocessed_artist_name": []})
            with pytest.raises(ValueError):
                cluster_with_distance_matrices(
                    df,
                    num_chunks=2,
                    clustering_threshold=0.5,
                    dtype_distance_matrix=np.uint8,
                    distance_metric=fuzz.ratio,
                    sparse_filter_threshold=0.1,
                )

        def test_single_unique_artist(self):
            df = pd.DataFrame(
                {
                    "preprocessed_artist_name": [
                        "artist1",
                        "artist1",
                        "artist1",
                        "artist1",
                    ]
                }
            )
            result = cluster_with_distance_matrices(
                df,
                num_chunks=2,
                clustering_threshold=0.5,
                dtype_distance_matrix=np.uint8,
                distance_metric=fuzz.ratio,
                sparse_filter_threshold=0.1,
            ).reset_index()

            assert len(result) == 1
            assert result["cluster"].nunique() == 1
            assert result["cluster"].min() == 0
            assert len(result.explode("preprocessed_artist_name")) == len(
                df.drop_duplicates()
            )
