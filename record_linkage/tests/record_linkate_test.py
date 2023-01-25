import pandas as pd
import pytest
from utils.linkage import get_linked_offers_from_graph


class TestPostprocess:
    @staticmethod
    @pytest.mark.parametrize(
        "test_offer_cluster,df_matches,df_source,expected_linked_data",
        [
            (
                [[1, 2, 5], [3, 4]],
                pd.DataFrame(
                    {
                        "index_1": [1, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5],
                        "index_2": [1, 2, 5, 2, 1, 3, 4, 4, 3, 5, 1],
                        "offer_id": [1, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5],
                        "item_id": [
                            "movie-1",
                            "movie-1",
                            "movie-1",
                            "product-2",
                            "product-2",
                            "product-3",
                            "product-3",
                            "product-4",
                            "product-4",
                            "product-5",
                            "product-5",
                        ],
                        "linked_id": [
                            "NC",
                            "NC",
                            "NC",
                            "NC",
                            "NC",
                            "NC",
                            "NC",
                            "NC",
                            "NC",
                            "NC",
                            "NC",
                        ],
                    },
                    index=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                ),
                pd.DataFrame(
                    {
                        "offer_id": [1, 2, 3, 4, 5],
                        "item_id": [
                            "movie-1",
                            "product-2",
                            "product-3",
                            "product-4",
                            "product-5",
                        ],
                        "linked_id": ["NC", "NC", "NC", "NC", "NC"],
                    },
                    index=[1, 2, 3, 4, 5],
                ),
                pd.DataFrame(
                    {
                        "offer_id": [1, 2, 3, 4, 5],
                        "item_id": [
                            "movie-1",
                            "product-2",
                            "product-3",
                            "product-4",
                            "product-5",
                        ],
                        "linked_id": ["a", "a", "b", "b", "a"],
                    },
                    index=[1, 2, 3, 4, 5],
                ),
            )
        ],
    )
    def test_postprocess(
        test_offer_cluster, df_source, df_matches, expected_linked_data
    ):
        df_linked_data = get_linked_offers_from_graph(df_source, df_matches)

        assert (
            df_linked_data.linked_id.nunique()
            == expected_linked_data.linked_id.nunique()
        ), f"Number of cluster is expected"

        ######
        # Extract linked offers
        linkage_offer_clusters = []
        for link_id in df_linked_data.linked_id.unique():
            linkage_offer_clusters.append(
                list(df_linked_data.query(f"linked_id=='{link_id}'").offer_id.unique())
            )

        for i in range(len(test_offer_cluster)):
            assert (
                test_offer_cluster[i] == linkage_offer_clusters[i]
            ), f"Cluster {i} is reconstructed has expected"
