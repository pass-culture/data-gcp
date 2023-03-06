import pandas as pd
import pytest
from postprocess import build_item_id_from_linkage


class TestPostprocess:
    @staticmethod
    @pytest.fixture
    def expected_postprocessed_data():
        return pd.DataFrame(
            {
                "offer_id": pd.Series(dtype="int"),
                "item_id": pd.Series(dtype="str"),
                "linked_id": pd.Series(dtype="str"),
                "item_linked_id": pd.Series(dtype="str"),
            }
        )

    @staticmethod
    @pytest.mark.parametrize(
        "df_offers_linked_full,expected_offers_linked_export_ready",
        [
            (
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
                        "linked_id": ["a", "a", "a", "b", "b"],
                    }
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
                        "linked_id": ["a", "a", "a", "b", "b"],
                        "item_linked_id": [
                            "movie-1",
                            "movie-1",
                            "movie-1",
                            "link-b",
                            "link-b",
                        ],
                    }
                ),
            )
        ],
    )
    def test_postprocess(
        expected_postprocessed_data,
        df_offers_linked_full,
        expected_offers_linked_export_ready,
    ):
        build_item_id_from_linkage(df_offers_linked_full)

        pd.testing.assert_frame_equal(
            df_offers_linked_full, expected_offers_linked_export_ready
        )
        pd.testing.assert_series_equal(
            df_offers_linked_full.dtypes, expected_postprocessed_data.dtypes
        )
