import pandas as pd
import pytest
from preprocess import preprocess


class TestPreprocess:
    @staticmethod
    @pytest.fixture
    def expected_clean_data():
        return pd.DataFrame(
            {
                "offer_id": [1, 2, 3],
                "item_id": ["product-1", "product-2", "product-3"],
                "offer_subcategoryid": ["SEANCE_CINE", "SEANCE_CINE", "SEANCE_CINE"],
                "offer_name": ["name-1", "name-2", "name-3"],
                "offer_description": [
                    "description-1",
                    "description-2",
                    "description-3",
                ],
                "performer": ["performer-1", "performer-2", "performer-3"],
                "linked_id": ["NC", "NC", "NC"],
            }
        )

    @staticmethod
    @pytest.mark.parametrize(
        "raw_data_df",
        [
            pd.DataFrame(
                {
                    "offer_id": ["1", "2", "3"],
                    "item_id": ["product-1", "product-2", "product-3"],
                    "offer_subcategoryid": [
                        "SEANCE_CINE",
                        "SEANCE_CINE",
                        "SEANCE_CINE",
                    ],
                    "offer_name": ["NAME-1", "nAmE-2", "name-3"],
                    "offer_description": [
                        "Description-1",
                        "DESCRIPTION-2",
                        "description-3",
                    ],
                    "performer": ["performer-1", "performer-2", "performer-3"],
                }
            )
        ],
    )
    def test_preprocess(expected_clean_data, raw_data_df):
        clean_data_df = preprocess(raw_data_df)
        pd.testing.assert_series_equal(clean_data_df.dtypes, expected_clean_data.dtypes)
        pd.testing.assert_frame_equal(clean_data_df, expected_clean_data)
