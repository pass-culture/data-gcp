import pandas as pd
import pytest

from tools.v1.preprocess_tools import preprocess


class TestPreprocess:
    @staticmethod
    @pytest.fixture
    def expected_clean_data():
        return pd.DataFrame(
            {
                "user_id": pd.Series(dtype="str"),
                "item_id": pd.Series(dtype="str"),
                "offer_subcategoryid": pd.Series(dtype="str"),
                "offer_categoryId": pd.Series(dtype="str"),
                "genres": pd.Series(dtype="str"),
                "rayon": pd.Series(dtype="str"),
                "type": pd.Series(dtype="str"),
                "venue_id": pd.Series(dtype="str"),
                "venue_name": pd.Series(dtype="str"),
                "count": pd.Series(dtype="int"),
            }
        )

    @staticmethod
    @pytest.mark.parametrize(
        "raw_data_df",
        [
            (
                pd.DataFrame(
                    {
                        "user_id": pd.Series(dtype="int"),
                        "item_id": pd.Series(dtype="int"),
                        "offer_subcategoryid": pd.Series(dtype="str"),
                        "offer_categoryId": pd.Series(dtype="str"),
                        "genres": pd.Series(dtype="str"),
                        "rayon": pd.Series(dtype="str"),
                        "type": pd.Series(dtype="str"),
                        "venue_id": pd.Series(dtype="str"),
                        "venue_name": pd.Series(dtype="str"),
                        "count": pd.Series(dtype="str"),
                    }
                )
            ),
            (
                pd.DataFrame(
                    columns=[
                        "user_id",
                        "item_id",
                        "offer_subcategoryid",
                        "offer_categoryId",
                        "genres",
                        "rayon",
                        "type",
                        "venue_id",
                        "venue_name",
                        "count",
                    ]
                )
            ),
        ],
    )
    def test_preprocess(expected_clean_data, raw_data_df):
        clean_data_df = preprocess(raw_data_df)
        pd.testing.assert_series_equal(clean_data_df.dtypes, expected_clean_data.dtypes)
