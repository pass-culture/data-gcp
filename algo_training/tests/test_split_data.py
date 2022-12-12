import pandas as pd
import pytest

from tools.v1.split_data_tools import split_by_column_and_ratio


class TestSplitData:
    @staticmethod
    @pytest.fixture
    def mock_input_data():
        return pd.DataFrame(
                    {
                        "user_id": [1, 1, 1, 1, 2, 2, 2, 3, 3],
                        "item_id": ["i", "j", "k", "l", "i", "j", "k", "i", "j"],
                    }
                )

    @staticmethod
    @pytest.mark.parametrize(
        "column_name, ratio, expected_tuple_df",
        [
            (
                "user_id",
                0.5,
                (
                        pd.DataFrame(
                            {
                                "user_id": [1, 1, 2, 2, 3],
                                "item_id": ["k", "l", "i", "k", "i"],
                            }
                        ),
                        pd.DataFrame(
                            {
                                "user_id": [1, 1, 2, 3],
                                "item_id": ["i", "j", "j", "j"],
                            }
                        )
                )
            )
        ],
    )
    def test_split_by_column_and_ratio(mock_input_data, column_name, ratio, expected_tuple_df):
        df1, df2 = split_by_column_and_ratio(
            df=mock_input_data, column_name=column_name, ratio=ratio, seed=0
        )
        expected_df1, expected_df2 = expected_tuple_df
        pd.testing.assert_frame_equal(df1.reset_index(drop=True), expected_df1)
        pd.testing.assert_frame_equal(df2.reset_index(drop=True), expected_df2)
