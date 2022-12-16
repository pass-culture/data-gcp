import pandas as pd
import pytest

from tools.split_data_tools import (
    split_by_column_and_ratio,
    split_by_ratio,
    reassign_extra_data_to_target,
)


class TestSplitData:
    @staticmethod
    @pytest.fixture
    def mock_clean_data():
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
                    ),
                ),
            )
        ],
    )
    def test_split_by_column_and_ratio(
        mock_clean_data, column_name, ratio, expected_tuple_df
    ):
        df1, df2 = split_by_column_and_ratio(
            df=mock_clean_data, column_name=column_name, ratio=ratio, seed=0
        )
        expected_df1, expected_df2 = expected_tuple_df
        pd.testing.assert_frame_equal(df1.reset_index(drop=True), expected_df1)
        pd.testing.assert_frame_equal(df2.reset_index(drop=True), expected_df2)

    @staticmethod
    @pytest.mark.parametrize(
        "ratio, expected_tuple_df",
        (
            (
                0.2,
                (
                    pd.DataFrame(
                        {
                            "user_id": [3, 1],
                            "item_id": ["i", "k"],
                        }
                    ),
                    pd.DataFrame(
                        {
                            "user_id": [1, 1, 1, 2, 2, 2, 3],
                            "item_id": ["i", "j", "l", "i", "j", "k", "j"],
                        }
                    ),
                ),
            ),
            (
                0.75,
                (
                    pd.DataFrame(
                        {
                            "user_id": [3, 1, 1, 2, 3, 2, 1],
                            "item_id": ["i", "k", "j", "i", "j", "k", "l"],
                        }
                    ),
                    pd.DataFrame(
                        {
                            "user_id": [1, 2],
                            "item_id": ["i", "j"],
                        }
                    ),
                ),
            ),
        ),
    )
    def test_split_by_ratio(mock_clean_data, ratio, expected_tuple_df):
        df1, df2 = split_by_ratio(df=mock_clean_data, ratio=ratio, seed=0)
        expected_df1, expected_df2 = expected_tuple_df
        pd.testing.assert_frame_equal(df1.reset_index(drop=True), expected_df1)
        pd.testing.assert_frame_equal(df2.reset_index(drop=True), expected_df2)

    @staticmethod
    @pytest.mark.parametrize(
        "source_df, column_name, expected_output_source_df",
        (
            (  # Test when an item in the validation set is not in the train set
                pd.DataFrame(
                    {
                        "user_id": [1, 4, 3],
                        "item_id": ["i", "i", "x"],
                    }
                ),
                "item_id",
                pd.DataFrame(
                    {
                        "user_id": [1, 4],
                        "item_id": ["i", "i"],
                    }
                ),
            ),
            (  # Test when validation set is empty when reassigning
                pd.DataFrame(
                    {
                        "user_id": [3],
                        "item_id": ["x"],
                    }
                ),
                "item_id",
                pd.DataFrame(
                    {
                        "user_id": [3],
                        "item_id": ["x"],
                    }
                ),
            ),
        ),
    )
    def test_reassign_extra_data_to_target(
        mock_clean_data, source_df, column_name, expected_output_source_df
    ):
        output_source_df, _ = reassign_extra_data_to_target(
            source_df=source_df,
            target_df=mock_clean_data,
            column_name=column_name,
        )
        pd.testing.assert_frame_equal(output_source_df, expected_output_source_df)
