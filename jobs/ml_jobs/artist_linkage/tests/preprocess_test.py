import pandas as pd

from preprocess import remove_punctuation


class TestPreprocess:
    class TestRemovePunctuation:
        @staticmethod
        def test_remove_punctuation_no_punctuation():
            df = pd.DataFrame(
                {"artist_name": ["Artist One", "Artist Two", "Artist Three"]}
            )
            result = remove_punctuation(df)
            pd.testing.assert_frame_equal(df, result)

        @staticmethod
        def test_remove_punctuation_some_punctuation():
            df = pd.DataFrame(
                {"artist_name": ["Artist One", "Artist#Two", "Artist Three"]}
            )
            expected = pd.DataFrame({"artist_name": ["Artist One", "Artist Three"]})
            result = remove_punctuation(df)
            pd.testing.assert_frame_equal(
                expected.reset_index(drop=True), result.reset_index(drop=True)
            )

        @staticmethod
        def test_remove_punctuation_all_punctuation():
            df = pd.DataFrame(
                {"artist_name": ["Artist#One", "Artist#Two", "Artist#Three"]}
            )
            result = remove_punctuation(df)

            assert len(result) == 0

        @staticmethod
        def test_remove_punctuation_empty_df():
            df = pd.DataFrame({"artist_name": []})
            result = remove_punctuation(df)
            pd.testing.assert_frame_equal(
                df.reset_index(drop=True), result.reset_index(drop=True)
            )
