import pandas as pd
import pytest

from preprocess import (
    format_artist_name,
    remove_punctuation,
    remove_writers_with_single_word_name,
)


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

    class TestFormatArtistName:
        @staticmethod
        def test_format_artist_name_returns_string():
            assert isinstance(format_artist_name("test"), str)

        @staticmethod
        def test_format_artist_name_sorts_words():
            assert format_artist_name("world hello") == "hello world"

        @staticmethod
        def test_format_artist_name_processes_string():
            # Assuming that rapidfuzz.utils.default_process lowercases the string and removes punctuation
            assert format_artist_name("Hello, World!") == "hello world"

        @staticmethod
        def test_format_artist_name_handles_empty_string():
            assert format_artist_name("") == ""

    class TestRemoveWritersWithSingleWordName:
        @pytest.fixture
        @staticmethod
        def input_data():
            return pd.DataFrame(
                {
                    "offer_category_id": ["LIVRE", "LIVRE", "MUSIC", "LIVRE"],
                    "preprocessed_artist_name": [
                        "John",
                        "John Doe",
                        "The Beatles",
                        "Doe",
                    ],
                }
            )

        @staticmethod
        def test_remove_writers_with_single_word_name(input_data):
            expected_output = pd.DataFrame(
                {
                    "offer_category_id": ["MUSIC", "LIVRE"],
                    "preprocessed_artist_name": ["The Beatles", "John Doe"],
                }
            )
            result = remove_writers_with_single_word_name(input_data)
            pd.testing.assert_frame_equal(
                result.reset_index(drop=True), expected_output.reset_index(drop=True)
            )

        @staticmethod
        def test_remove_writers_with_single_word_name_empty_df():
            input_data = pd.DataFrame(
                columns=["offer_category_id", "preprocessed_artist_name"]
            )
            result = remove_writers_with_single_word_name(input_data)
            pd.testing.assert_frame_equal(result, input_data)

        @staticmethod
        def test_remove_writers_with_single_word_name_all_single_word_names(input_data):
            input_data["preprocessed_artist_name"] = ["John", "Paul", "George", "Ringo"]
            expected_output = input_data.loc[lambda df: df.offer_category_id != "LIVRE"]
            result = remove_writers_with_single_word_name(input_data)
            pd.testing.assert_frame_equal(result, expected_output)

        @staticmethod
        def test_remove_writers_with_single_word_name_no_single_word_names(input_data):
            input_data["preprocessed_artist_name"] = [
                "John Lennon",
                "Paul McCartney",
                "George Harrison",
                "Ringo Starr",
            ]
            result = remove_writers_with_single_word_name(input_data)
            pd.testing.assert_frame_equal(
                result.sort_values("preprocessed_artist_name").reset_index(drop=True),
                input_data.sort_values("preprocessed_artist_name").reset_index(
                    drop=True
                ),
            )
