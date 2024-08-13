import pandas as pd
from utils.preprocessing_utils import (
    FilteringParamsType,
    clean_names,
    extract_first_artist,
    filter_artists,
    format_names,
)


def assert_frame_equal_ignore_index_nor_orders(result_df, expected_df):
    pd.testing.assert_frame_equal(
        result_df.loc[:, lambda df: df.columns.sort_values()].reset_index(drop=True),
        expected_df.loc[:, lambda df: df.columns.sort_values()].reset_index(drop=True),
    )


class TestPreprocessingUtils:
    class TestCleanNames:
        @staticmethod
        def test_clean_names():
            # Create a sample DataFrame
            data = {
                "artist_name": [
                    " Beyoncé (sdsdq)",
                    " Adele (sdfsd",
                    "!/# Coldplay ",
                    "Bon Jovi ']",
                ]
            }
            df = pd.DataFrame(data)

            # Call the function with the sample DataFrame
            result_df = clean_names(df)

            # Create the expected DataFrame
            expected_data = {
                "artist_name": ["Beyoncé", "Adele", "Coldplay", "Bon Jovi"]
            }
            expected_df = pd.DataFrame(expected_data)

            # Check if the resulting DataFrame matches the expected DataFrame
            pd.testing.assert_frame_equal(result_df, expected_df)

        @staticmethod
        def test_clean_names_empty_dataframe():
            # Create an empty DataFrame
            df = pd.DataFrame({"artist_name": []}, dtype="string")

            # Call the function with the empty DataFrame
            result_df = clean_names(df)

            # Check if the resulting DataFrame is also empty
            assert result_df.empty

    class TestExtractFirstArtist:
        @staticmethod
        def test_extract_first_artist_single_artist():
            df = pd.DataFrame({"artist_name": ["John Doe"]})
            result = extract_first_artist(df)
            assert result["first_artist"].iloc[0] == "John Doe"
            assert not result["is_multi_artists"].iloc[0]

        @staticmethod
        def test_extract_first_artist_multiple_artists_pattern():
            df = pd.DataFrame({"artist_name": ["John Doe & Jane Doe"]})
            result = extract_first_artist(df)
            assert result["first_artist"].iloc[0] == "John Doe"
            assert result["is_multi_artists"].iloc[0]

        @staticmethod
        def test_extract_first_artist_multiple_artists_comma():
            df = pd.DataFrame({"artist_name": ["John Doe, Jane Doe"]})
            result = extract_first_artist(df)
            assert result["first_artist"].iloc[0] == "John Doe"
            assert result["is_multi_artists"].iloc[0]

        @staticmethod
        def test_extract_first_artist_single_artists_comma():
            df = pd.DataFrame({"artist_name": ["Doe, John"]})
            result = extract_first_artist(df)
            assert result["first_artist"].iloc[0] == "Doe, John"
            assert not result["is_multi_artists"].iloc[0]

    class TestFilterArtists:
        @staticmethod
        def test_filter_artists_min_word_count():
            df = pd.DataFrame(
                {
                    "first_artist": ["a", "b", "ab", "abc"],
                    "offer_number": [1, 5, 5, 5],
                    "total_booking_count": [5, 5, 5, 5],
                }
            )
            filtering_params = FilteringParamsType(
                min_word_count=2,
                max_word_count=5,
                min_offer_count=2,
                min_booking_count=2,
            )
            result = filter_artists(df, filtering_params)

            expected_df = pd.DataFrame(
                {
                    "first_artist": ["", "ab", "abc"],
                    "offer_number": [5, 5, 5],
                    "total_booking_count": [5, 5, 5],
                    "artist_word_count": [0, 0, 1],
                }
            )
            assert_frame_equal_ignore_index_nor_orders(result, expected_df)
            assert len(result) == 3
            assert "a" not in result["first_artist"].values

        @staticmethod
        def test_filter_artists_max_word_count():
            df = pd.DataFrame(
                {
                    "first_artist": ["abc dqsdqs dsqdsqd dqdqs", "abcd", "abcde"],
                    "offer_number": [5, 5, 5],
                    "total_booking_count": [5, 5, 5],
                }
            )
            filtering_params = FilteringParamsType(
                min_word_count=2,
                max_word_count=3,
                min_offer_count=1,
                min_booking_count=1,
            )
            result = filter_artists(df, filtering_params)
            assert len(result) == 2
            assert "abc dqsdqs dsqdsqd dqdqs" not in result["first_artist"].values

        @staticmethod
        def test_filter_artists_min_offer_and_booking_count():
            df = pd.DataFrame(
                {
                    "first_artist": ["abc", "abcd", "abcde"],
                    "offer_number": [1, 2, 3],
                    "total_booking_count": [1, 2, 3],
                }
            )
            filtering_params = FilteringParamsType(
                min_word_count=2,
                max_word_count=5,
                min_offer_count=2,
                min_booking_count=2,
            )
            result = filter_artists(df, filtering_params)
            assert len(result) == 2
            assert "abc" not in result["first_artist"].values

    class TestFormatNames:
        @staticmethod
        def test_format_names_multiple_artists():
            # Create a sample DataFrame
            data = {"first_artist": ["Beyoncé", "Adele", "Coldplay"]}
            df = pd.DataFrame(data)

            # Call the function with the sample DataFrame
            result_df = format_names(df)

            # Create the expected DataFrame
            expected_data = {
                "first_artist": ["Beyoncé", "Adele", "Coldplay"],
                "preprocessed_artist_name": ["beyonce", "adele", "coldplay"],
            }
            expected_df = pd.DataFrame(expected_data)

            # Check if the resulting DataFrame matches the expected DataFrame
            pd.testing.assert_frame_equal(result_df, expected_df)

        @staticmethod
        def test_format_names_single_artist():
            # Create a sample DataFrame
            data = {"first_artist": ["Beyoncé"]}
            df = pd.DataFrame(data)

            # Call the function with the sample DataFrame
            result_df = format_names(df)

            # Create the expected DataFrame
            expected_data = {
                "first_artist": ["Beyoncé"],
                "preprocessed_artist_name": ["beyonce"],
            }
            expected_df = pd.DataFrame(expected_data)

            # Check if the resulting DataFrame matches the expected DataFrame
            pd.testing.assert_frame_equal(result_df, expected_df)

        @staticmethod
        def test_format_names_empty_dataframe():
            # Create an empty DataFrame
            df = pd.DataFrame({"first_artist": []})

            # Call the function with the empty DataFrame
            result_df = format_names(df)

            # Check if the resulting DataFrame is also empty
            assert result_df.empty
