import string
from typing import TypedDict

import pandas as pd
import rapidfuzz
from unidecode import unidecode

from constants import TOTAL_OFFER_COUNT

### Cleaning


def _remove_leading_punctuation(artist_df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes leading punctuation and parentheses from the artist names in the given DataFrame.

    Args:
        artist_df (pd.DataFrame): The DataFrame containing the artist names.
            Required columns: artist_name.

    Returns:
        pd.DataFrame: The DataFrame with leading punctuation and parentheses removed from the artist names.
    """
    return artist_df.assign(
        artist_name=lambda df: df.artist_name.str.lstrip(string.punctuation).str.rstrip(
            string.punctuation
        )
    )


def _remove_parenthesis(artist_df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes parentheses and the contents inside from the artist names in the given DataFrame.

    Args:
        artist_df (pd.DataFrame): The DataFrame containing the artist names.
            Required columns: artist_name.

    Returns:
        pd.DataFrame: The DataFrame with parentheses removed from the artist names.
    """
    return artist_df.assign(
        artist_name=lambda df: df.artist_name.str.replace("\([.*]+\))", "")
        .str.split("\(", regex=True)
        .map(lambda ll: ll[0])
    )


def clean_names(artist_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the names in the artist DataFrame.

    Args:
        artist_df (pd.DataFrame): The DataFrame containing artist names.
            Required columns: artist_name.

    Returns:
        pd.DataFrame: The cleaned DataFrame with artist names.

    """
    return (
        artist_df.pipe(_remove_leading_punctuation)
        .pipe(_remove_parenthesis)
        .assign(artist_name=lambda df: df.artist_name.str.strip())
    )


### Multi Artists


def _extract_first_artist_pattern(artist_df: pd.DataFrame):
    """
    Extracts the first artist thanks to a multi-artists punctuation pattern from the artist_name column of the given DataFrame.

    Args:
        artist_df (pd.DataFrame): The DataFrame containing the artist_name column.
            Required columns: artist_name.

    Returns:
        pd.DataFrame: The input DataFrame with two additional columns:
            - first_artist_pattern: The first artist pattern extracted from the artist_name column.
            - is_multi_artists_pattern: A boolean column indicating whether the artist_name contains multiple patterns.
    """
    pattern = ";|/|\+|\&"
    return artist_df.assign(
        first_artist_pattern=lambda df: df.artist_name.str.split(
            pattern, regex=True
        ).map(lambda artist_list: artist_list[0]),
        is_multi_artists_pattern=lambda df: df.artist_name.str.contains(
            pattern, regex=True
        ),
    )


def _extract_first_artist_comma(artist_df: pd.DataFrame):
    """
    Extracts the first artist from the given artist DataFrame by splitting the first_artist_pattern column on commas.
    If the first_artist_pattern column contains multiple artists, it selects the first artist.
    If the first_artist_pattern column does not contain multiple artists, it returns the value as is.

    Args:
        artist_df (pd.DataFrame): The DataFrame containing the artist information.
            Required columns: first_artist_pattern, is_multi_artists_pattern.

    Returns:
        pd.DataFrame: The DataFrame with additional columns 'is_multi_artists' and 'first_artist'.

    """
    pattern = "^(?![\w\-']+,).*,.*|.*,.*,.*"
    return artist_df.assign(
        is_multi_artists_comma=lambda df: (
            df.first_artist_pattern.str.contains(pattern, regex=True)
            & (~df.is_multi_artists_pattern)
        ),
        first_artist=lambda df: df.first_artist_pattern.str.split(",", regex=True)
        .map(lambda artist_list: artist_list[0])
        .where(df.is_multi_artists_comma, df.first_artist_pattern),
        is_multi_artists=lambda df: df.is_multi_artists_pattern
        | df.is_multi_artists_comma,
    )


def _remove_single_characters(artist_df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes single characters from the 'first_artist' column of the input DataFrame.

    Args:
        artist_df (pd.DataFrame): The input DataFrame containing the 'first_artist' column.
            Required columns: first_artist.

    Returns:
        pd.DataFrame: The modified DataFrame with single characters removed from the 'first_artist' column.
    """
    pattern = r"\b[a-zA-Z]\b(?!\.)"
    return artist_df.assign(
        first_artist=lambda df: df.first_artist.str.strip()
        .str.replace(pattern, "", regex=True)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
        .pipe(normalize_string_series)
    )


def extract_first_artist(artist_df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts the first artist from the given DataFrame.

    Args:
        artist_df (pd.DataFrame): The DataFrame containing artist information.
            Required columns: artist_name.

    Returns:
        pd.DataFrame: The DataFrame with the first artist extracted.
    """
    return (
        artist_df.pipe(_extract_first_artist_pattern)
        .pipe(_extract_first_artist_comma)
        .pipe(_remove_single_characters)
    )


### Filtering


def _extract_artist_word_count(artist_df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts the word count of each artist name in the given DataFrame. The word count is calculated by counting the number of words
        in the artist name that are longer than 2 characters (excluding punctuation / initials).

    Args:
        artist_df (pd.DataFrame): The DataFrame containing the artist names.
            Required columns: first_artist.

    Returns:
        pd.DataFrame: The input DataFrame with an additional column 'artist_word_count' that represents the word count of each artist name.
    """
    num_characters = 2

    # count the number of words in the artist name that are longer than 2 characters (excluding punctuation / initials)
    return artist_df.assign(
        artist_word_count=lambda df: df.first_artist.str.replace(
            r"[^\w\s]", "", regex=True
        )
        .str.split()
        .map(lambda ll: len([x for x in ll if len(x) > num_characters])),
    )


class FilteringParamsType(TypedDict):
    min_word_count: int
    max_word_count: int
    min_offer_count: int
    min_booking_count: int


def _filter_artists(
    artist_df: pd.DataFrame, filtering_params: FilteringParamsType
) -> bool:
    """
    Filters the artist DataFrame based on various criteria.

    The function filters out artists if:
    - The 'first_artist' name contains patterns like "word/word" or "word+word".
    - The 'artist_word_count' is below 'min_word_count' AND either 'total_offer_count' is below 'min_offer_count'
      OR 'total_booking_count' is below 'min_booking_count'.
    - The 'artist_word_count' is above 'max_word_count'.
    - The 'first_artist' name is empty or NaN.

    Args:
        artist_df (pd.DataFrame): The DataFrame to filter.
            Required columns: first_artist, artist_word_count, TOTAL_OFFER_COUNT, total_booking_count.
        filtering_params (FilteringParamsType): A dictionary containing filtering parameters:
            - min_word_count (int): Minimum word count for an artist name.
            - max_word_count (int): Maximum word count for an artist name.
            - min_offer_count (int): Minimum total offer count for an artist.
            - min_booking_count (int): Minimum total booking count for an artist.

    Returns:
        pd.DataFrame: The filtered DataFrame, containing only the rows that do not meet the filtering criteria.
    """
    pattern = "[\w\-\.]+\/[\w-]+|\+"  # pattern for multi artists separated by + or /

    matching_patterns_indexes = artist_df.first_artist.str.contains(pattern, regex=True)
    too_few_words_indexes = (
        artist_df.artist_word_count < filtering_params["min_word_count"]
    ) & (
        (artist_df[TOTAL_OFFER_COUNT] < filtering_params["min_offer_count"])
        | (artist_df.total_booking_count < filtering_params["min_booking_count"])
    )
    too_many_words_indexes = (
        artist_df.artist_word_count > filtering_params["max_word_count"]
    )
    empty_first_artist_indexes = (artist_df.first_artist == "") | (
        artist_df.first_artist.isna()
    )

    should_be_filtered = (
        matching_patterns_indexes
        | too_few_words_indexes
        | too_many_words_indexes
        | empty_first_artist_indexes
    )

    return artist_df.loc[~should_be_filtered]


def filter_artists(
    artist_df: pd.DataFrame, filtering_params: FilteringParamsType
) -> pd.DataFrame:
    """
    Filters the artist DataFrame based on the given filtering parameters.

    Args:
        artist_df (pd.DataFrame): The DataFrame containing artist data.
            Required columns: first_artist, artist_word_count, total_offer_count, total_booking_count.
        filtering_params (FilteringParamsType): The filtering parameters to be applied.

    Returns:
        pd.DataFrame: The filtered artist DataFrame.
    """
    return artist_df.pipe(_extract_artist_word_count).pipe(
        _filter_artists, filtering_params=filtering_params
    )


### Formatting


def format_names(artist_df: pd.DataFrame) -> pd.DataFrame:
    """
    Formats the artist names in the given DataFrame.

    Args:
        artist_df (pd.DataFrame): The DataFrame containing artist information.
            Required columns: first_artist.

    Returns:
        pd.DataFrame: The DataFrame with formatted artist names.
    """
    return artist_df.assign(
        preprocessed_artist_name=lambda df: df.first_artist.map(unidecode).map(
            lambda s: " ".join(sorted(rapidfuzz.utils.default_process(s).split()))
        )
    )


def normalize_string_series(s: pd.Series) -> pd.Series:
    """
    Normalize a pandas Series of strings by converting to lowercase, removing accents,
    encoding to ASCII, stripping whitespace, and removing periods.
    Args:
        s (pd.Series): A pandas Series containing strings to be normalized.
    Returns:
        pd.Series: A pandas Series with normalized strings.
    """

    return (
        s.str.lower()
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
        .str.strip()
        .str.replace(".", "")
    )
