import string
from typing import TypedDict

import pandas as pd
import rapidfuzz
from unidecode import unidecode

### Cleaning


def remove_leading_punctuation(artist_df: pd.DataFrame) -> pd.DataFrame:
    return artist_df.assign(
        artist_name=lambda df: df.artist_name.str.lstrip(
            string.whitespace + string.punctuation
        ).str.replace("\(.*\)", "", regex=True)
    )


def remove_parenthesis(artist_df: pd.DataFrame) -> pd.DataFrame:
    return artist_df.assign(
        artist_name=lambda df: df.artist_name.str.replace("\([.*]+\))", "")
        .str.split("\(", regex=True)
        .map(lambda ll: ll[0])
    )


### Multi Artists


def extract_first_artist_pattern(artist_df: pd.DataFrame):
    pattern = ";|/|\+|\&"
    return artist_df.assign(
        first_artist_pattern=lambda df: df.artist_name.str.split(
            pattern, regex=True
        ).map(lambda artist_list: artist_list[0]),
        is_multi_artists_pattern=lambda df: df.artist_name.str.contains(
            pattern, regex=True
        ),
    )


def extract_first_artist(artist_df: pd.DataFrame):
    pattern = "^(?![\w\-']+,).*,.*|.*,.*,.*"
    return artist_df.assign(
        is_multi_artists=lambda df: (
            df.first_artist_pattern.str.contains(pattern, regex=True)
            & (~df.is_multi_artists_pattern)
        ),
        first_artist=lambda df: df.first_artist_pattern.str.split(",", regex=True)
        .map(lambda artist_list: artist_list[0])
        .where(df.is_multi_artists, df.first_artist_pattern),
    )


### Cleaning after extracting multi-authors


def remove_single_characters(artist_df: pd.DataFrame) -> pd.DataFrame:
    pattern = r"\b[a-zA-Z]\b(?!\.)"
    return artist_df.assign(
        first_artist=lambda df: df.first_artist.str.replace(pattern, "", regex=True)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )


### Filtering
class FilteringParamsType(TypedDict):
    min_word_count: int
    max_word_count: int
    min_offer_count: int
    min_booking_count: int


def extract_artist_word_count(artist_df: pd.DataFrame) -> pd.DataFrame:
    # count the number of words in the artist name that are longer than 2 characters (excluding punctuation / initials)
    return artist_df.assign(
        artist_word_count=lambda df: df.first_artist.str.replace(
            r"[^\w\s]", "", regex=True
        )
        .str.split()
        .map(lambda ll: len([x for x in ll if len(x) > 2])),
    )


def filter_artists(
    artist_df: pd.DataFrame, filtering_params: FilteringParamsType
) -> bool:
    pattern = "[\w\-\.]+\/[\w-]+|\+"  # pattern for multi artists separated by + or /

    matching_patterns_indexes = artist_df.artist_name.str.contains(pattern, regex=True)
    too_few_words_indexes = (
        artist_df.artist_word_count < filtering_params["min_word_count"]
    ) & (
        (artist_df.offer_number < filtering_params["min_offer_count"])
        | (artist_df.total_booking_count < filtering_params["min_booking_count"])
    )
    too_many_words_indexes = (
        artist_df.artist_word_count > filtering_params["max_word_count"]
    )

    should_be_filtered = (
        matching_patterns_indexes | too_few_words_indexes | too_many_words_indexes
    )

    return artist_df.loc[~should_be_filtered]


### Formatting


def format_name(artist_df: pd.DataFrame) -> pd.DataFrame:
    return artist_df.assign(
        preprocessed_name=lambda df: df.first_artist.map(unidecode).map(
            lambda s: " ".join(sorted(rapidfuzz.utils.default_process(s).split()))
        )
    )
