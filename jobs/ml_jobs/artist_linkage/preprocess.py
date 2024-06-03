# %%
import string

import pandas as pd
import rapidfuzz
import typer
from unidecode import unidecode

app = typer.Typer()


### Parameters
MIN_WORD_COUNT = 2
MAX_WORD_COUNT = 5
MIN_OFFER_COUNT = 100
MIN_BOOKING_COUNT = 100


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


def extract_artist_word_count(artist_df: pd.DataFrame) -> pd.DataFrame:
    # count the number of words in the artist name that are longer than 2 characters (excluding punctuation / initials)
    return artist_df.assign(
        artist_word_count=lambda df: df.first_artist.str.replace(
            r"[^\w\s]", "", regex=True
        )
        .str.split()
        .map(lambda ll: len([x for x in ll if len(x) > 2])),
    )


def filter_artists(artist_df: pd.DataFrame) -> bool:
    pattern = "[\w\-\.]+\/[\w-]+|\+"  # pattern for multi artists separated by + or /

    matching_patterns_indexes = artist_df.artist_name.str.contains(pattern, regex=True)
    too_few_words_indexes = (artist_df.artist_word_count < MIN_WORD_COUNT) & (
        (artist_df.offer_number < MIN_OFFER_COUNT)
        | (artist_df.total_booking_count < MIN_BOOKING_COUNT)
    )
    too_many_words_indexes = artist_df.artist_word_count > MAX_WORD_COUNT

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


### Main


@app.command()
def main(
    source_file_path: str = typer.Option(), output_file_path: str = typer.Option()
) -> None:
    artists_to_match_df = read_parquet(source_file_path)

    cleaned_df = artists_to_match_df.pipe(remove_leading_punctuation).pipe(
        remove_parenthesis
    )

    single_artist_df = cleaned_df.pipe(extract_first_artist_pattern).pipe(
        extract_first_artist
    )

    filtered_df = (
        single_artist_df.pipe(extract_artist_word_count)
        .pipe(remove_single_characters)
        .pipe(filter_artists)
    )

    preprocessed_df = filtered_df.pipe(format_name)

    upload_parquet(
        dataframe=preprocessed_df,
        gcs_path=output_file_path,
    )


# %%
if __name__ == "__main__":
    app()
