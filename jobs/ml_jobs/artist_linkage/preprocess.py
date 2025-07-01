import pandas as pd
import typer

from constants import (
    ARTIST_NAME_KEY,
    ARTIST_TYPE_KEY,
    FIRST_ARTIST_KEY,
    IS_MULTI_ARTISTS_KEY,
    OFFER_CATEGORY_ID_KEY,
    OFFER_IS_SYNCHRONISED,
    PREPROCESSED_ARTIST_NAME_KEY,
    TOTAL_BOOKING_COUNT,
    TOTAL_OFFER_COUNT,
)
from utils.preprocessing_utils import (
    FilteringParamsType,
    clean_names,
    extract_first_artist,
    filter_artists,
    format_names,
)

app = typer.Typer()

### Params
FILTERING_PARAMS = FilteringParamsType(
    min_word_count=2, max_word_count=5, min_offer_count=100, min_booking_count=100
)


def preprocess_artists(
    artists_to_match_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Preprocess artists to match them with Wikidata.
    """
    return (
        artists_to_match_df.pipe(clean_names)
        .pipe(extract_first_artist)
        .pipe(format_names)
        .pipe(filter_artists, filtering_params=FILTERING_PARAMS)
        .loc[
            :,
            [
                ARTIST_NAME_KEY,
                OFFER_CATEGORY_ID_KEY,
                OFFER_IS_SYNCHRONISED,
                TOTAL_OFFER_COUNT,
                TOTAL_BOOKING_COUNT,
                ARTIST_TYPE_KEY,
                IS_MULTI_ARTISTS_KEY,
                FIRST_ARTIST_KEY,
                PREPROCESSED_ARTIST_NAME_KEY,
            ],
        ]
    )


@app.command()
def main(
    source_file_path: str = typer.Option(), output_file_path: str = typer.Option()
) -> None:
    artists_to_match_df = pd.read_parquet(source_file_path)

    preprocessed_df = artists_to_match_df.pipe(preprocess_artists)

    preprocessed_df.to_parquet(output_file_path)


if __name__ == "__main__":
    app()
