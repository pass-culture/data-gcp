import pandas as pd
import typer

from constants import OFFER_IS_SYNCHRONISED, TOTAL_OFFER_COUNT
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


@app.command()
def main(
    source_file_path: str = typer.Option(), output_file_path: str = typer.Option()
) -> None:
    artists_to_match_df = pd.read_parquet(source_file_path)

    preprocessed_df = (
        artists_to_match_df.pipe(clean_names)
        .pipe(extract_first_artist)
        .pipe(format_names)
        .pipe(filter_artists, filtering_params=FILTERING_PARAMS)
    ).loc[
        :,
        [
            "artist_name",
            "offer_category_id",
            OFFER_IS_SYNCHRONISED,
            TOTAL_OFFER_COUNT,
            "total_booking_count",
            "artist_type",
            "is_multi_artists",
            "first_artist",
            "preprocessed_artist_name",
        ],
    ]

    preprocessed_df.to_parquet(output_file_path)


if __name__ == "__main__":
    app()
