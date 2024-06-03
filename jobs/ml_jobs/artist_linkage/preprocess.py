# %%

import pandas as pd
import typer

from utils.preprocess_utils import (
    FilteringParamsType,
    extract_artist_word_count,
    extract_first_artist,
    extract_first_artist_pattern,
    filter_artists,
    format_name,
    remove_leading_punctuation,
    remove_parenthesis,
    remove_single_characters,
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
        .pipe(filter_artists, filtering_params=FILTERING_PARAMS)
    )

    preprocessed_df = filtered_df.pipe(format_name)

    upload_parquet(
        dataframe=preprocessed_df,
        gcs_path=output_file_path,
    )


# %%
if __name__ == "__main__":
    app()
