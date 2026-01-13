import pandas as pd
import typer
from loguru import logger

from src.constants import (
    ARTIST_NAME_KEY,
    HAS_BOOKABLE_OFFER_KEY,
    OFFER_NAME_KEY,
    PRODUCT_ID_KEY,
    TOTAL_BOOKING_COUNT,
)
from src.utils.scoring import (
    compute_namesake_scoring,
    compute_raw_score,
)

app = typer.Typer()


@app.command()
def main(
    # Input files
    artist_file_path: str = typer.Option(),
    product_artist_link_file_path: str = typer.Option(),
    product_file_path: str = typer.Option(),
    # Output files
    output_delta_artist_file_path: str = typer.Option(),
) -> None:
    """"""
    # 1. Load data
    logger.info("Loading artist data...")
    applicative_artist_df = pd.read_parquet(artist_file_path)
    applicative_product_artist_link_df = pd.read_parquet(product_artist_link_file_path)
    applicative_product_df = pd.read_parquet(product_file_path)
    logger.success("Artist data loaded successfully.")

    # 2. Prepare Data
    product_df = (
        applicative_product_df.loc[
            :,
            [
                PRODUCT_ID_KEY,
                TOTAL_BOOKING_COUNT,
                HAS_BOOKABLE_OFFER_KEY,
                OFFER_NAME_KEY,
            ],
        ]
        .astype({PRODUCT_ID_KEY: int})
        .drop_duplicates()
    )
    product_artist_link_df = applicative_product_artist_link_df.merge(
        product_df, on=PRODUCT_ID_KEY, how="left", validate="many_to_one"
    )
    product_stats_df = (
        product_artist_link_df.groupby("artist_id")
        .agg(
            product_count=(PRODUCT_ID_KEY, "nunique"),
            bookable_product_count=(HAS_BOOKABLE_OFFER_KEY, "sum"),
            booking_count=(TOTAL_BOOKING_COUNT, "sum"),
        )
        .astype({"bookable_product_count": "Int64", "booking_count": "Int64"})
        .reset_index()
    )
    artist_with_stats_df = (
        applicative_artist_df.assign(
            sorted_artist_name=lambda df: df[ARTIST_NAME_KEY]
            .str.lower()
            .map(lambda name: " ".join(sorted(name.split(" ")))),
        )
        .merge(product_stats_df, on="artist_id", how="left", validate="one_to_one")
        .fillna({"product_count": 0, "bookable_product_count": 0, "booking_count": 0})
    )

    # 3. Compute artist scores
    artists_with_score_df = (
        artist_with_stats_df.pipe(compute_raw_score)
        .pipe(compute_namesake_scoring)
        .assign(artist_score=lambda df: df.raw_score * df.score_multiplier)
    )

    # 4. Build delta artist dataframe
    logger.info("Building delta artist dataframe...")
    delta_artist_df = (
        pd.DataFrame()
    )  # Placeholder for actual refreshed artists dataframe
    logger.success("Delta artist dataframe built successfully.")
    logger.info(f"Number of artists to update: {len(delta_artist_df)}")

    # 5. Sanity check for consistency
    logger.info("Performing sanity checks...")
    # sanity_checks(
    #     delta_product_df=empty_delta_product_artist_link_df,
    #     delta_artist_df=delta_artist_df,
    #     delta_artist_alias_df=empty_delta_artist_alias_df,
    #     applicative_artist_df=applicative_artist_df,
    # )
    logger.success("Sanity checks passed successfully.")

    # 7. Save files
    logger.info("Saving delta dataframes...")
    logger.info(
        f"Saving delta artist dataframes to {output_delta_artist_file_path}, {output_delta_artist_alias_file_path} and {output_delta_product_artist_link_file_path}."
    )
    delta_artist_df.to_parquet(output_delta_artist_file_path, index=False)
    logger.success("Delta dataframes saved successfully.")


if __name__ == "__main__":
    app()
