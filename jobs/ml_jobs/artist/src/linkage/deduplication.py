import pandas as pd


def get_namesakes(artist_with_stats_df: pd.DataFrame) -> pd.DataFrame:
    return (
        artist_with_stats_df.groupby("normalized_artist_name")
        .agg(
            artist_id_list=("artist_id", list),
            artist_names=("artist_name", list),
            artist_count=("artist_id", "nunique"),
            product_count=("artist_product_count", "sum"),
        )
        .loc[lambda df: df.artist_count > 1]
    )
