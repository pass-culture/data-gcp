import pandas as pd

from src.constants import ARTIST_ID_KEY, ARTIST_NAME_KEY, SORTED_ARTIST_NAME_KEY


# Raw Scoring
def smooth_ponderation(
    x: float,
) -> float:
    """
    Apply smooth ponderation to a numeric value.

    This function applies a smoothing transformation to values >= 1, using a square root
    transformation (SMOOTHING_PARAM = 0.5) to dampen large values. Values below 1 are
    mapped to 0.

    Args:
        x: The input value to transform.

    Returns:
        The smoothed value, ranging from 0.0 (for x < 1) to approximately 2.0 (as x approaches infinity).
    """
    SMOOTHING_PARAM = 0.5
    return (1 + (x**SMOOTHING_PARAM - 1) / (x**SMOOTHING_PARAM + 1)) if x >= 1 else 0.0


def compute_raw_score(df: pd.DataFrame) -> pd.DataFrame:
    MIN_BOOKABLE_PRODUCTS_FOR_SCORE = 1
    PRODUCT_SCORE_DELTA_X = -1
    ROUND_DECIMALS = 2
    return df.assign(
        wiki_score=lambda df: df.wikidata_id.notna().astype(int),
        image_score=lambda df: df.wikidata_image_file_url.notna().astype(int),
        bookable_score=lambda df: (
            df.bookable_product_count.fillna(0) >= MIN_BOOKABLE_PRODUCTS_FOR_SCORE
        ).astype(int),
        bookings_score=lambda df: (df.booking_count.fillna(0).map(smooth_ponderation)),
        product_score=lambda df: (
            df.product_count.fillna(0).map(
                lambda x: smooth_ponderation(x + PRODUCT_SCORE_DELTA_X)
            )
        ),
        raw_score=lambda df: (
            5 * df.wiki_score
            + df.image_score
            + df.bookings_score
            + df.product_score
            + df.bookable_score
        ).round(ROUND_DECIMALS),
    )


# Namesake Scoring
def get_namesakes(artist_df: pd.DataFrame) -> pd.DataFrame:
    return (
        artist_df.groupby(SORTED_ARTIST_NAME_KEY)
        .agg(
            artist_ids=(ARTIST_ID_KEY, list),
            artist_count=(ARTIST_ID_KEY, "nunique"),
            artist_names=(ARTIST_NAME_KEY, list),
        )
        .loc[lambda df: df.artist_count > 1]
    )


def compute_namesake_scoring(artist_df: pd.DataFrame) -> pd.DataFrame:
    # Combine artist_df with namesakes info
    artist_with_namesakes_df = artist_df.merge(
        get_namesakes(artist_df).reset_index(),
        on=SORTED_ARTIST_NAME_KEY,
        how="left",
    )

    # Determine which sorted artist names have at least one wiki_id
    sorted_artist_names_with_wiki_id = artist_with_namesakes_df.loc[
        lambda d: d.wikidata_id.notna(), SORTED_ARTIST_NAME_KEY
    ].unique()

    # Case 1: Namesake group with at least one wiki_id -> assign score_multiplier = 1 to all with wiki_id, 0 otherwise
    with_wiki_id_df = artist_with_namesakes_df.loc[
        lambda d: d[SORTED_ARTIST_NAME_KEY].isin(sorted_artist_names_with_wiki_id)
    ].assign(score_multiplier=lambda d: d.wikidata_id.notna().astype(int))

    # Case 2 : Namesake group with no wiki_id -> assign score_multiplier = 1 to the highest raw_score, 0 otherwise
    without_wiki_id_df = (
        artist_with_namesakes_df.loc[
            lambda d: ~d[SORTED_ARTIST_NAME_KEY].isin(sorted_artist_names_with_wiki_id)
        ]
        .sort_values(
            by=[SORTED_ARTIST_NAME_KEY, "raw_score", "artist_id"],
            ascending=[True, False, False],
        )
        .assign(
            score_multiplier=lambda df: [
                i == 0
                for _, group in df.loc[
                    lambda d: ~d[SORTED_ARTIST_NAME_KEY].isin(
                        sorted_artist_names_with_wiki_id
                    )
                ].groupby(SORTED_ARTIST_NAME_KEY)
                for i in range(len(group))
            ]
        )
    )

    # Assign score multipliers
    return pd.concat(
        [
            with_wiki_id_df,
            without_wiki_id_df,
        ]
    )
