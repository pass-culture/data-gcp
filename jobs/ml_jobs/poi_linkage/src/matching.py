import re

import pandas as pd
from rapidfuzz import fuzz

from src.constants import (
    ADDRESS_CITY_COL,
    ADDRESS_POSTAL_CODE_COL,
    ADDRESS_STREET_COL,
    DISTANCE_METERS_COL,
    OFFERER_ADDRESS_ID_COL,
    OFFERER_ADDRESS_LABEL_COL,
    POI_ADDRESS_COL,
    POI_COMMON_NAME_COL,
    POI_ID_COL,
    POI_NAME_COL,
    POI_PUBLIC_ENTRANCE_ADDRESS_COL,
    SEARCH_RADIUS_METERS,
    VENUE_ID_FK_COL,
)

VENUE_ID_COL = VENUE_ID_FK_COL
VENUE_NAME_COL = "venue_name"
VENUE_ADDRESS_COL = "venue_address"
# Matching
NAME_SIMILARITY_COL = "name_similarity"
ADDRESS_SIMILARITY_COL = "address_similarity"
MATCH_SCORE_COL = "match_score"

NAME_SIMILARITY_WEIGHT = 0.6
ADDRESS_SIMILARITY_WEIGHT = 0.3
DISTANCE_SCORE_WEIGHT = 0.1

MATCH_SCORE_THRESHOLD = 70


def _normalize_text(value: object) -> str:
    """Lowercase and strip so fuzzy matching isn't thrown off by casing or incidental whitespace."""
    return str(value).strip().lower()


_TRAILING_FRANCE_RE = re.compile(r"\s*france\s*$")


def _sanitize_poi_address(value: object) -> object:
    """Lowercase, drop a trailing 'france' country suffix, drop commas, and strip.

    Preserves nulls as-is (rather than stringifying them) so downstream NaN checks
    in _fuzzy_score still catch them.
    """
    if pd.isna(value):
        return value
    text = _TRAILING_FRANCE_RE.sub("", _normalize_text(value))
    return text.replace(",", "").strip()


def _concat_offerer_address(df: pd.DataFrame) -> pd.Series:
    """address_street + ' ' + address_postal_code + ' ' + address_city, NaN if any part is missing."""
    return (
        df[ADDRESS_STREET_COL]
        .astype("string")
        .str.cat(df[ADDRESS_POSTAL_CODE_COL].astype("string"), sep=" ")
        .str.cat(df[ADDRESS_CITY_COL].astype("string"), sep=" ")
    )


# OffererAddressPOILink: offerer_address_label vs. each candidate POI name column.
POI_NAME_SIMILARITY_COL = "poi_name_similarity"
POI_COMMON_NAME_SIMILARITY_COL = "poi_common_name_similarity"

# OffererAddressPOILink: offerer_address (street/postal_code/city) vs. each candidate POI address column.
OFFERER_ADDRESS_CONCAT_COL = "offerer_address_concat"
POI_ADDRESS_SIMILARITY_COL = "poi_address_similarity"
POI_PUBLIC_ENTRANCE_ADDRESS_SIMILARITY_COL = "poi_public_entrance_address_similarity"

OFFERER_ADDRESS_POI_LINK_COLUMNS = [
    OFFERER_ADDRESS_ID_COL,
    POI_ID_COL,
    DISTANCE_METERS_COL,
    OFFERER_ADDRESS_LABEL_COL,
    POI_NAME_COL,
    POI_COMMON_NAME_COL,
    POI_NAME_SIMILARITY_COL,
    POI_COMMON_NAME_SIMILARITY_COL,
    OFFERER_ADDRESS_CONCAT_COL,
    POI_ADDRESS_COL,
    POI_PUBLIC_ENTRANCE_ADDRESS_COL,
    POI_ADDRESS_SIMILARITY_COL,
    POI_PUBLIC_ENTRANCE_ADDRESS_SIMILARITY_COL,
]


def score_candidates(
    candidates_df: pd.DataFrame,
    venues_df: pd.DataFrame,
    poi_df: pd.DataFrame,
    radius_m: float = SEARCH_RADIUS_METERS,
) -> pd.DataFrame:
    """Score candidate venue/POI pairs on name similarity, address similarity, and distance.

    Returns the input candidates enriched with per-field similarity scores and a
    composite match_score, sorted by venue then descending match_score. No pair is
    dropped or forced into a 1:1 assignment -- this is a ranked list for manual review.
    """
    merged_df = candidates_df.merge(
        venues_df[[VENUE_ID_COL, VENUE_NAME_COL, VENUE_ADDRESS_COL]],
        on=VENUE_ID_COL,
        how="left",
    ).merge(
        poi_df[[POI_ID_COL, POI_NAME_COL, POI_ADDRESS_COL]],
        on=POI_ID_COL,
        how="left",
    )

    merged_df[NAME_SIMILARITY_COL] = merged_df.apply(
        lambda row: fuzz.WRatio(
            _normalize_text(row[VENUE_NAME_COL]), _normalize_text(row[POI_NAME_COL])
        ),
        axis=1,
    )
    merged_df[ADDRESS_SIMILARITY_COL] = merged_df.apply(
        lambda row: fuzz.WRatio(
            _normalize_text(row[VENUE_ADDRESS_COL]), _normalize_text(row[POI_ADDRESS_COL])
        ),
        axis=1,
    )
    distance_score = (1 - merged_df[DISTANCE_METERS_COL] / radius_m).clip(lower=0) * 100

    merged_df[MATCH_SCORE_COL] = (
        NAME_SIMILARITY_WEIGHT * merged_df[NAME_SIMILARITY_COL]
        + ADDRESS_SIMILARITY_WEIGHT * merged_df[ADDRESS_SIMILARITY_COL]
        + DISTANCE_SCORE_WEIGHT * distance_score
    )

    return merged_df.sort_values(
        by=[VENUE_ID_COL, MATCH_SCORE_COL], ascending=[True, False]
    ).reset_index(drop=True)


def _fuzzy_score(left: object, right: object) -> float:
    """WRatio similarity on normalized text, or NaN if either side is null/empty.

    Guards against rapidfuzz silently scoring stringified nulls (str(nan) == "nan")
    as if they were real text.
    """
    if pd.isna(left) or pd.isna(right):
        return float("nan")
    left_normalized = _normalize_text(left)
    right_normalized = _normalize_text(right)
    if not left_normalized or not right_normalized:
        return float("nan")
    return fuzz.WRatio(left_normalized, right_normalized)


def build_offerer_address_poi_link(
    candidates_df: pd.DataFrame,
    addresses_df: pd.DataFrame,
    poi_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build the OffererAddressPOILink table linking offerer_address_id to poi_id.

    Restricted to offerer addresses with a non-null, non-empty offerer_address_label,
    since that's the only text available to match against POI names. Every geographic
    candidate pair from candidates_df (see src.candidates_retrieval.find_candidates) is
    kept and scored -- no distance cutoff or score threshold is applied beyond the
    search radius already baked into candidates_df -- so the relationship between
    distance and name similarity, and the choice between POI_NAME_COL and
    POI_COMMON_NAME_COL, can be analyzed downstream to design the final heuristic.

    Also scores each candidate's POI_ADDRESS_COL and POI_PUBLIC_ENTRANCE_ADDRESS_COL
    (sanitized: lowercased, trailing "france" country suffix and commas dropped)
    against the offerer_address's street/postal_code/city, concatenated.
    """
    labeled_addresses_df = addresses_df[
        addresses_df[OFFERER_ADDRESS_LABEL_COL].notna()
        & (addresses_df[OFFERER_ADDRESS_LABEL_COL].str.strip() != "")
    ].assign(**{OFFERER_ADDRESS_CONCAT_COL: _concat_offerer_address})

    link_df = candidates_df.merge(
        labeled_addresses_df[
            [OFFERER_ADDRESS_ID_COL, OFFERER_ADDRESS_LABEL_COL, OFFERER_ADDRESS_CONCAT_COL]
        ],
        on=OFFERER_ADDRESS_ID_COL,
        how="inner",
    ).merge(
        poi_df[
            [
                POI_ID_COL,
                POI_NAME_COL,
                POI_COMMON_NAME_COL,
                POI_ADDRESS_COL,
                POI_PUBLIC_ENTRANCE_ADDRESS_COL,
            ]
        ],
        on=POI_ID_COL,
        how="left",
    )

    link_df[POI_NAME_SIMILARITY_COL] = link_df.apply(
        lambda row: _fuzzy_score(row[OFFERER_ADDRESS_LABEL_COL], row[POI_NAME_COL]),
        axis=1,
    )
    link_df[POI_COMMON_NAME_SIMILARITY_COL] = link_df.apply(
        lambda row: _fuzzy_score(row[OFFERER_ADDRESS_LABEL_COL], row[POI_COMMON_NAME_COL]),
        axis=1,
    )
    link_df[POI_ADDRESS_SIMILARITY_COL] = link_df.apply(
        lambda row: _fuzzy_score(
            _sanitize_poi_address(row[POI_ADDRESS_COL]), row[OFFERER_ADDRESS_CONCAT_COL]
        ),
        axis=1,
    )
    link_df[POI_PUBLIC_ENTRANCE_ADDRESS_SIMILARITY_COL] = link_df.apply(
        lambda row: _fuzzy_score(
            _sanitize_poi_address(row[POI_PUBLIC_ENTRANCE_ADDRESS_COL]),
            row[OFFERER_ADDRESS_CONCAT_COL],
        ),
        axis=1,
    )

    return (
        link_df[OFFERER_ADDRESS_POI_LINK_COLUMNS]
        .sort_values(by=[OFFERER_ADDRESS_ID_COL, DISTANCE_METERS_COL])
        .reset_index(drop=True)
    )


if __name__ == "__main__":
    pass