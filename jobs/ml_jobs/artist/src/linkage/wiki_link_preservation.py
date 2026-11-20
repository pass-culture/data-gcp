"""[TEMPORARY / REMEDIATION] Safeguard against losing quality (Wikidata) artist links.

Only used by `remediate-product-links`, not by the incremental
`link-new-products`.

While product artist names are being remediated with new data sources, the
remediation replaces many links to artists having a wikidata_id with links to
non-Wikidata artists that are the same person written differently
("Olivier Kempf" -> "Kempf/Olivier", "Pierre Kalmar" -> "P. Kalmar").

This module cancels such swaps when both names are compatible, and only in
that situation. Remove it (and its call in `remediate_product_links` in
cli/linkage.py) once the product artist names are reliable.
"""

import re
import unicodedata
from difflib import SequenceMatcher
from functools import lru_cache

import pandas as pd
from loguru import logger

from src.common.constants import ARTIST_ID_KEY, ARTIST_NAME_KEY, WIKIDATA_ID_KEY
from src.linkage.constants import (
    ACTION_KEY,
    ARTIST_TYPE_KEY,
    PRODUCT_ID_KEY,
    PRODUCTS_KEYS,
    RAW_ALIAS_KEY,
    Action,
)
from src.linkage.remediation import anti_join_on_products_keys

# Tokens carrying no identity information.
IGNORED_TOKENS = frozenset(
    {
        "collectif",
        "collectifs",
        "anonyme",
        "anonymes",
        "mr",
        "mme",
        "dr",
        "ii",
        "iii",
        "iv",
        "de",
        "d",
        "du",
        "des",
        "la",
        "le",
        "l",
    }
)
FUZZY_MIN_TOKEN_LENGTH = 4
FUZZY_MIN_RATIO = 0.85
MIN_ANCHOR_TOKEN_LENGTH = 3
MAX_TOKENS = 8

_REMOVED_NAME_KEY = "removed_name"
_ADDED_NAME_KEY = "added_name"
_REMOVED_ARTIST_ID_KEY = "removed_artist_id"
_ADDED_ARTIST_ID_KEY = "added_artist_id"


def _tokenize(name: str | None) -> tuple[str, ...]:
    """Lowercase, strip accents and parentheses, drop IGNORED_TOKENS."""
    if not isinstance(name, str):
        return ()
    normalized = (
        unicodedata.normalize("NFKD", name.lower())
        .encode("ascii", "ignore")
        .decode("ascii")
    )
    normalized = re.sub(r"\(.*?\)", " ", normalized)
    return tuple(
        token
        for token in re.split(r"[^a-z0-9]+", normalized)
        if token and token not in IGNORED_TOKENS
    )


def _tokens_match(short_token: str, long_token: str) -> tuple[bool, bool]:
    """Return (is_match, is_anchor). An anchor is a non-initial match of length >= 3."""
    if short_token == long_token:
        return True, len(short_token) >= MIN_ANCHOR_TOKEN_LENGTH
    if len(short_token) == 1 or len(long_token) == 1:
        return short_token[0] == long_token[0], False
    if (
        min(len(short_token), len(long_token)) >= FUZZY_MIN_TOKEN_LENGTH
        and SequenceMatcher(None, short_token, long_token).ratio() >= FUZZY_MIN_RATIO
    ):
        return True, True
    return False, False


def _can_assign(
    short_tokens: tuple[str, ...],
    long_tokens: tuple[str, ...],
    used: frozenset[int] = frozenset(),
    *,
    has_anchor: bool = False,
) -> bool:
    """Assign each short token to a distinct long token (backtracking)."""
    if not short_tokens:
        return has_anchor
    head, *tail = short_tokens
    for index, long_token in enumerate(long_tokens):
        if index in used:
            continue
        is_match, is_anchor = _tokens_match(head, long_token)
        if is_match and _can_assign(
            tuple(tail),
            long_tokens,
            used | {index},
            has_anchor=has_anchor or is_anchor,
        ):
            return True
    return False


@lru_cache(maxsize=200_000)
def are_artist_names_compatible(name_a: str | None, name_b: str | None) -> bool:
    """Whether one name is a less precise but consistent version of the other.

    Every token of the shortest name must match a distinct token of the longest
    one (exact, fuzzy for typos, or initial), with at least one non-initial
    token of length >= 3 in common. Word order is ignored.

    Examples:
        "Olivier Kempf" / "Kempf"         -> True
        "Pierre Kalmar" / "P. Kalmar"     -> True
        "Emily Brontë" / "Charlotte Brontë" -> False (first names contradict)
    """
    tokens_a, tokens_b = _tokenize(name_a), _tokenize(name_b)
    if not tokens_a or not tokens_b:
        return False
    short_tokens, long_tokens = sorted((tokens_a, tokens_b), key=len)
    if len(long_tokens) > MAX_TOKENS:
        return False
    return _can_assign(short_tokens, long_tokens)


def _build_wiki_artist_names_df(
    artist_df: pd.DataFrame, wiki_df: pd.DataFrame | None
) -> pd.DataFrame:
    """Canonical names and Wikidata aliases of artists having a wikidata_id."""
    wiki_artists_df = artist_df.loc[
        lambda df: df[WIKIDATA_ID_KEY].notna(),
        [ARTIST_ID_KEY, ARTIST_NAME_KEY, WIKIDATA_ID_KEY],
    ]
    name_frames = [wiki_artists_df.loc[:, [ARTIST_ID_KEY, ARTIST_NAME_KEY]]]
    if wiki_df is not None and {WIKIDATA_ID_KEY, RAW_ALIAS_KEY}.issubset(
        wiki_df.columns
    ):
        name_frames.append(
            wiki_artists_df.loc[:, [ARTIST_ID_KEY, WIKIDATA_ID_KEY]]
            .merge(
                wiki_df.loc[:, [WIKIDATA_ID_KEY, RAW_ALIAS_KEY]].drop_duplicates(),
                how="inner",
                on=WIKIDATA_ID_KEY,
            )
            .rename(columns={RAW_ALIAS_KEY: ARTIST_NAME_KEY})
            .loc[:, [ARTIST_ID_KEY, ARTIST_NAME_KEY]]
        )
    return (
        pd.concat(name_frames, ignore_index=True)
        .dropna()
        .drop_duplicates()
        .rename(
            columns={
                ARTIST_ID_KEY: _REMOVED_ARTIST_ID_KEY,
                ARTIST_NAME_KEY: _REMOVED_NAME_KEY,
            }
        )
    )


def preserve_wiki_links_replaced_by_compatible_artists(
    delta_product_df: pd.DataFrame,
    delta_artist_df: pd.DataFrame,
    artist_df: pd.DataFrame,
    wiki_df: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """TEMPORARY: cancel the replacement of a Wikidata artist link by a compatible non-Wikidata artist.

    For each removed link to an artist having a wikidata_id, look at the links
    added on the same product and artist_type to artists without wikidata_id.
    If an added artist name is compatible (see `are_artist_names_compatible`)
    with the canonical name or a Wikidata alias of the removed artist, both
    the removal and that addition are cancelled. New artists left without any
    added link are dropped from the delta.

    Args:
        delta_product_df: product-artist link delta (PRODUCTS_KEYS + action + comment).
        delta_artist_df: new artists delta.
        artist_df: existing artists, with artist_name and wikidata_id.
        wiki_df: Wikidata dump with wikidata_id and raw_alias columns.

    Returns:
        The filtered (delta_product_df, delta_artist_df).
    """
    removed_links_df = delta_product_df.loc[
        lambda df: df[ACTION_KEY] == Action.remove, PRODUCTS_KEYS
    ].rename(columns={ARTIST_ID_KEY: _REMOVED_ARTIST_ID_KEY})
    added_links_df = delta_product_df.loc[
        lambda df: df[ACTION_KEY] == Action.add, PRODUCTS_KEYS
    ].rename(columns={ARTIST_ID_KEY: _ADDED_ARTIST_ID_KEY})
    if removed_links_df.empty or added_links_df.empty:
        return delta_product_df, delta_artist_df

    non_wiki_artist_names_df = (
        pd.concat(
            [
                artist_df.loc[:, [ARTIST_ID_KEY, ARTIST_NAME_KEY, WIKIDATA_ID_KEY]],
                delta_artist_df.loc[
                    :, [ARTIST_ID_KEY, ARTIST_NAME_KEY, WIKIDATA_ID_KEY]
                ],
            ],
            ignore_index=True,
        )
        .drop_duplicates(subset=[ARTIST_ID_KEY])
        .loc[lambda df: df[WIKIDATA_ID_KEY].isna(), [ARTIST_ID_KEY, ARTIST_NAME_KEY]]
        .rename(
            columns={
                ARTIST_ID_KEY: _ADDED_ARTIST_ID_KEY,
                ARTIST_NAME_KEY: _ADDED_NAME_KEY,
            }
        )
    )

    candidates_df = (
        removed_links_df.merge(
            _build_wiki_artist_names_df(artist_df, wiki_df),
            how="inner",
            on=_REMOVED_ARTIST_ID_KEY,
        )
        .merge(added_links_df, how="inner", on=[PRODUCT_ID_KEY, ARTIST_TYPE_KEY])
        .merge(non_wiki_artist_names_df, how="inner", on=_ADDED_ARTIST_ID_KEY)
    )
    if candidates_df.empty:
        return delta_product_df, delta_artist_df

    compatible_df = candidates_df.loc[
        lambda df: [
            are_artist_names_compatible(removed_name, added_name)
            for removed_name, added_name in zip(
                df[_REMOVED_NAME_KEY], df[_ADDED_NAME_KEY], strict=True
            )
        ]
    ]
    kept_removed_links_df = compatible_df.loc[
        :, [PRODUCT_ID_KEY, _REMOVED_ARTIST_ID_KEY, ARTIST_TYPE_KEY]
    ].rename(columns={_REMOVED_ARTIST_ID_KEY: ARTIST_ID_KEY})
    cancelled_added_links_df = compatible_df.loc[
        :, [PRODUCT_ID_KEY, _ADDED_ARTIST_ID_KEY, ARTIST_TYPE_KEY]
    ].rename(columns={_ADDED_ARTIST_ID_KEY: ARTIST_ID_KEY})

    filtered_delta_product_df = pd.concat(
        [
            delta_product_df.loc[lambda df: df[ACTION_KEY] == Action.remove].pipe(
                anti_join_on_products_keys, kept_removed_links_df
            ),
            delta_product_df.loc[lambda df: df[ACTION_KEY] == Action.add].pipe(
                anti_join_on_products_keys, cancelled_added_links_df
            ),
            delta_product_df.loc[
                lambda df: ~df[ACTION_KEY].isin([Action.remove, Action.add])
            ],
        ],
        ignore_index=True,
    )

    remaining_added_artist_ids = set(
        filtered_delta_product_df.loc[
            lambda df: df[ACTION_KEY] == Action.add, ARTIST_ID_KEY
        ]
    )
    orphan_artist_ids = (
        set(cancelled_added_links_df[ARTIST_ID_KEY]) - remaining_added_artist_ids
    )
    filtered_delta_artist_df = delta_artist_df.loc[
        lambda df: ~df[ARTIST_ID_KEY].isin(orphan_artist_ids)
    ].reset_index(drop=True)

    logger.info(
        "Preserved Wikidata artist links replaced by compatible "
        f"non-Wikidata artists: {kept_removed_links_df.drop_duplicates().shape[0]} "
        f"removals and {cancelled_added_links_df.drop_duplicates().shape[0]} additions "
        f"cancelled, {len(delta_artist_df) - len(filtered_delta_artist_df)} new artists dropped."
    )

    return filtered_delta_product_df, filtered_delta_artist_df
