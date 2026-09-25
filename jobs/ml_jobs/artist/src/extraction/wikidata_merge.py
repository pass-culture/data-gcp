"""Assembly of `extract`'s per-target raw files into the final artist table.

Used by `cli/extraction.py::merge`: `merge_data` combines the raw
per-target dataframes into one (music's platform IDs get pre-merged into music's
own row), and `postprocess_data` flattens the alias columns and normalizes text.
Kept separate from src/extraction/wikidata_extraction.py and src/extraction/qlever.py,
which handle `extract`'s own fetch/retry logic — this is a distinct concern
with its own inputs and outputs.
"""

import pandas as pd
from loguru import logger

from src.common.constants import WIKIDATA_ID_KEY
from src.extraction.wikidata_config import MUSIC_IDS_KEY
from src.linkage.preprocessing_utils import normalize_string_series


def merge_data(dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    # Pre-merge music metadata and music IDs so the resulting music df has the same
    # structure (including matching_score and platform IDs) as the other dfs before concat.
    if "music" in dfs and MUSIC_IDS_KEY in dfs:
        dfs = {
            **dfs,
            "music": dfs["music"].merge(
                dfs[MUSIC_IDS_KEY], on=WIKIDATA_ID_KEY, how="left"
            ),
        }
    elif MUSIC_IDS_KEY in dfs:
        logger.warning(
            "music_ids retrieved but no music df found — skipping ID pre-merge."
        )

    # An entity matched by more than one target (e.g. gkg's broad "any notable
    # person" population overlapping with movie/book/music's narrower ones) ends
    # up with one row per target here, pre-concat — each potentially populated in
    # different columns (only gkg's row has gkg_id, only movie's has its own
    # professions/genres values, etc.). groupby(...).first() collapses those rows
    # into one *per column*, taking the first non-null value across all of an
    # entity's rows — not plain drop_duplicates(subset=[WIKIDATA_ID_KEY]), which
    # keeps only the literal first row and silently discards every other target's
    # columns for that entity, even when they're the only source of that data.
    main_dfs = {name: df for name, df in dfs.items() if name != MUSIC_IDS_KEY}
    merged_df = (
        pd.concat(main_dfs.values())
        .groupby(WIKIDATA_ID_KEY, as_index=False, sort=False)
        .first()
    )

    for query_name, df in main_dfs.items():
        wiki_ids = df[WIKIDATA_ID_KEY].unique()
        merged_df = merged_df.assign(
            **{
                query_name: lambda df, wiki_ids=wiki_ids: df[WIKIDATA_ID_KEY].isin(
                    wiki_ids
                )
            }
        )

    return merged_df


def postprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    EMPTY_ALIAS_KEYWORD = "EMPTY_ALIAS"
    SEPARATOR_KEY = "|"  # Wikidata Queries also use the '|' separator, so be careful when changing this
    return (
        df.assign(
            artist_name=lambda df: df.artist_name_fr.combine_first(df.artist_name_en),
            aliases=lambda df: (
                df.artist_name_fr.fillna(EMPTY_ALIAS_KEYWORD)
                + "|"
                + df.artist_name_en.fillna(EMPTY_ALIAS_KEYWORD)
                + "|"
                + df.aliases_fr.fillna(EMPTY_ALIAS_KEYWORD)
                + "|"
                + df.aliases_en.fillna(EMPTY_ALIAS_KEYWORD)
            )
            .str.replace(f"{SEPARATOR_KEY}{EMPTY_ALIAS_KEYWORD}", "")
            .str.replace(f"{EMPTY_ALIAS_KEYWORD}{SEPARATOR_KEY}", ""),
            aliases_list=lambda df: df.aliases.str.split(SEPARATOR_KEY),
            img=lambda df: df.img.str.replace("http://", "https://"),
        )
        .drop(
            columns=[
                "artist_name_fr",
                "artist_name_en",
                "aliases_fr",
                "aliases_en",
                "aliases",
            ]
        )
        .explode("aliases_list")
        .rename(
            columns={
                "aliases_list": "alias",
            }
        )
        .assign(
            raw_alias=lambda df: df.alias,
            alias=lambda df: df.alias.pipe(normalize_string_series),
        )
        .loc[
            lambda df: (df.alias.notna())
            & (df.alias != "")
            & (df.alias != EMPTY_ALIAS_KEYWORD)
        ]
        .drop_duplicates()
    )
