"""Extraction logic used by `cli/extraction.py::extract`.

Kept out of the CLI entrypoint so this logic can be unit-tested and reused
directly, without going through Typer. The underlying HTTP fetch/retry
machinery against QLever itself lives in src/extraction/qlever.py — this module
is the Wikidata-domain layer on top of it (query rendering, entity ID
normalization).
"""

import pandas as pd
from loguru import logger

from src.extraction.constants import WIKIDATA_ENTITY_PREFIX
from src.extraction.qlever import fetch_wikidata_qlever_csv
from src.extraction.wikidata_config import QUERY_CONFIGS, SHARDS, render_query


def extract_wikidata_id(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        wikidata_id=lambda df: df.wikidata_id.str.replace(
            WIKIDATA_ENTITY_PREFIX, "", regex=True
        ),
    )


def extract_query(query_name: str) -> pd.DataFrame:
    """Fetch one extraction target, one QLever query per shard if it is sharded.

    Shards are disjoint (one per last digit of the Q-id), so their results are
    concatenated without deduplication.
    """
    config = QUERY_CONFIGS[query_name]
    shard_dfs = []
    for shard in SHARDS if config.sharded else [None]:
        query_string = render_query(query_name, shard=shard)
        logger.debug(f"SPARQL Query (shard={shard}): \n{query_string}")
        shard_df = fetch_wikidata_qlever_csv(
            query_string, dtype=config.id_column_dtypes
        )
        logger.info(f"[{query_name}] shard={shard}: {len(shard_df)} rows")
        shard_dfs.append(shard_df)
    return pd.concat(shard_dfs, ignore_index=True).pipe(extract_wikidata_id)
