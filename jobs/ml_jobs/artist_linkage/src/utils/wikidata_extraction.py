"""Two-pass discovery+hydration extraction logic used by
`cli/extract_from_wikidata.py::extract`.

Kept out of the CLI entrypoint so this logic can be unit-tested and reused
directly, without going through Typer. The underlying HTTP fetch/retry
machinery against QLever itself lives in src/utils/qlever.py — this module is
the Wikidata-domain layer on top of it (entity ID normalization, Pass 1
discovery, Pass 2 batch hydration with bisection).
"""

import time

import pandas as pd
from loguru import logger

from src.utils.qlever import (
    QLeverQueryTooExpensive,
    fetch_wikidata_qlever_csv,
    fetch_wikidata_qlever_csv_batch,
)
from src.wikidata_config import HYDRATION_TEMPLATE, render_query

# Pause between Pass 2 (hydration) batch requests — see QueryConfig.hydration_batch_size.
HYDRATION_BATCH_DELAY_SECONDS = 0.2

WIKIDATA_ENTITY_PREFIX = r"https?://www\.wikidata\.org/entity/"


def extract_wikidata_id(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        wikidata_id=lambda df: df.wikidata_id.str.replace(
            WIKIDATA_ENTITY_PREFIX, "", regex=True
        ),
    )


def fetch_discovery(query_name: str) -> pd.DataFrame:
    """Pass 1 of the two-pass discovery+hydration pattern (see
    QueryConfig.hydration_batch_size): a cheap, single query enumerating every
    entity matching `query_name`'s entity filter, with its external-ID value(s)
    and matching score. No multi-valued joins, no GROUP_CONCAT, no sort — so this
    stays fast no matter how large the candidate population is. Pass 2
    (`hydrate_batch`) fetches the expensive multi-valued attributes afterwards, in
    small VALUES-scoped batches of exactly the entities this pass found.
    """
    query = render_query(query_name)
    logger.debug(f"SPARQL Query (discovery): \n{query}")
    return fetch_wikidata_qlever_csv(query)


def hydrate_batch(
    query_name: str, wikidata_ids: list[str], dropped_ids: list[str]
) -> list[pd.DataFrame]:
    """Fetch Pass 2 attributes for a batch of entities via a VALUES-scoped query
    (HYDRATION_TEMPLATE), bisecting the batch and recursing whenever QLever
    rejects it as too expensive, down to a single entity.

    Unlike the old ID-range batching this replaces, a VALUES batch's cost is
    bounded by exactly how many (known, real) entities are in it — not by guessing
    how dense an unknown ID range might be — so batches can be a simple fixed size
    (QueryConfig.hydration_batch_size) instead of a density-informed partition.
    HYDRATION_TEMPLATE computes each multi-valued field in its own subquery for
    exactly this reason — see its docstring for the measured 42s-timeout-vs-3.6s
    comparison against the flat alternative that was tried and dropped.

    Even so, a single entity's own fields (aliases_fr x aliases_en x professions x
    genres x languages) could in principle still exceed the budget on their own —
    at that floor, if QLever still rejects it, that one entity is skipped (loudly,
    and recorded in `dropped_ids`) rather than blocking the whole extraction.

    `dropped_ids` is a caller-owned accumulator (not a return value) so every
    recursive call appends to the same list; the caller reports it once the whole
    hydration pass is done.
    """
    query = render_query(
        query_name, wikidata_ids=wikidata_ids, template=HYDRATION_TEMPLATE
    )
    try:
        df = fetch_wikidata_qlever_csv_batch(query)
    except QLeverQueryTooExpensive:
        if len(wikidata_ids) <= 1:
            logger.warning(
                f"{query_name}: {wikidata_ids[0]} rejected by QLever as too "
                "expensive even alone — skipping this one entity."
            )
            dropped_ids.append(wikidata_ids[0])
            return []
        mid = len(wikidata_ids) // 2
        left, right = wikidata_ids[:mid], wikidata_ids[mid:]
        logger.info(
            f"{query_name} hydration batch of {len(wikidata_ids)} too expensive "
            f"for QLever — splitting into batches of {len(left)} and {len(right)}."
        )
        time.sleep(HYDRATION_BATCH_DELAY_SECONDS)
        return hydrate_batch(query_name, left, dropped_ids) + hydrate_batch(
            query_name, right, dropped_ids
        )

    logger.info(
        f"{query_name} hydration batch of {len(wikidata_ids)}: retrieved {len(df)} rows."
    )
    return [df] if not df.empty else []
