"""Two-pass discovery+hydration extraction logic used by
`cli/extract_from_wikidata.py::extract`.

Kept out of the CLI entrypoint so this logic can be unit-tested and reused
directly, without going through Typer: `extract_two_pass`/`extract_single_pass`
are the two top-level entry points `extract` dispatches to based on
QueryConfig.hydration_batch_size. The underlying HTTP fetch/retry machinery
against QLever itself lives in src/utils/qlever.py — this module is the
Wikidata-domain layer on top of it (entity ID normalization, Pass 1 discovery,
Pass 2 batch hydration with bisection, and checkpoint-aware orchestration of
both passes).
"""

import time

import pandas as pd
from loguru import logger

from src.utils import wikidata_checkpoint as checkpoint
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


def run_discovery(query_name: str, checkpoint_dir: str) -> pd.DataFrame:
    """Pass 1: discover candidate entities, resuming from checkpoint if present."""
    logger.info(f"[{query_name}] Pass 1: discovering candidate entities")
    discovery_df = checkpoint.load_discovery_checkpoint(checkpoint_dir)
    if discovery_df is not None:
        logger.info(f"[{query_name}] Pass 1: resuming from checkpoint")
    else:
        discovery_df = fetch_discovery(query_name).pipe(extract_wikidata_id)
        checkpoint.save_discovery_checkpoint(checkpoint_dir, discovery_df)
    logger.info(f"[{query_name}] Pass 1: found {len(discovery_df)} candidate entities")
    return discovery_df


def hydrate_batches(
    query_name: str,
    checkpoint_dir: str,
    batches: list[list[str]],
    processed_batches: set[int],
    dropped_ids: list[str],
) -> list[pd.DataFrame]:
    """Pass 2: hydrate each batch, resuming already-processed batches from checkpoint."""
    hydration_dfs: list[pd.DataFrame] = []
    for i, batch in enumerate(batches):
        if i in processed_batches:
            batch_df = checkpoint.load_batch_checkpoint(checkpoint_dir, i)
            if batch_df is not None:
                hydration_dfs.append(batch_df)
            continue
        batch_dfs = hydrate_batch(query_name, batch, dropped_ids)
        if batch_dfs:
            batch_df = pd.concat(batch_dfs, ignore_index=True).pipe(extract_wikidata_id)
            checkpoint.save_batch_checkpoint(checkpoint_dir, i, batch_df)
            hydration_dfs.append(batch_df)
        # Persist after every batch (not just at the end): dropped_ids and the
        # processed-batches log must reflect exactly what's been checkpointed
        # to disk so far, in case this attempt itself gets interrupted.
        checkpoint.save_dropped_ids(checkpoint_dir, dropped_ids)
        checkpoint.mark_batch_processed(checkpoint_dir, i)
        if i < len(batches) - 1:
            time.sleep(HYDRATION_BATCH_DELAY_SECONDS)
    return hydration_dfs


def extract_two_pass(
    query_name: str, batch_size: int, checkpoint_dir: str
) -> tuple[pd.DataFrame, list[str]]:
    """Discovery + hydration extraction for a two-pass target
    (QueryConfig.hydration_batch_size), resuming from a local checkpoint."""
    discovery_df = run_discovery(query_name, checkpoint_dir)

    wikidata_ids = discovery_df["wikidata_id"].tolist()
    batches = [
        wikidata_ids[i : i + batch_size]
        for i in range(0, len(wikidata_ids), batch_size)
    ]

    processed_batches = checkpoint.load_processed_batches(checkpoint_dir)
    dropped_ids = checkpoint.load_dropped_ids(checkpoint_dir)
    if processed_batches:
        logger.info(
            f"[{query_name}] Pass 2: resuming — {len(processed_batches)}/"
            f"{len(batches)} batches already hydrated in a previous attempt"
        )
    logger.info(
        f"[{query_name}] Pass 2: hydrating {len(wikidata_ids)} entities in "
        f"{len(batches)} batches of up to {batch_size}"
    )

    hydration_dfs = hydrate_batches(
        query_name, checkpoint_dir, batches, processed_batches, dropped_ids
    )

    # Inner merge: entities in dropped_ids simply have no row in hydration_df,
    # so they're naturally excluded here without extra filtering logic.
    df = (
        discovery_df.merge(
            pd.concat(hydration_dfs, ignore_index=True),
            on="wikidata_id",
            how="inner",
        )
        if hydration_dfs
        else pd.DataFrame()
    )
    return df, dropped_ids


def extract_single_pass(query_name: str) -> pd.DataFrame:
    """Single-query extraction for a target with no hydration pass."""
    query_string = render_query(query_name)
    logger.debug(f"SPARQL Query: \n{query_string}")
    return fetch_wikidata_qlever_csv(query_string).pipe(extract_wikidata_id)
