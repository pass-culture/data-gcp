import os
import time
from io import StringIO

import pandas as pd
import requests
import typer
from loguru import logger

from src.constants import WIKIDATA_ID_KEY
from src.utils.preprocessing_utils import normalize_string_series
from src.wikidata_config import (
    HYDRATION_TEMPLATE,
    MUSIC_IDS_KEY,
    QUERY_CONFIGS,
    render_query,
)

QLEVER_ENDPOINT = "https://qlever.cs.uni-freiburg.de/api/wikidata"
QLEVER_HEADERS = {
    "Accept": "text/csv",
    "Content-Type": "application/sparql-query",
    # Same identification string used for other external APIs (see
    # src.constants.WIKIMEDIA_REQUEST_HEADER) — good practice for any shared
    # third-party endpoint, and QLever's own docs ask for one explicitly.
    "User-Agent": "PassCulture/1.0 (https://passculture.app; contact@passculture.app) Python/requests",
}

# Pause between Pass 2 (hydration) batch requests — see QueryConfig.hydration_batch_size.
HYDRATION_BATCH_DELAY_SECONDS = 0.2

app = typer.Typer()


class QLeverQueryTooExpensive(Exception):
    """QLever rejected a query as too costly to run — split it and retry, don't
    just retry the identical (deterministically doomed) query."""


WIKIDATA_ENTITY_PREFIX = r"https?://www\.wikidata\.org/entity/"


def extract_wikidata_id(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        wikidata_id=lambda df: df.wikidata_id.str.replace(
            WIKIDATA_ENTITY_PREFIX, "", regex=True
        ),
    )


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

    # The drop duplicates is done on the wikidata_id column due to the fact that professions or aliases can be unsorted lists
    main_dfs = {name: df for name, df in dfs.items() if name != MUSIC_IDS_KEY}
    merged_df = pd.concat(main_dfs.values()).drop_duplicates(subset=[WIKIDATA_ID_KEY])

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


def clear_qlever_cache(retries: int = 3, backoff_factor: int = 10) -> None:
    for attempt in range(retries):
        try:
            response = requests.get(
                QLEVER_ENDPOINT,
                params={"cmd": "clear-cache"},
                headers=QLEVER_HEADERS,
                timeout=30,
            )
            if response.status_code == 200:
                logger.info(f"Cache cleared for {QLEVER_ENDPOINT}")
                return
            logger.warning(
                f"Cache clear attempt {attempt + 1} failed ({response.status_code}): {response.text[:150]}"
            )
        except requests.RequestException as e:
            logger.warning(f"Cache clear attempt {attempt + 1} request error: {e}")

        # Exponential backoff
        time.sleep(backoff_factor * (2**attempt))

    logger.warning(
        "Failed to reset QLever cache after retries. Proceeding with execution..."
    )


def fetch_wikidata_qlever_csv(
    sparql_query: str, retries: int = 3, backoff_factor: int = 10
) -> pd.DataFrame:
    # POST, not GET: a large VALUES-scoped hydration query can run to tens of KB,
    # well past a GET URI's length limit (confirmed live: 414 Request-URI Too
    # Large at ~30KB). POST puts the query in the body instead, with no such
    # ceiling — QLEVER_HEADERS' Content-Type is exactly the SPARQL-protocol
    # "query is the raw POST body" convention this relies on.
    for attempt in range(retries):
        try:
            response = requests.post(
                QLEVER_ENDPOINT,
                data=sparql_query.encode("utf-8"),
                headers=QLEVER_HEADERS,
                timeout=120,
            )
            if response.status_code == 200:
                response.encoding = "utf-8"
                return pd.read_csv(StringIO(response.text))

            logger.warning(
                f"Attempt {attempt + 1} failed ({response.status_code}): {response.text[:200]}"
            )
        except requests.RequestException as e:
            logger.warning(f"Attempt {attempt + 1} request error: {e}")

        # Exponential backoff
        time.sleep(backoff_factor * (2**attempt))

    raise requests.RequestException(
        f"Failed to fetch data from {QLEVER_ENDPOINT} after {retries} attempts."
    )


def _is_cost_rejection(response: requests.Response) -> bool:
    """True if QLever rejected the query outright as too expensive to run (its own
    cost estimator gave up), as opposed to a transient network/server error worth
    retrying as-is. Distinguishing the two matters: retrying an expensive query
    unchanged just fails the same way every time — it needs a smaller range.
    """
    if response.status_code != 429:
        return False
    try:
        exception_message = response.json().get("exception", "")
    except ValueError:
        return False
    return any(
        phrase in exception_message
        for phrase in ("timed out", "time estimate exceeded", "canceled")
    )


def fetch_wikidata_qlever_csv_batch(sparql_query: str) -> pd.DataFrame:
    """Single-attempt fetch for one Pass 2 hydration batch.

    Raises QLeverQueryTooExpensive (no retry — see `_is_cost_rejection`) so the
    caller can bisect the batch instead; retries transient failures like
    `fetch_wikidata_qlever_csv` does.
    """
    retries, backoff_factor = 3, 5
    for attempt in range(retries):
        try:
            response = requests.post(
                QLEVER_ENDPOINT,
                data=sparql_query.encode("utf-8"),
                headers=QLEVER_HEADERS,
                timeout=120,
            )
            if response.status_code == 200:
                response.encoding = "utf-8"
                return pd.read_csv(StringIO(response.text))
            if _is_cost_rejection(response):
                raise QLeverQueryTooExpensive(response.text[:300])

            logger.warning(
                f"Attempt {attempt + 1} failed ({response.status_code}): {response.text[:200]}"
            )
        except requests.RequestException as e:
            logger.warning(f"Attempt {attempt + 1} request error: {e}")

        # Exponential backoff
        time.sleep(backoff_factor * (2**attempt))

    raise requests.RequestException(
        f"Failed to fetch data from {QLEVER_ENDPOINT} after {retries} attempts."
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


@app.command()
def extract(
    query_name: str = typer.Option(),
    output_file_path: str = typer.Option(),
) -> None:
    """Fetch one extraction target from Wikidata and save its raw rows.

    Run once per key of QUERY_CONFIGS so a target-specific QLever failure only
    retries/fails that target instead of every other already-fetched target.
    """
    if query_name not in QUERY_CONFIGS:
        raise typer.BadParameter(
            f"Unknown query_name {query_name!r}. Expected one of {list(QUERY_CONFIGS)}."
        )

    start_time = time.time()

    # Clear cache on qlever to prevent any resource issues
    clear_qlever_cache()

    logger.info(f"Fetch the data in CSV format for {query_name}")

    config = QUERY_CONFIGS[query_name]
    dropped_ids: list[str] = []
    if config.hydration_batch_size:
        logger.info(f"[{query_name}] Pass 1: discovering candidate entities")
        discovery_df = fetch_discovery(query_name).pipe(extract_wikidata_id)
        logger.info(
            f"[{query_name}] Pass 1: found {len(discovery_df)} candidate entities"
        )

        wikidata_ids = discovery_df["wikidata_id"].tolist()
        batch_size = config.hydration_batch_size
        batches = [
            wikidata_ids[i : i + batch_size]
            for i in range(0, len(wikidata_ids), batch_size)
        ]
        logger.info(
            f"[{query_name}] Pass 2: hydrating {len(wikidata_ids)} entities in "
            f"{len(batches)} batches of up to {batch_size}"
        )

        hydration_dfs: list[pd.DataFrame] = []
        for i, batch in enumerate(batches, start=1):
            hydration_dfs.extend(hydrate_batch(query_name, batch, dropped_ids))
            if i < len(batches):
                time.sleep(HYDRATION_BATCH_DELAY_SECONDS)

        # Inner merge: entities in dropped_ids simply have no row in hydration_df,
        # so they're naturally excluded here without extra filtering logic.
        df = (
            discovery_df.merge(
                pd.concat(hydration_dfs, ignore_index=True).pipe(extract_wikidata_id),
                on="wikidata_id",
                how="inner",
            )
            if hydration_dfs
            else pd.DataFrame()
        )
    else:
        query_string = render_query(query_name)
        logger.debug(f"SPARQL Query: \n{query_string}")
        df = fetch_wikidata_qlever_csv(query_string).pipe(extract_wikidata_id)

    if df.empty:
        if query_name == MUSIC_IDS_KEY:
            logger.warning("No music artist IDs retrieved — skipping raw file.")
            return
        error_message = f"No data retrieved for {query_name}."
        logger.error(error_message)
        raise ValueError(error_message)

    logger.info(f"Retrieved {len(df)} rows.")
    logger.info(f"Saving raw results to {output_file_path}")
    df.to_parquet(output_file_path, index=False)
    logger.info(f"Raw results saved successfully to {output_file_path}")

    if dropped_ids:
        logger.warning(
            f"{query_name}: dropped {len(dropped_ids)} entit"
            f"{'y' if len(dropped_ids) == 1 else 'ies'} QLever rejected as too "
            f"expensive even alone: {', '.join(dropped_ids)}"
        )

    elapsed = time.time() - start_time
    dropped_entity_word = "entity" if len(dropped_ids) == 1 else "entities"
    logger.info(
        f"[{query_name}] summary: {len(df)} rows, {len(dropped_ids)} "
        f"{dropped_entity_word} dropped, {elapsed:.1f}s elapsed, "
        f"saved to {output_file_path}"
    )


@app.command()
def merge(
    input_dir_path: str = typer.Option(
        help="Directory holding one <query_name>.parquet raw file per `extract` target."
    ),
    output_file_path: str = typer.Option(),
) -> None:
    """Merge and postprocess the raw per-target files produced by `extract`."""
    dfs: dict[str, pd.DataFrame] = {}

    for query_name in QUERY_CONFIGS:
        raw_file_path = os.path.join(input_dir_path, f"{query_name}.parquet")
        try:
            dfs[query_name] = pd.read_parquet(raw_file_path)
        except FileNotFoundError:
            if query_name == MUSIC_IDS_KEY:
                logger.warning(f"{raw_file_path} not found — skipping music_ids merge.")
                continue
            error_message = (
                f"Missing raw extraction for {query_name} at {raw_file_path}."
            )
            logger.error(error_message)
            raise ValueError(error_message) from None

    logger.info("Merging the data")
    merged_df = merge_data(dfs)

    logger.info("Postprocessing the data")
    postprocessed_df = postprocess_data(merged_df)
    logger.info(
        f"Found {len(postprocessed_df)} unique (wikidata_id, alias) pairs for {postprocessed_df.wikidata_id.nunique()} wikidata_ids"
    )

    logger.info(f"Saving results to {output_file_path}")
    postprocessed_df.to_parquet(output_file_path, index=False)
    logger.info(f"Results saved successfully to {output_file_path}")


if __name__ == "__main__":
    app()
