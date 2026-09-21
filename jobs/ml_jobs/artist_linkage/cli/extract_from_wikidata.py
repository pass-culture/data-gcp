import os
import time
from io import StringIO

import pandas as pd
import requests
import typer
from loguru import logger

from src.constants import WIKIDATA_ID_KEY
from src.utils.preprocessing_utils import normalize_string_series
from src.wikidata_config import MUSIC_IDS_KEY, QUERY_CONFIGS, render_query

QLEVER_ENDPOINT = "https://qlever.cs.uni-freiburg.de/api/wikidata"
QLEVER_HEADERS = {"Accept": "text/csv", "Content-Type": "application/sparql-query"}

# Below this width (in numeric Wikidata IDs), give up on splitting a batch further
# and surface it as a real failure instead of recursing indefinitely.
BATCH_MIN_WIDTH = 50_000

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


def clear_qlever_cache(retries: int = 3, backoff_factor: int = 5) -> None:
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

        time.sleep(backoff_factor * (attempt + 1))

    logger.warning(
        "Failed to reset QLever cache after retries. Proceeding with execution..."
    )


def fetch_wikidata_qlever_csv(
    sparql_query: str, retries: int = 3, backoff_factor: int = 5
) -> pd.DataFrame:
    for attempt in range(retries):
        try:
            response = requests.get(
                QLEVER_ENDPOINT,
                params={"query": sparql_query},
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

        time.sleep(backoff_factor * (attempt + 1))

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


def fetch_wikidata_qlever_csv_range(sparql_query: str) -> pd.DataFrame:
    """Single-attempt fetch for one batch of a range-partitioned query.

    Raises QLeverQueryTooExpensive (no retry — see `_is_cost_rejection`) so the
    caller can bisect the range instead; retries transient failures like
    `fetch_wikidata_qlever_csv` does.
    """
    retries, backoff_factor = 3, 5
    for attempt in range(retries):
        try:
            response = requests.get(
                QLEVER_ENDPOINT,
                params={"query": sparql_query},
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

        time.sleep(backoff_factor * (attempt + 1))

    raise requests.RequestException(
        f"Failed to fetch data from {QLEVER_ENDPOINT} after {retries} attempts."
    )


def fetch_batch_range(
    query_name: str, lo: int, hi: int, min_width: int = BATCH_MIN_WIDTH
) -> list[pd.DataFrame]:
    """Fetch one numeric-ID range of a batched query, splitting it in half and
    recursing whenever QLever rejects it as too expensive, down to `min_width`.

    Candidate count alone doesn't predict cost reliably (older, lower-numbered
    Wikidata entities carry richer multi-valued data), so this adapts to whatever
    the real cost distribution turns out to be instead of trusting a fixed
    partition — see GKG_ID_BATCH_RANGES and extract_artists_flat.rq.j2.
    """
    query = render_query(query_name, id_range=(lo, hi))
    try:
        df = fetch_wikidata_qlever_csv_range(query)
    except QLeverQueryTooExpensive:
        if hi - lo <= min_width:
            raise ValueError(
                f"QLever still rejects {query_name} batch Q{lo}-Q{hi} as too "
                f"expensive even at the minimum batch width ({min_width})."
            ) from None
        mid = (lo + hi) // 2
        logger.info(
            f"{query_name} batch Q{lo}-Q{hi} too expensive for QLever — "
            f"splitting into Q{lo}-Q{mid} and Q{mid}-Q{hi}."
        )
        return fetch_batch_range(query_name, lo, mid, min_width) + fetch_batch_range(
            query_name, mid, hi, min_width
        )

    logger.info(f"{query_name} batch Q{lo}-Q{hi}: retrieved {len(df)} rows.")
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

    # Clear cache on qlever to prevent any resource issues
    clear_qlever_cache()

    logger.info(f"Fetch the data in CSV format for {query_name}")

    config = QUERY_CONFIGS[query_name]
    if config.batch_ranges:
        dfs = [
            batch_df
            for lo, hi in config.batch_ranges
            for batch_df in fetch_batch_range(query_name, lo, hi)
        ]
        # No .pipe(extract_wikidata_id) when empty: an empty concat result has no
        # wikidata_id column to strip the URI prefix from. Falls through to the
        # empty-data check below either way.
        df = (
            pd.concat(dfs, ignore_index=True).pipe(extract_wikidata_id)
            if dfs
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
