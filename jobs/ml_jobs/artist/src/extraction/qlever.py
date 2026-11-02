"""Generic HTTP client for QLever (https://qlever.cs.uni-freiburg.de/api/wikidata).

Retry policy and raw CSV fetch — no Wikidata-domain logic (entity filters, ID
properties, query rendering) lives here; that's
src/extraction/wikidata_extraction.py, which calls into this.
"""

from io import StringIO

import pandas as pd
import requests
from loguru import logger
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.extraction.constants import QLEVER_ENDPOINT, QLEVER_HEADERS


def _log_retry_attempt(retry_state) -> None:
    logger.warning(
        f"Attempt {retry_state.attempt_number} failed: {retry_state.outcome.exception()}"
    )


# Shared retry policy for every QLever request: 5 attempts total, exponential
# backoff (10s, 20s, 40s, capped at 60s) between them. Exponential, not linear —
# a retry means something actually went wrong, so give the shared endpoint real
# room to recover instead of coming back quickly.
#
# 5 attempts (not 3): live production traffic shows QLever's own nginx
# front-end plain-HTTP-429 rate-limiting a fresh burst of requests (e.g. the
# cache clear immediately followed by the extraction query), recovering within
# 2-3 retries once several seconds apart. 5 attempts (~130s of cumulative
# backoff) gives real margin against that rate limit without blocking
# indefinitely if a query is genuinely stuck.
qlever_retry = retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=10, max=60),
    retry=retry_if_exception_type(requests.RequestException),
    before_sleep=_log_retry_attempt,
    reraise=True,
)


@qlever_retry
def _clear_qlever_cache_once() -> None:
    response = requests.get(
        QLEVER_ENDPOINT,
        params={"cmd": "clear-cache"},
        headers=QLEVER_HEADERS,
        timeout=30,
    )
    if response.status_code != 200:
        raise requests.RequestException(
            f"Cache clear failed ({response.status_code}): {response.text[:150]}"
        )


def clear_qlever_cache() -> None:
    try:
        _clear_qlever_cache_once()
        logger.info(f"Cache cleared for {QLEVER_ENDPOINT}")
    except requests.RequestException:
        logger.warning(
            "Failed to reset QLever cache after retries. Proceeding with execution..."
        )


@qlever_retry
def fetch_wikidata_qlever_csv(
    sparql_query: str, dtype: dict[str, type]
) -> pd.DataFrame:
    # POST, not GET: the query goes in the request body, so its length is never
    # bounded by a URI-length limit — QLEVER_HEADERS' Content-Type is exactly the
    # SPARQL-protocol "query is the raw POST body" convention this relies on.
    response = requests.post(
        QLEVER_ENDPOINT,
        data=sparql_query.encode("utf-8"),
        headers=QLEVER_HEADERS,
        timeout=120,
    )
    if response.status_code != 200:
        raise requests.RequestException(
            f"Failed to fetch data from {QLEVER_ENDPOINT} "
            f"({response.status_code}): {response.text[:200]}"
        )
    response.encoding = "utf-8"
    return pd.read_csv(StringIO(response.text), dtype=dtype)
