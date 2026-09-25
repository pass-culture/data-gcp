"""Generic HTTP client for QLever (https://qlever.cs.uni-freiburg.de/api/wikidata).

Retry policy, cost-rejection detection, and raw CSV fetch — no Wikidata-domain
logic (entity filters, ID properties, the two-pass discovery+hydration pattern)
lives here; that's src/extraction/wikidata_extraction.py, which calls into this.
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

QLEVER_ENDPOINT = "https://qlever.cs.uni-freiburg.de/api/wikidata"
QLEVER_HEADERS = {
    "Accept": "text/csv",
    "Content-Type": "application/sparql-query",
    # Same identification string used for other external APIs (see
    # src.common.constants.WIKIMEDIA_REQUEST_HEADER) — good practice for any shared
    # third-party endpoint, and QLever's own docs ask for one explicitly.
    "User-Agent": "PassCulture/1.0 (https://passculture.app; contact@passculture.app) Python/requests",
}


class QLeverQueryTooExpensive(Exception):
    """QLever rejected a query as too costly to run — split it and retry, don't
    just retry the identical (deterministically doomed) query."""


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
# front-end plain-HTTP-429 rate-limiting a fresh burst of requests (cache
# clear, then Pass 1, then Pass 2's first batch, fired back to back) — every
# one of those got a 429 on its very first attempt, but recovered within 2-3
# retries once several seconds apart. 3 attempts (30s of cumulative backoff)
# was enough for the lighter calls but not quite enough for the heaviest one
# (a ~480KB hydration batch POST), which then exhausted retries and crashed
# the whole extract task — see failure_log.txt. 5 attempts (~130s of
# cumulative backoff) gives real margin against that same rate limit without
# blocking indefinitely if a batch is genuinely stuck.
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
def fetch_wikidata_qlever_csv(sparql_query: str) -> pd.DataFrame:
    # POST, not GET: a large VALUES-scoped hydration query can run to tens of KB,
    # well past a GET URI's length limit (confirmed live: 414 Request-URI Too
    # Large at ~30KB). POST puts the query in the body instead, with no such
    # ceiling — QLEVER_HEADERS' Content-Type is exactly the SPARQL-protocol
    # "query is the raw POST body" convention this relies on.
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
    return pd.read_csv(StringIO(response.text))


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


@qlever_retry
def fetch_wikidata_qlever_csv_batch(sparql_query: str) -> pd.DataFrame:
    """Fetch one VALUES-scoped batch query.

    Raises QLeverQueryTooExpensive without retrying (`retry_if_exception_type`
    on `qlever_retry` only matches `requests.RequestException` — this doesn't
    subclass it) so the caller can bisect the batch instead; transient failures
    still retry like `fetch_wikidata_qlever_csv` does.
    """
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

    raise requests.RequestException(
        f"Failed to fetch data from {QLEVER_ENDPOINT} "
        f"({response.status_code}): {response.text[:200]}"
    )
