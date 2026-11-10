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
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from src.extraction.constants import QLEVER_ENDPOINT, QLEVER_HEADERS

# QLever streams results, so a mid-export failure (typically "Operation timed
# out") still returns HTTP 200: the error is appended after this marker to the
# partial CSV body.
QLEVER_EXPORT_ERROR_MARKER = "An error has occurred while exporting the query result"


def _log_retry_attempt(retry_state) -> None:
    logger.warning(
        f"Attempt {retry_state.attempt_number} failed: {retry_state.outcome.exception()}"
    )


def _is_retryable(exc: BaseException) -> bool:
    # 4xx other than 429 (e.g. a malformed query) won't fix itself on retry.
    # `is not None`, not truthiness: a 4xx Response is falsy.
    response = getattr(exc, "response", None)
    if response is not None and 400 <= response.status_code < 500:
        return response.status_code == 429
    return isinstance(exc, requests.RequestException)


# 4 attempts with exponential backoff between them (30s, 60s, 120s — ~210s of
# waiting in total). Retries 429s (QLever's nginx front-end rate-limits bursts
# and typically recovers after a few spaced-out retries), 5xx and network
# errors; fails fast on any other 4xx (see _is_retryable).
qlever_retry = retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=30, max=240),
    retry=retry_if_exception(_is_retryable),
    before_sleep=_log_retry_attempt,
    reraise=True,
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
            f"({response.status_code}): {response.text[:200]}",
            response=response,
        )
    response.encoding = "utf-8"
    error_start = response.text.find(QLEVER_EXPORT_ERROR_MARKER)
    if error_start != -1:
        # No `response=`: a truncated export is retryable (see _is_retryable).
        raise requests.RequestException(
            f"Truncated result from {QLEVER_ENDPOINT}: "
            f"{response.text[error_start:].strip()[-200:]}"
        )
    return pd.read_csv(StringIO(response.text), dtype=dtype)
