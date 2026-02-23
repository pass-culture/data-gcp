import logging
import time
from collections import deque

import httpx

from constants import (
    API_BASE_URL,
    API_MOVIE_ENDPOINT,
    API_REQUESTED_BATCH_SIZE,
    MAX_API_RETRIES,
    RATE_LIMIT_BACKOFF,
    RATE_LIMIT_CALLS,
    RATE_LIMIT_PERIOD,
)

logger = logging.getLogger(__name__)


class TokenBucketRateLimiter:
    def __init__(self, calls: int, period: float, backoff: float = RATE_LIMIT_BACKOFF):
        self.calls = calls
        self.period = period
        self.backoff = backoff
        self._timestamps: deque = deque()

    def wait(self) -> None:
        now = time.monotonic()
        while len(self._timestamps) >= self.calls:
            oldest = self._timestamps[0]
            elapsed = now - oldest
            if elapsed >= self.period:
                self._timestamps.popleft()
            else:
                time.sleep(self.period - elapsed)
                now = time.monotonic()
        self._timestamps.append(now)

    def backoff_sleep(self) -> None:
        logger.warning("Rate limited (429). Backing off for %ss...", self.backoff)
        time.sleep(self.backoff)


class AllocineClient:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self._rate_limiter = TokenBucketRateLimiter(calls=RATE_LIMIT_CALLS, period=RATE_LIMIT_PERIOD)
        self._http = httpx.Client(timeout=30.0)

    def _get(self, endpoint: str, params: dict) -> dict:
        url = f"{API_BASE_URL}{endpoint}"
        for attempt in range(MAX_API_RETRIES + 1):
            self._rate_limiter.wait()
            try:
                response = self._http.get(url, params=params)
                if response.status_code == 429:
                    if attempt < MAX_API_RETRIES:
                        self._rate_limiter.backoff_sleep()
                        continue
                    response.raise_for_status()
                elif response.status_code == 401:
                    raise ValueError("Unauthorized (401): check your API key.")
                else:
                    response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as e:
                logger.error("HTTP error on attempt %d: %s", attempt + 1, e)
                if attempt >= MAX_API_RETRIES:
                    raise
        raise RuntimeError("Exhausted retries without a valid response.")

    def get_movies(self, after_cursor: str | None = None, first: int = API_REQUESTED_BATCH_SIZE) -> dict:
        params: dict = {"token": self.api_key, "first": first}
        if after_cursor:
            params["after"] = after_cursor
        return self._get(API_MOVIE_ENDPOINT, params)

    def fetch_all_movies(self) -> list[dict]:
        all_movies: list[dict] = []
        cursor: str | None = None
        page = 0

        while True:
            page += 1
            logger.info("Fetching page %d (cursor=%r)...", page, cursor)
            response = self.get_movies(after_cursor=cursor)

            if not response:
                logger.warning("Empty response, stopping pagination.")
                break

            edges = response.get("movieList", {}).get("edges", [])
            if not edges:
                break

            for edge in edges:
                node = edge.get("node", {})
                if node:
                    all_movies.append(node)

            page_info = response.get("movieList", {}).get("pageInfo", {})
            has_next = page_info.get("hasNextPage", False)
            cursor = page_info.get("endCursor")

            actual_batch = len(edges)
            if actual_batch < API_REQUESTED_BATCH_SIZE:
                logger.debug(
                    "API returned %d items, requested %d (server may be capping page size).",
                    actual_batch,
                    API_REQUESTED_BATCH_SIZE,
                )

            if not has_next or not cursor:
                break

        logger.info("Total movies fetched: %d", len(all_movies))
        return all_movies

    def close(self) -> None:
        self._http.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
