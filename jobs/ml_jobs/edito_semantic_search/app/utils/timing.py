"""Lightweight timing utilities for structured logging."""

import time
from contextlib import contextmanager

from loguru import logger


@contextmanager
def log_timing(label: str):
    """Context manager that logs elapsed time for a block of code.

    Usage:
        with log_timing("Vector search"):
            results = client.vector_search(...)
        # logs: "Vector search completed in 0.42s"

    The elapsed time (in seconds) is also available via the yielded dict:
        with log_timing("LLM call") as t:
            ...
        print(t["elapsed"])
    """
    timing: dict[str, float] = {}
    start = time.time()
    try:
        yield timing
    finally:
        timing["elapsed"] = time.time() - start
        logger.info(f"{label} completed in {timing['elapsed']:.2f}s")
