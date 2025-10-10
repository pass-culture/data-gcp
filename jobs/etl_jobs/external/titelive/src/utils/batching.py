"""Utilities for batching and pagination."""

import math
from collections.abc import Iterator


def chunk_list(items: list, chunk_size: int) -> Iterator[list]:
    """
    Split a list into chunks of specified size.

    Args:
        items: List to chunk
        chunk_size: Size of each chunk

    Yields:
        Chunks of the original list

    Raises:
        ValueError: If chunk_size is less than 1
    """
    if chunk_size < 1:
        msg = "Chunk size must be at least 1"
        raise ValueError(msg)

    for i in range(0, len(items), chunk_size):
        yield items[i : i + chunk_size]


def calculate_total_pages(total_results: int, per_page: int) -> int:
    """
    Calculate total number of pages needed for pagination.

    Args:
        total_results: Total number of results
        per_page: Number of results per page

    Returns:
        Total number of pages

    Raises:
        ValueError: If per_page is less than 1
    """
    if per_page < 1:
        msg = "Per page value must be at least 1"
        raise ValueError(msg)

    return math.ceil(total_results / per_page)
