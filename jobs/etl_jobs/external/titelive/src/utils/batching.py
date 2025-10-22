"""Utilities for batching and pagination."""

import math


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
