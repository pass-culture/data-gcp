import numpy as np

MAX_DEPTH = 4


def _validate_gtl_id(gtl_id: str):
    """Raise an error if GTL ID is invalid."""
    if not isinstance(gtl_id, str):
        raise TypeError("GTL ID must be a string")
    if len(gtl_id) != 8:
        raise ValueError("GTL ID must be 8 characters long")
    if not gtl_id.isdigit():
        raise ValueError("GTL ID must contain only digits")
    if gtl_id[:2] == "00":
        raise ValueError("GTL ID must not start with '00'")


def _get_gtl_depth(gtl_id: str) -> int:
    """Compute the hierarchical depth level of a GTL identifier.

    The GTL (Geographic or Grouping Taxonomy Level) identifier is expected
    to be an 8-character string composed of 2-character hierarchical codes.
    Each trailing pair of "00" represents a missing depth level (null).

    Examples:
        - "01000000" → depth 1
        - "01020000" → depth 2
        - "01020300" → depth 3
        - "01020304" → depth 4

    Args:
        gtl_id (str): An 8-character GTL identifier.

    Returns:
        int: The hierarchical depth (1 to MAX_DEPTH).

    Raises:
        AssertionError: If the input is not a valid GTL ID (wrong type,
            wrong length, or starts with "00").
    """
    _validate_gtl_id(gtl_id)
    trailing_zero_pairs = (len(gtl_id) - len(gtl_id.rstrip("0"))) // 2
    return MAX_DEPTH - trailing_zero_pairs


def _get_gtl_walk_dist(gtl_id_a: str, gtl_id_b: str) -> float:
    """Compute the shortest path distance between two GTL identifiers in the GTL forest.

    Each level-1 GTL is the root of a tree. If the first two characters differ,
    the identifiers are considered unrelated (distance = ∞). Otherwise, the distance
    depends on how deep their shared prefix extends.

    Args:
        gtl_id_a (str): First GTL identifier (8-character string).
        gtl_id_b (str): Second GTL identifier (8-character string).

    Returns:
        float: A numeric distance value:
            - `np.inf` if the GTLs are unrelated.
            - A smaller positive number for closer relationships.

    Examples:
        >>> _get_gtl_dist("01020300", "01020400")
        2.0
        >>> _get_gtl_dist("01000000", "02000000")
        inf
    """
    if gtl_id_a[:2] != gtl_id_b[:2]:
        return np.inf

    # Split GTL into 2-character hierarchical levels
    chunks1 = [gtl_id_a[i : i + 2] for i in range(0, 8, 2)]
    chunks2 = [gtl_id_b[i : i + 2] for i in range(0, 8, 2)]

    dist = np.inf
    for idx, (a, b) in enumerate(zip(chunks1, chunks2, strict=True), start=1):
        if a != b or (a == "00" and b == "00"):
            return dist
        dist = MAX_DEPTH - idx
    return 0


def get_gtl_walk_score(query_gtl_id: str, result_gtl_id: str) -> float:
    """Compute a similarity score between two GTL identifiers.

    The score is the inverse of their hierarchical distance. Higher values indicate
    greater similarity (closer relationship in the GTL taxonomy).

    Args:
        query_gtl_id (str): The query GTL identifier.
        result_gtl_id (str): The result GTL identifier.

    Returns:
        float: Similarity score in the range (0, 1].

    Examples:
        >>> get_gtl_walk_score("01020000", "01020300")
        0.33
        >>> get_gtl_walk_score("01020300", "01020000")
        0.33
    """
    dist = _get_gtl_walk_dist(query_gtl_id, result_gtl_id)
    if dist == np.inf:
        return 0.0
    return 1 / (1 + dist)


def get_gtl_retrieval_score(query_gtl_id: str, result_gtl_id: str) -> float:
    """Compute an asymmetric matching score between two GTL identifiers.

    This function measures how much of the query hierarchy is correctly matched
    by the result. The score is normalized by the query's depth, ensuring deeper
    queries are penalized more heavily when mismatches occur.

    Args:
        query_gtl_id (str): The query GTL identifier.
        result_gtl_id (str): The GTL identifier to compare against.

    Returns:
        float: Asymmetric score in the range [0, 1].

    Examples:
        >>> get_gtl_retrieval_score("01020000", "01020300")
        1
        >>> get_gtl_retrieval_score("01020300", "01020000")
        0.66
    """
    if query_gtl_id[:2] != result_gtl_id[:2]:
        return 0.0

    # Split GTL into 2-character hierarchical levels
    chunks1 = [query_gtl_id[i : i + 2] for i in range(0, 8, 2)]
    chunks2 = [result_gtl_id[i : i + 2] for i in range(0, 8, 2)]
    gtl_query_depth = _get_gtl_depth(query_gtl_id)

    matching_parts = 0
    for a, b in zip(chunks1, chunks2, strict=True):
        if a != b or (a == "00" and b == "00"):
            return matching_parts / gtl_query_depth
        matching_parts += 1
    return matching_parts / gtl_query_depth
