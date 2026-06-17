import numpy as np

MAX_DEPTH = 4
_GTL_PREFIX_SEP = "-"


def _strip_gtl_prefix(gtl_id: str) -> str:
    """Strip an optional item-type prefix from a GTL identifier.

    When GTL IDs are prefixed at the source (e.g. ``"b-01020300"`` for books,
    ``"m-01020300"`` for music), this function returns the bare 8-character
    numeric code.  IDs that are already unprefixed are returned unchanged.

    Examples:
        >>> _strip_gtl_prefix("b-01020300")
        '01020300'
        >>> _strip_gtl_prefix("m-01020300")
        '01020300'
        >>> _strip_gtl_prefix("01020300")
        '01020300'
    """
    if _GTL_PREFIX_SEP in gtl_id:
        return gtl_id.split(_GTL_PREFIX_SEP, 1)[1]
    return gtl_id


# Extract and compare item-type prefixes before stripping.
# A book GTL and a music GTL are semantically unrelated even when their
# numeric codes are identical — cross-type comparisons always return 0.0.
def _extract_prefix(gtl_id: str) -> str | None:
    return gtl_id.split(_GTL_PREFIX_SEP, 1)[0] if _GTL_PREFIX_SEP in gtl_id else None


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
    Accepts both bare GTL IDs (``"01020000"``) and prefixed ones (``"b-01020000"``).

    Args:
        query_gtl_id (str): The query GTL identifier.
        result_gtl_id (str): The result GTL identifier.

    Returns:
        float: Similarity score in the range (0, 1].

    Examples:
        >>> get_gtl_walk_score("01020000", "01020300")
        0.33
        >>> get_gtl_walk_score("b-01020000", "b-01020300")
        0.33
    """
    # Early exit if None
    if query_gtl_id is None:
        return None
    if result_gtl_id is None:
        return 0.0

    dist = _get_gtl_walk_dist(
        _strip_gtl_prefix(query_gtl_id), _strip_gtl_prefix(result_gtl_id)
    )
    if dist == np.inf:
        return 0.0
    return 1 / (1 + dist)


def get_gtl_retrieval_score(
    query_gtl_id: str | None, result_gtl_id: str | None
) -> float | None:
    """
    Compute a depth-normalized asymmetric matching score between two GTL identifiers.

    This metric normalizes by the query's depth, which means:
    - Deeper queries score higher when missing the same absolute number of levels
    - A result missing 1 level from a depth-4 query scores higher (3/4 = 0.75) than
      a result missing 1 level from a depth-3 query (2/3 = 0.667)
    - Penalizes missing deeper (more specific) levels more heavily

    Accepts both bare GTL IDs (``"01020301"``) and prefixed ones (``"b-01020301"``).
    The prefix is stripped before comparison, so cross-type comparisons (e.g.
    ``"b-01020301"`` vs ``"m-01020301"``) correctly return 0.0 only when the
    numeric prefixes differ — the item-type tag itself does **not** influence the score.

    Use this metric when:
    - You want to emphasize precision in deeper hierarchies
    - Missing a child node should be penalized less than missing a parent node
    - Query specificity should influence the scoring

    Args:
        query_gtl_id: The query GTL identifier (e.g. "01020301" or "b-01020301")
        result_gtl_id: The result GTL identifier to score against the query

    Returns:
        float: Asymmetric score in the range [0, 1], where:
               - 1.0 = result matches or is a descendant of all query levels
               - 0.0 = result shares no hierarchy with query (different root)
               - Intermediate = proportion of query levels matched

    Examples:
        >>> get_gtl_retrieval_score("01020301", "01020301")
        1.0  # Exact match

        >>> get_gtl_retrieval_score("01020301", "01020302")
        0.75  # 3 out of 4 query levels match

        >>> get_gtl_retrieval_score("01020000", "01020301")
        1.0  # Result is descendant of query (all query levels match)

        >>> get_gtl_retrieval_score("01020301", "01030000")
        0.25  # Only 1 out of 4 query levels match

        >>> get_gtl_retrieval_score("b-01020301", "b-01020302")
        0.75  # Prefix stripped before comparison, 3 out of 4 query levels match

        >>> get_gtl_retrieval_score("b-01020301", "m-01020301")
        0.0  # Cross-type: different item-type prefix → always 0.0
        even if numeric codes are identical
    """

    # Early exit if completely independent (different first level) or None
    if query_gtl_id is None:
        return None

    if result_gtl_id is None or _extract_prefix(query_gtl_id) != _extract_prefix(
        result_gtl_id
    ):
        return 0.0

    query_gtl_id = _strip_gtl_prefix(query_gtl_id)
    result_gtl_id = _strip_gtl_prefix(result_gtl_id)
    # Early exit if the root level (first 2 characters) does not match
    if query_gtl_id[:2] != result_gtl_id[:2]:
        return 0.0

    # Split GTL IDs into hierarchical levels
    q_chunks = [query_gtl_id[i : i + 2] for i in range(0, 8, 2)]
    r_chunks = [result_gtl_id[i : i + 2] for i in range(0, 8, 2)]
    query_depth = _get_gtl_depth(query_gtl_id)

    # Check if result fully matches all query levels (subset/descendant case)
    # Result levels deeper than query depth are not considered
    if all(q_chunks[i] == r_chunks[i] for i in range(query_depth)):
        return 1.0

    # Count number of shared hierarchical levels
    matching_parts = 0
    for a, b in zip(q_chunks, r_chunks, strict=True):
        if a != b or (a == "00" and b == "00"):
            break
        matching_parts += 1

    # Normalize by query depth
    return matching_parts / query_depth
