"""Simple tests for gtl metrics utilities."""

from __future__ import annotations

import numpy as np
import pytest

from src.utils.metadata_metrics import (
    _get_gtl_depth,
    _get_gtl_walk_dist,
    _strip_gtl_prefix,
    get_gtl_retrieval_score,
    get_gtl_walk_score,
)


def test_strip_gtl_prefix() -> None:
    """Test that item-type prefixes are correctly stripped."""
    assert _strip_gtl_prefix("b-01020300") == "01020300"
    assert _strip_gtl_prefix("m-01020300") == "01020300"
    assert _strip_gtl_prefix("01020300") == "01020300"  # no prefix → unchanged


# ...existing code...


def test_gtl_retrieval_score_with_prefix() -> None:
    """Test that prefixed GTL IDs are scored correctly after stripping the prefix."""
    # Same prefix → same as unprefixed comparison
    assert get_gtl_retrieval_score("b-01020301", "b-01020301") == pytest.approx(1.0, abs=1e-3)
    assert get_gtl_retrieval_score("b-01020301", "b-01020302") == pytest.approx(3 / 4, abs=1e-3)
    assert get_gtl_retrieval_score("m-01020301", "m-01020301") == pytest.approx(1.0, abs=1e-3)


def test_gtl_retrieval_score_cross_type_is_zero() -> None:
    """Test that cross-type comparisons always return 0.0.

    A book GTL and a music GTL are semantically unrelated even when their
    numeric codes are identical, so the score must be 0.0.
    """
    assert get_gtl_retrieval_score("b-01020301", "m-01020301") == pytest.approx(0.0, abs=1e-3)
    assert get_gtl_retrieval_score("m-01000000", "b-01000000") == pytest.approx(0.0, abs=1e-3)
    assert get_gtl_retrieval_score("b-01020301", "m-02000000") == pytest.approx(0.0, abs=1e-3)


def test_gtl_retrieval_score_unprefixed_vs_prefixed_is_zero() -> None:
    """Test that comparing prefixed and unprefixed IDs returns 0.0.

    A prefixed ID has an item-type prefix (e.g. "b-"), while an unprefixed one
    does not. They should be treated as different types.
    """
    assert get_gtl_retrieval_score("b-01020301", "01020301") == pytest.approx(0.0, abs=1e-3)
    assert get_gtl_retrieval_score("01020301", "b-01020301") == pytest.approx(0.0, abs=1e-3)


def test_get_gtl_depth():
    assert _get_gtl_depth("01000000") == 1
    assert _get_gtl_depth("01020000") == 2
    assert _get_gtl_depth("01020300") == 3
    assert _get_gtl_depth("01020304") == 4


def test_get_gtl_walk_dist():
    assert _get_gtl_walk_dist("01020301", "01020301") == 0
    assert _get_gtl_walk_dist("01020100", "01020100") == 1
    assert _get_gtl_walk_dist("01020304", "01020300") == 1
    assert _get_gtl_walk_dist("01020100", "01020101") == 1
    assert _get_gtl_walk_dist("01020300", "01020400") == 2
    assert _get_gtl_walk_dist("01020301", "01020401") == 2
    assert _get_gtl_walk_dist("01000000", "01103020") == 3
    assert _get_gtl_walk_dist("01203020", "01103020") == 3
    assert _get_gtl_walk_dist("01000000", "02000000") == np.inf


def test_get_gtl_walk_score():
    assert get_gtl_walk_score("01020301", "01020301") == pytest.approx(1, abs=1e-3)
    assert get_gtl_walk_score("01020100", "01020100") == pytest.approx(1 / 2, abs=1e-3)
    assert get_gtl_walk_score("01020304", "01020300") == pytest.approx(1 / 2, abs=1e-3)
    assert get_gtl_walk_score("01020100", "01020101") == pytest.approx(1 / 2, abs=1e-3)
    assert get_gtl_walk_score("01020300", "01020400") == pytest.approx(1 / 3, abs=1e-3)
    assert get_gtl_walk_score("01020301", "01020401") == pytest.approx(1 / 3, abs=1e-3)
    assert get_gtl_walk_score("01000000", "01103020") == pytest.approx(1 / 4, abs=1e-3)
    assert get_gtl_walk_score("01203020", "01103020") == pytest.approx(1 / 4, abs=1e-3)
    assert get_gtl_walk_score("01000000", "02000000") == pytest.approx(0, abs=1e-3)


def test_gtl_retrieval_score_independent_gtls():
    """
    Test: Independent GTL sets (different root levels) should return minimum score.

    When two GTLs have completely different hierarchies (different first level),
    they share nothing in common, so the score should be 0.
    """
    assert get_gtl_retrieval_score("01000000", "02000000") == pytest.approx(0, abs=1e-3)


def test_gtl_retrieval_score_identical_gtls():
    """
    Test: Identical GTLs should return maximum score regardless of depth.

    When query and result are identical, all query levels match, so score = 1
    """
    assert get_gtl_retrieval_score("01020301", "01020301") == pytest.approx(1, abs=1e-3)
    assert get_gtl_retrieval_score("01020100", "01020100") == pytest.approx(1, abs=1e-3)
    assert get_gtl_retrieval_score("01000000", "01000000") == pytest.approx(1, abs=1e-3)


def test_gtl_retrieval_score_descendant_results():
    """
    Test: Results that are descendants of the query should return maximum score.

    When a result contains all query levels plus additional deeper levels,
    all query requirements are satisfied, so score = 1.0
    """
    assert get_gtl_retrieval_score("01000000", "01103020") == pytest.approx(1, abs=1e-3)
    assert get_gtl_retrieval_score("01020100", "01020101") == pytest.approx(1, abs=1e-3)
    assert get_gtl_retrieval_score("01020000", "01020304") == pytest.approx(1, abs=1e-3)


def test_gtl_retrieval_score_deeper_queries_score_higher():
    """
    Test: Deeper queries score higher when missing the same deepest level.

    KEY PROPERTY: When comparing results that miss only the deepest query level,
    deeper queries should score higher because they successfully match more levels.

    This reflects that missing a specific detail (level 4) in a specific query
    is less severe than missing a broader category (level 3) in a general query.
    """
    # Depth 4 query missing only level 4: 3 out of 4 levels match
    miss_depth_4_score = get_gtl_retrieval_score("01020304", "01020301")
    assert miss_depth_4_score == pytest.approx(3 / 4, abs=1e-3)  # 0.75

    # Depth 3 query missing only level 3: 2 out of 3 levels match
    miss_depth_3_score = get_gtl_retrieval_score("01020300", "01020401")
    assert miss_depth_3_score == pytest.approx(2 / 3, abs=1e-3)  # ~0.667

    # Depth 2 query missing only level 2: 1 out of 2 levels match
    miss_depth_2_score = get_gtl_retrieval_score("01020000", "01010401")
    assert miss_depth_2_score == pytest.approx(1 / 2, abs=1e-3)  # 0.5

    # Verify the ordering: deeper queries score higher
    assert miss_depth_4_score > miss_depth_3_score > miss_depth_2_score > 0


def test_gtl_retrieval_score_fewer_matches_score_lower():
    """
    Test: For a given query, fewer matching parent levels result in lower scores.

    This tests the basic property that scores decrease as the hierarchical
    similarity decreases.
    """
    query = "01020301"  # Depth 4 query

    # Result matches 3 out of 4 query levels (missing only level 4)
    three_shared = get_gtl_retrieval_score(query, "01020302")
    assert three_shared == pytest.approx(3 / 4, abs=1e-3)  # 0.75

    # Result matches 2 out of 4 query levels (diverges at level 3)
    two_shared = get_gtl_retrieval_score(query, "01020400")
    assert two_shared == pytest.approx(2 / 4, abs=1e-3)  # 0.5

    # Result matches 1 out of 4 query levels (diverges at level 2)
    one_shared = get_gtl_retrieval_score(query, "01030301")
    assert one_shared == pytest.approx(1 / 4, abs=1e-3)  # 0.25

    # Verify the ordering
    assert three_shared > two_shared > one_shared


def test_gtl_retrieval_score_various_depths():
    """
    Test: Verify correct scoring across various query depths.

    This demonstrates how depth normalization affects the absolute score values.
    """
    # Shallow query (depth 1) with partial match
    score_depth_1 = get_gtl_retrieval_score("01000000", "01020000")
    assert score_depth_1 == pytest.approx(1, abs=1e-3)  # Result is descendant

    # Medium query (depth 2) with 1 level matching
    score_depth_2 = get_gtl_retrieval_score("01020000", "01030000")
    assert score_depth_2 == pytest.approx(1 / 2, abs=1e-3)  # 0.5

    # Deep query (depth 4) with 2 levels matching
    score_depth_4 = get_gtl_retrieval_score("01020304", "01020000")
    assert score_depth_4 == pytest.approx(2 / 4, abs=1e-3)  # 0.5
