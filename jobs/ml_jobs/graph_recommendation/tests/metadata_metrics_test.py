"""Simple tests for gtl metrics utilities."""

from __future__ import annotations

import numpy as np
import pytest

from src.utils.metadata_metrics import (
    _get_gtl_depth,
    _get_gtl_walk_dist,
    get_gtl_retrieval_score,
    get_gtl_walk_score,
)


def test_get_gtl_depth():
    assert _get_gtl_depth("01000000") == 1
    assert _get_gtl_depth("01020000") == 2
    assert _get_gtl_depth("01020300") == 3
    assert _get_gtl_depth("01020304") == 4

    with pytest.raises(ValueError, match="GTL ID must not start with '00'"):
        _get_gtl_depth("00100000")  # starts with 00
    with pytest.raises(ValueError, match="GTL ID must be 8 characters long"):
        _get_gtl_depth("010203")  # too short
    with pytest.raises(ValueError, match="GTL ID must be 8 characters long"):
        _get_gtl_depth("01 02 03 00")  # too long
    with pytest.raises(ValueError, match="GTL ID must contain only digits"):
        _get_gtl_depth("0010000e")  # not a digit string
    with pytest.raises(TypeError):
        _get_gtl_depth(1020300)  # not a string
    with pytest.raises(
        ValueError,
        match="Invalid GTL ID: null branches cannot include additional sublevels",
    ):
        _get_gtl_depth("01000001")  # branching from null


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
    assert pytest.approx(get_gtl_walk_score("01020301", "01020301"), 1e-3) == 1.0
    assert pytest.approx(get_gtl_walk_score("01020100", "01020100"), 1e-3) == 1 / 2
    assert pytest.approx(get_gtl_walk_score("01020304", "01020300"), 1e-3) == 1 / 2
    assert pytest.approx(get_gtl_walk_score("01020100", "01020101"), 1e-3) == 1 / 2
    assert pytest.approx(get_gtl_walk_score("01020300", "01020400"), 1e-3) == 1 / 3
    assert pytest.approx(get_gtl_walk_score("01020301", "01020401"), 1e-3) == 1 / 3
    assert pytest.approx(get_gtl_walk_score("01000000", "01103020"), 1e-3) == 1 / 4
    assert pytest.approx(get_gtl_walk_score("01203020", "01103020"), 1e-3) == 1 / 4
    assert pytest.approx(get_gtl_walk_score("01000000", "02000000"), 1e-3) == 0.0


def test_gtl_retrieval_score_independent_gtls():
    """
    Test: Independent GTL sets (different root levels) should return minimum score.

    When two GTLs have completely different hierarchies (different first level),
    they share nothing in common, so the score should be 0.
    """
    assert pytest.approx(get_gtl_retrieval_score("01000000", "02000000"), 1e-3) == 0.0


def test_gtl_retrieval_score_identical_gtls():
    """
    Test: Identical GTLs should return maximum score regardless of depth.

    When query and result are identical, all query levels match, so score = 1.0
    """
    # Depth 4 query with identical result
    assert pytest.approx(get_gtl_retrieval_score("01020301", "01020301"), 1e-3) == 1.0

    # Depth 3 query with identical result
    assert pytest.approx(get_gtl_retrieval_score("01020100", "01020100"), 1e-3) == 1.0

    # Depth 1 query with identical result
    assert pytest.approx(get_gtl_retrieval_score("01000000", "01000000"), 1e-3) == 1.0


def test_gtl_retrieval_score_descendant_results():
    """
    Test: Results that are descendants of the query should return maximum score.

    When a result contains all query levels plus additional deeper levels,
    all query requirements are satisfied, so score = 1.0
    """
    # Depth 1 query, result has 3 additional deeper levels
    assert pytest.approx(get_gtl_retrieval_score("01000000", "01103020"), 1e-3) == 1.0

    # Depth 3 query, result has 1 additional deeper level
    assert pytest.approx(get_gtl_retrieval_score("01020100", "01020101"), 1e-3) == 1.0

    # Depth 2 query, result has 2 additional deeper levels
    assert pytest.approx(get_gtl_retrieval_score("01020000", "01020304"), 1e-3) == 1.0


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
    assert pytest.approx(miss_depth_4_score, 1e-3) == 3 / 4  # 0.75

    # Depth 3 query missing only level 3: 2 out of 3 levels match
    miss_depth_3_score = get_gtl_retrieval_score("01020300", "01020401")
    assert pytest.approx(miss_depth_3_score, 1e-3) == 2 / 3  # ~0.667

    # Depth 2 query missing only level 2: 1 out of 2 levels match
    miss_depth_2_score = get_gtl_retrieval_score("01020000", "01010401")
    assert pytest.approx(miss_depth_2_score, 1e-3) == 1 / 2  # 0.5

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
    assert pytest.approx(three_shared, 1e-3) == 3 / 4  # 0.75

    # Result matches 2 out of 4 query levels (diverges at level 3)
    two_shared = get_gtl_retrieval_score(query, "01020400")
    assert pytest.approx(two_shared, 1e-3) == 2 / 4  # 0.5

    # Result matches 1 out of 4 query levels (diverges at level 2)
    one_shared = get_gtl_retrieval_score(query, "01030301")
    assert pytest.approx(one_shared, 1e-3) == 1 / 4  # 0.25

    # Verify the ordering
    assert three_shared > two_shared > one_shared


def test_gtl_retrieval_score_various_depths():
    """
    Test: Verify correct scoring across various query depths.

    This demonstrates how depth normalization affects the absolute score values.
    """
    # Shallow query (depth 1) with partial match
    score_depth_1 = get_gtl_retrieval_score("01000000", "01020000")
    assert pytest.approx(score_depth_1, 1e-3) == 1.0  # Result is descendant

    # Medium query (depth 2) with 1 level matching
    score_depth_2 = get_gtl_retrieval_score("01020000", "01030000")
    assert pytest.approx(score_depth_2, 1e-3) == 1 / 2  # 0.5

    # Deep query (depth 4) with 2 levels matching
    score_depth_4 = get_gtl_retrieval_score("01020304", "01020000")
    assert pytest.approx(score_depth_4, 1e-3) == 2 / 4  # 0.5
