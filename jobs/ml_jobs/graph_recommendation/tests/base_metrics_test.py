"""Simple tests for gtl metrics utilities."""

from __future__ import annotations

import numpy as np
import pytest
from utils.base_metrics import (
    _get_gtl_depth,
    _get_gtl_dist,
    get_gtl_matching_score,
    get_gtl_score,
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


def test_get_gtl_dist():
    assert _get_gtl_dist("01020301", "01020301") == 1
    assert _get_gtl_dist("01020100", "01020100") == 2
    assert _get_gtl_dist("01020304", "01020300") == 2
    assert _get_gtl_dist("01020100", "01020101") == 2
    assert _get_gtl_dist("01020300", "01020400") == 3
    assert _get_gtl_dist("01020301", "01020401") == 3
    assert _get_gtl_dist("01000000", "01103020") == 4
    assert _get_gtl_dist("01203020", "01103020") == 4
    assert _get_gtl_dist("01000000", "02000000") == np.inf


def test_get_gtl_score():
    assert pytest.approx(get_gtl_score("01020301", "01020301"), 1e-3) == 1.0
    assert pytest.approx(get_gtl_score("01020100", "01020100"), 1e-3) == 1 / 2
    assert pytest.approx(get_gtl_score("01020304", "01020300"), 1e-3) == 1 / 2
    assert pytest.approx(get_gtl_score("01020100", "01020101"), 1e-3) == 1 / 2
    assert pytest.approx(get_gtl_score("01020300", "01020400"), 1e-3) == 1 / 3
    assert pytest.approx(get_gtl_score("01020301", "01020401"), 1e-3) == 1 / 3
    assert pytest.approx(get_gtl_score("01000000", "01103020"), 1e-3) == 1 / 4
    assert pytest.approx(get_gtl_score("01203020", "01103020"), 1e-3) == 1 / 4
    assert pytest.approx(get_gtl_score("01000000", "02000000"), 1e-3) == 0.0


def test_get_gtl_matching_score():
    assert pytest.approx(get_gtl_matching_score("01020301", "01020301"), 1e-3) == 1.0
    assert pytest.approx(get_gtl_matching_score("01020100", "01020100"), 1e-3) == 1.0
    assert pytest.approx(get_gtl_matching_score("01020304", "01020300"), 1e-3) == 3 / 4
    assert pytest.approx(get_gtl_matching_score("01020100", "01020101"), 1e-3) == 1.0
    assert pytest.approx(get_gtl_matching_score("01020300", "01020400"), 1e-3) == 2 / 3
    assert pytest.approx(get_gtl_matching_score("01020301", "01020401"), 1e-3) == 2 / 4
    assert pytest.approx(get_gtl_matching_score("01000000", "01103020"), 1e-3) == 1.0
    assert pytest.approx(get_gtl_matching_score("01203020", "01103020"), 1e-3) == 1 / 4
    assert pytest.approx(get_gtl_matching_score("01000000", "02000000"), 1e-3) == 0.0
