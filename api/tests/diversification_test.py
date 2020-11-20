import pytest
from numpy.testing import assert_array_equal

from recommendation import order_offers_by_score_and_diversify_types


@pytest.mark.parametrize(
    ["offers", "output"],
    [
        (
            [
                {"id": 1, "url": None, "type": "A", "score": 1},
                {"id": 2, "url": None, "type": "A", "score": 1},
                {"id": 3, "url": "https://url.com", "type": "B", "score": 10},
                {"id": 4, "url": None, "type": "B", "score": 10},
            ],
            [2, 3, 4, 1],
        ),
        (
            [
                {"id": 1, "url": None, "type": "A", "score": 1},
                {"id": 2, "url": None, "type": "A", "score": 1},
                {"id": 3, "url": None, "type": "B", "score": 10},
                {"id": 4, "url": None, "type": "B", "score": 10},
            ],
            [4, 2, 3, 1],
        ),
        (
            [
                {"id": 1, "url": None, "type": "A", "score": 1},
                {"id": 2, "url": None, "type": "A", "score": 2},
                {"id": 3, "url": None, "type": "A", "score": 10},
                {"id": 4, "url": None, "type": "A", "score": 11},
            ],
            [4, 3, 2, 1],
        ),
        (
            [
                {"id": 1, "url": None, "type": "A", "score": 1},
                {"id": 2, "url": None, "type": "A", "score": 2},
                {"id": 3, "url": "test", "type": "A", "score": 10},
                {"id": 4, "url": "test", "type": "A", "score": 11},
            ],
            [4, 2, 3, 1],
        ),
    ],
)
def test_diversification(offers, output):
    assert_array_equal(output, order_offers_by_score_and_diversify_types(offers))
