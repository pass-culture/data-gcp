import os
from unittest.mock import Mock, patch
import pytest
import random
from numpy.testing import assert_array_equal
from typing import Any

from sqlalchemy import false
from pcreco.core.utils.diversification import (
    order_offers_by_score_and_diversify_categories,
)

ENV_SHORT_NAME = os.getenv("ENV_SHORT_NAME")
ACTIVE_MODEL = os.getenv("ACTIVE_MODEL")
SHUFFLE_RECOMMENDATION = os.getenv("SHUFFLE_RECOMMENDATION", False)

mock_scored_offers = [
    {"id": "item_1", "product_id": "item_1", "subcategory_id": "LIVRE", "score": 1},
    {"id": "item_2", "product_id": "item_2", "subcategory_id": "LIVRE", "score": 2},
    {"id": "item_3", "product_id": "item_3", "subcategory_id": "LIVRE", "score": 3},
    {"id": "item_4", "product_id": "item_4", "subcategory_id": "SPECTACLE", "score": 1},
    {"id": "item_5", "product_id": "item_5", "subcategory_id": "CINEMA", "score": 2},
]
## Reminder on diversification rule
# output list is order by frequency of the category then by score , picking one in each category until reaching NbofRecommendations
mock_expected_output = ["item_3", "item_5", "item_4", "item_2", "item_1"]


class DiversificationTest:
    def test_diversification(
        self,
    ):
        assert_array_equal(
            mock_expected_output,
            order_offers_by_score_and_diversify_categories(mock_scored_offers),
        )
