import os
from numpy.testing import assert_array_equal
from pcreco.core.utils.mixing import (
    order_offers_by_score_and_diversify_features,
)
from pcreco.core.model.recommendable_offer import RecommendableOffer


ENV_SHORT_NAME = os.getenv("ENV_SHORT_NAME")
SHUFFLE_RECOMMENDATION = os.getenv("SHUFFLE_RECOMMENDATION", False)

mock_scored_offers = [
    RecommendableOffer(
        offer_id="item_1",
        item_id="item_1",
        venue_id=None,
        user_distance=None,
        booking_number=None,
        category=None,
        subcategory_id="LIVRE",
        stock_price=None,
        offer_creation_date=None,
        stock_beginning_date=None,
        search_group_name=None,
        venue_latitude=None,
        venue_longitude=None,
        is_geolocated=0,
        item_score=1,
        item_rank=1,
        query_order=None,
        random=None,
        offer_score=None,
        offer_output=None,
    ),
    RecommendableOffer(
        offer_id="item_2",
        item_id="item_2",
        venue_id=None,
        user_distance=None,
        booking_number=None,
        category=None,
        subcategory_id="LIVRE",
        stock_price=None,
        offer_creation_date=None,
        stock_beginning_date=None,
        search_group_name=None,
        venue_latitude=None,
        venue_longitude=None,
        is_geolocated=0,
        item_score=2,
        item_rank=2,
        query_order=None,
        random=None,
        offer_score=None,
        offer_output=None,
    ),
    RecommendableOffer(
        offer_id="item_3",
        item_id="item_3",
        venue_id=None,
        user_distance=None,
        booking_number=None,
        category=None,
        subcategory_id="LIVRE",
        stock_price=None,
        offer_creation_date=None,
        stock_beginning_date=None,
        search_group_name=None,
        venue_latitude=None,
        venue_longitude=None,
        is_geolocated=0,
        item_score=3,
        item_rank=3,
        query_order=None,
        random=None,
        offer_score=None,
        offer_output=None,
    ),
    RecommendableOffer(
        offer_id="item_4",
        item_id="item_4",
        venue_id=None,
        user_distance=None,
        booking_number=None,
        category=None,
        subcategory_id="SPECTACLE",
        stock_price=None,
        offer_creation_date=None,
        stock_beginning_date=None,
        search_group_name=None,
        venue_latitude=None,
        venue_longitude=None,
        is_geolocated=0,
        item_score=1,
        item_rank=4,
        query_order=None,
        random=None,
        offer_score=None,
        offer_output=None,
    ),
    RecommendableOffer(
        offer_id="item_5",
        item_id="item_5",
        venue_id=None,
        user_distance=None,
        booking_number=None,
        category=None,
        subcategory_id="CINEMA",
        stock_price=None,
        offer_creation_date=None,
        stock_beginning_date=None,
        search_group_name=None,
        venue_latitude=None,
        venue_longitude=None,
        is_geolocated=0,
        item_score=2,
        item_rank=5,
        query_order=None,
        random=None,
        offer_score=None,
        offer_output=None,
    ),
]
## Reminder on diversification rule
# output list is order by top score of the category, picking one in each category until reaching NbofRecommendations
mock_expected_output = ["item_3", "item_5", "item_4", "item_2", "item_1"]


class DiversificationTest:
    def test_diversification(
        self,
    ):

        offers = order_offers_by_score_and_diversify_features(
            mock_scored_offers,
            score_column="item_score",
            score_order_ascending=False,
            shuffle_recommendation=None,
            feature="subcategory_id",
            nb_reco_display=20,
        )
        ids = [x.offer_id for x in offers]
        assert_array_equal(
            mock_expected_output,
            ids,
        )
