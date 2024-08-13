import collections
import random
from typing import Any, Dict, List, Tuple, Union

import numpy as np
import pandas as pd

from utils.constants import NUMBER_OF_PRESELECTED_OFFERS, RECOMMENDATION_NUMBER


def order_offers_by_score_and_diversify_categories(
    offers: Union[List[Dict[str, Any]], pd.DataFrame],
    shuffle_recommendation: bool,
) -> List[int]:
    """
    Group offers by category.
    Order offer groups by decreasing number of offers in each group and decreasing maximal score.
    Order each offers within a group by increasing score.
    Sort offers by taking the last offer of each group (maximum score), by decreasing size of group.
    Return only the ids of these sorted offers.
    """

    offers = offers.to_dict("records")
    if shuffle_recommendation:
        # select TOP NUMBER_OF_PRESELECTED_OFFERS to shuffle from
        offers = offers[:NUMBER_OF_PRESELECTED_OFFERS]
        for recommendation in offers:
            recommendation["score"] = random.random()

    offers_by_category = _get_offers_grouped_by_category(offers)

    offers_by_category_ordered_by_frequency = collections.OrderedDict(
        sorted(
            offers_by_category.items(),
            key=_get_number_of_offers_and_max_score_by_category,
            reverse=True,
        )
    )
    for offer_category in offers_by_category_ordered_by_frequency:
        offers_by_category_ordered_by_frequency[offer_category] = sorted(
            offers_by_category_ordered_by_frequency[offer_category],
            key=lambda k: k["score"],
            reverse=False,
        )

    diversified_offers = []
    while len(diversified_offers) != np.sum(
        [len(l) for l in offers_by_category.values()]
    ):
        for offer_category in offers_by_category_ordered_by_frequency.keys():
            if offers_by_category_ordered_by_frequency[offer_category]:
                diversified_offers.append(
                    offers_by_category_ordered_by_frequency[offer_category].pop()
                )
        if len(diversified_offers) >= RECOMMENDATION_NUMBER:
            break

    ordered_and_diversified_offers = [offer["item_id"] for offer in diversified_offers][
        :RECOMMENDATION_NUMBER
    ]

    return ordered_and_diversified_offers


def _get_offers_grouped_by_category(offers: List[Dict[str, Any]]) -> Dict:
    offers_by_category = dict()
    product_ids = set()
    for offer in offers:
        offer_category = offer["offer_subcategoryid"]
        offer_product_id = offer["item_id"]
        if offer_category in offers_by_category.keys():
            if offer_product_id not in product_ids:
                offers_by_category[offer_category].append(offer)
                product_ids.add(offer_product_id)
        else:
            offers_by_category[offer_category] = [offer]

    return offers_by_category


def _get_number_of_offers_and_max_score_by_category(
    category_and_offers: Tuple,
) -> Tuple:
    return (
        len(category_and_offers[1]),
        max([offer["score"] for offer in category_and_offers[1]]),
    )
