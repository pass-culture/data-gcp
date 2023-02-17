import collections
import time
from typing import Any, Dict, List, Tuple

import numpy as np
import random
from pcreco.utils.env_vars import (
    NUMBER_OF_RECOMMENDATIONS,
    log_duration,
)


def order_offers_by_score_and_diversify_features(
    offers: List[Dict[str, Any]],
    shuffle_recommendation=None,
    feature="subcategory_id",
    nb_reco_display=NUMBER_OF_RECOMMENDATIONS,
) -> List[int]:
    """
    Group offers by feature.
    Order offer groups by decreasing number of offers in each group and decreasing maximal score.
    Order each offers within a group by increasing score.
    Sort offers by taking the last offer of each group (maximum score), by decreasing size of group.
    Return only the ids of these sorted offers.
    """

    if shuffle_recommendation:
        for recommendation in offers:
            recommendation["score"] = random.random()

    offers_by_feature = _get_offers_grouped_by_feature(
        offers, feature
    )  # here we group offers by cat (and score)
    offers_by_feature_length = np.sum([len(l) for l in offers_by_feature.values()])

    offers_by_feature_ordered_by_frequency = collections.OrderedDict(
        sorted(
            offers_by_feature.items(),
            key=_get_number_of_offers_and_max_score_by_feature,
            reverse=True,
        )
    )
    for offer_feature in offers_by_feature_ordered_by_frequency:
        offers_by_feature_ordered_by_frequency[offer_feature] = sorted(
            offers_by_feature_ordered_by_frequency[offer_feature],
            key=lambda k: k["score"],
            reverse=False,
        )

    diversified_offers = []
    while len(diversified_offers) != offers_by_feature_length:
        # here we pop one offer of eachsubcat
        for offer_feature in offers_by_feature_ordered_by_frequency.keys():
            if offers_by_feature_ordered_by_frequency[offer_feature]:
                diversified_offers.append(
                    offers_by_feature_ordered_by_frequency[offer_feature].pop()
                )
        if len(diversified_offers) >= nb_reco_display:
            break

    return diversified_offers


def _get_offers_grouped_by_feature(
    offers: List[Dict[str, Any]], feature="subcategory_id"
) -> Dict:
    start = time.time()
    offers_by_feature = dict()
    product_ids = set()
    for offer in offers:
        offer_feature = offer[f"{feature}"]
        offer_product_id = offer["item_id"]
        if offer_feature in offers_by_feature.keys():  ## Here we filter subcat
            if offer_product_id not in product_ids:
                offers_by_feature[offer_feature].append(offer)
                product_ids.add(offer_product_id)
        else:
            offers_by_feature[offer_feature] = [offer]

    log_duration(f"_get_offers_grouped_by_{feature}", start)
    return offers_by_feature


def _get_number_of_offers_and_max_score_by_feature(
    feature_and_offers: Tuple,
) -> Tuple:
    return (
        len(feature_and_offers[1]),
        max([offer["score"] for offer in feature_and_offers[1]]),
    )
