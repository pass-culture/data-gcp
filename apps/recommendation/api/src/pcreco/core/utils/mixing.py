import collections
import time
from typing import Any, Dict, List, Tuple

import numpy as np
import random
from pcreco.utils.env_vars import (
    NUMBER_OF_RECOMMENDATIONS,
)
from pcreco.core.model.recommendable_offer import RecommendableOffer


def order_offers_by_score_and_diversify_features(
    offers: List[RecommendableOffer],
    score_column="item_score",
    score_order_ascending=False,
    shuffle_recommendation=None,
    feature="subcategory_id",
    nb_reco_display=NUMBER_OF_RECOMMENDATIONS,
) -> List[RecommendableOffer]:
    """
    Group offers by feature.
    Order offer groups by decreasing number of offers in each group and decreasing maximal score.
    Order each offers within a group by increasing score.
    Sort offers by taking the last offer of each group (maximum score), by decreasing size of group.
    Return only the ids of these sorted offers.
    score_order_ascending is False, score = the higher the better
    """

    if shuffle_recommendation:
        for recommendation in offers:
            setattr(recommendation, score_column, random.random())

    offers_by_feature = _get_offers_grouped_by_feature(
        offers, feature
    )  # here we group offers by cat (and score)
    offers_by_feature_length = np.sum([len(l) for l in offers_by_feature.values()])

    offers_by_feature_ordered_by_frequency = collections.OrderedDict(
        sorted(
            offers_by_feature.items(),
            key=lambda x: _get_number_of_offers_and_max_score_by_feature(
                x,
                score_column=score_column,
                score_order_ascending=score_order_ascending,
            ),
            reverse=not score_order_ascending,
        )
    )

    for offer_feature in offers_by_feature_ordered_by_frequency:
        offers_by_feature_ordered_by_frequency[offer_feature] = sorted(
            offers_by_feature_ordered_by_frequency[offer_feature],
            key=lambda k: getattr(k, score_column),
            reverse=score_order_ascending,
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
    offers: List[RecommendableOffer], feature="subcategory_id"
) -> Dict:
    offers_by_feature = dict()
    product_ids = set()
    for offer in offers:
        offer_feature = getattr(offer, feature)
        offer_product_id = offer.item_id
        if offer_feature in offers_by_feature.keys():  ## Here we filter subcat
            if offer_product_id not in product_ids:
                offers_by_feature[offer_feature].append(offer)
                product_ids.add(offer_product_id)
        else:
            offers_by_feature[offer_feature] = [offer]
    return offers_by_feature


def _get_number_of_offers_and_max_score_by_feature(
    feature_and_offers: Tuple,
    score_column: str = "score",
    score_order_ascending: bool = False,
) -> Tuple:
    if score_order_ascending:
        sum_score = min(
            [getattr(offer, score_column) for offer in feature_and_offers[1]]
        )
    else:
        sum_score = max(
            [getattr(offer, score_column) for offer in feature_and_offers[1]]
        )
    return sum_score
