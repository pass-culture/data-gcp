import collections
import datetime
import random
import time
from typing import Any, Dict, List, Tuple

from sqlalchemy import text
import numpy as np
import pytz

from not_eac.cold_start import (
    get_cold_start_status,
    get_cold_start_categories,
    get_cold_start_scored_recommendations_for_user,
)
from eac.eac_cold_start import (
    get_cold_start_status_eac,
    get_cold_start_categories_eac,
    get_cold_start_scored_recommendations_for_user_eac,
)
from not_eac.ab_testing import query_ab_testing_table, ab_testing_assign_user
from eac.eac_ab_testing import query_ab_testing_table_eac, ab_testing_assign_user_eac
from not_eac.scoring import (
    get_intermediate_recommendations_for_user,
    get_scored_recommendation_for_user,
)
from eac.eac_scoring import (
    get_intermediate_recommendations_for_user_eac,
    get_scored_recommendation_for_user_eac,
)
from geolocalisation import get_iris_from_coordinates
from utils import create_db_connection, log_duration, NUMBER_OF_RECOMMENDATIONS


def get_user_metadata(user_id: int, longitude: int, latitude: int):
    is_eac = is_eac_user(user_id)

    ab_testing = fork_query_ab_testing_table(user_id, is_eac)
    if not ab_testing:
        group_id = fork_ab_testing_assign_user(user_id, is_eac)
    else:
        group_id = ab_testing[0]

    is_cold_start = fork_get_cold_start_status(user_id, is_eac, group_id)
    user_iris_id = get_iris_from_coordinates(longitude, latitude)

    return group_id, is_cold_start, is_eac, user_iris_id


def get_final_recommendations(
    user_id: int, longitude: int, latitude: int, playlist_arg=None
) -> List[int]:

    group_id, is_cold_start, is_eac, user_iris_id = get_user_metadata(
        user_id, longitude, latitude
    )
    if is_cold_start:
        reco_origin = "cold_start"
        cold_start_categories = fork_get_cold_start_categories(user_id, is_eac)
        scored_recommendation_for_user = (
            fork_cold_start_scored_recommendations_for_user(
                user_id,
                user_iris_id,
                cold_start_categories,
                is_eac,
                group_id,
                playlist_arg,
            )
        )
    else:
        reco_origin = "algo"
        recommendations_for_user = fork_intermediate_recommendations_for_user(
            user_id, user_iris_id, is_eac, playlist_arg
        )
        scored_recommendation_for_user = fork_scored_recommendation_for_user(
            user_id, group_id, recommendations_for_user, is_eac
        )
        # Keep the top 40 offers and shuffle them
        scored_recommendation_for_user = sorted(
            scored_recommendation_for_user, key=lambda k: k["score"], reverse=True
        )[:40]
        for recommendation in scored_recommendation_for_user:
            recommendation["score"] = random.random()

    # For playlists we dont need to shuffle the categories
    if playlist_arg:
        final_recommendations = sorted(
            scored_recommendation_for_user, key=lambda k: k["score"], reverse=True
        )[:10]
    else:
        final_recommendations = order_offers_by_score_and_diversify_categories(
            scored_recommendation_for_user
        )

    save_recommendation(user_id, final_recommendations, group_id, reco_origin)
    return final_recommendations, group_id, is_cold_start


def is_eac_user(
    user_id,
):
    start = time.time()

    with create_db_connection() as connection:
        request_response = connection.execute(
            text(
                f"SELECT count(1) > 0 "
                f"FROM public.enriched_user "
                f"WHERE user_id = '{str(user_id)}' "
                f"AND user_deposit_initial_amount < 300 "
                f"AND FLOOR(DATE_PART('DAY',user_deposit_creation_date - user_birth_date)/365) < 18"
            )
        ).scalar()
    print(f"is_eac_user = {request_response}")
    log_duration(f"is_eac_user for {user_id}", start)
    return request_response


def fork_query_ab_testing_table(user_id, is_eac):
    start = time.time()
    print(f"is_eac : {is_eac}")
    if is_eac:
        print("Search in eac table ab_testing")
        request_response = query_ab_testing_table_eac(user_id)
    else:
        print("Search in not eac table ab_testing")
        request_response = query_ab_testing_table(user_id)
    log_duration(f"query_ab_testing_table for {user_id}", start)
    return request_response


def fork_ab_testing_assign_user(user_id, is_eac):
    start = time.time()
    if is_eac:
        group_id = ab_testing_assign_user_eac(user_id)
    else:
        group_id = ab_testing_assign_user(user_id)
    log_duration(f"ab_testing_assign_user for {user_id}", start)
    return group_id


def fork_get_cold_start_status(user_id, is_eac, group_id):
    start = time.time()
    if is_eac:
        user_cold_start_status = get_cold_start_status_eac(user_id, group_id)
    else:
        user_cold_start_status = get_cold_start_status(user_id, group_id)
    log_duration(f"get_cold_start_status for {user_id}", start)
    return user_cold_start_status


def fork_get_cold_start_categories(user_id, is_eac):
    start = time.time()
    if is_eac:
        cold_start_categories = get_cold_start_categories_eac(user_id)
    else:
        cold_start_categories = get_cold_start_categories(user_id)
    log_duration(f"get_cold_start_categories for {user_id}", start)
    return cold_start_categories


def fork_cold_start_scored_recommendations_for_user(
    user_id: int,
    user_iris_id: int,
    cold_start_categories: list,
    is_eac: bool,
    group_id: str,
    playlist_arg=None,
):
    start = time.time()
    if is_eac:
        cold_start_recommendations = get_cold_start_scored_recommendations_for_user_eac(
            user_id, user_iris_id, cold_start_categories, playlist_arg
        )
    else:
        cold_start_recommendations = get_cold_start_scored_recommendations_for_user(
            user_id, user_iris_id, cold_start_categories, group_id, playlist_arg
        )
    log_duration(
        f"get_cold_start_scored_recommendations_for_user for {user_id} {'with localisation' if user_iris_id else ''}",
        start,
    )
    return cold_start_recommendations


def fork_intermediate_recommendations_for_user(
    user_id: int, user_iris_id: int, is_eac: bool, playlist_arg=None
) -> List[Dict[str, Any]]:
    start = time.time()
    if is_eac:
        user_recommendation = get_intermediate_recommendations_for_user_eac(
            user_id, user_iris_id, playlist_arg
        )
    else:
        user_recommendation = get_intermediate_recommendations_for_user(
            user_id, user_iris_id, playlist_arg
        )
    log_duration(
        f"get_intermediate_recommendations_for_user for {user_id} {'with localisation' if user_iris_id else ''}",
        start,
    )
    return user_recommendation


def fork_scored_recommendation_for_user(
    user_id: int,
    group_id: str,
    user_recommendations: List[Dict[str, Any]],
    is_eac: bool,
) -> List[Dict[str, int]]:
    if is_eac:
        recommendations = get_scored_recommendation_for_user_eac(
            user_id, group_id, user_recommendations
        )
    else:
        recommendations = get_scored_recommendation_for_user(
            user_id, group_id, user_recommendations
        )
    return recommendations


def order_offers_by_score_and_diversify_categories(
    offers: List[Dict[str, Any]]
) -> List[int]:
    """
    Group offers by category.
    Order offer groups by decreasing number of offers in each group and decreasing maximal score.
    Order each offers within a group by increasing score.
    Sort offers by taking the last offer of each group (maximum score), by decreasing size of group.
    Return only the ids of these sorted offers.
    """

    start = time.time()
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
        if len(diversified_offers) >= NUMBER_OF_RECOMMENDATIONS:
            break

    ordered_and_diversified_offers = [offer["id"] for offer in diversified_offers][
        :NUMBER_OF_RECOMMENDATIONS
    ]

    log_duration("order_offers_by_score_and_diversify_categories", start)
    return ordered_and_diversified_offers


def _get_offers_grouped_by_category(offers: List[Dict[str, Any]]) -> Dict:
    start = time.time()
    offers_by_category = dict()
    product_ids = set()
    for offer in offers:
        offer_category = offer["category"]
        offer_product_id = offer["product_id"]
        if offer_category in offers_by_category.keys():
            if offer_product_id not in product_ids:
                offers_by_category[offer_category].append(offer)
                product_ids.add(offer_product_id)
        else:
            offers_by_category[offer_category] = [offer]

    log_duration("_get_offers_grouped_by_category", start)
    return offers_by_category


def _get_number_of_offers_and_max_score_by_category(
    category_and_offers: Tuple,
) -> Tuple:
    return (
        len(category_and_offers[1]),
        max([offer["score"] for offer in category_and_offers[1]]),
    )


def save_recommendation(
    user_id: int, recommendations: List[int], group_id: str, reco_origin: str
):
    start = time.time()
    date = datetime.datetime.now(pytz.utc)
    rows = []
    for offer_id in recommendations:
        rows.append(
            {
                "user_id": user_id,
                "offer_id": offer_id,
                "date": date,
                "group_id": group_id,
                "reco_origin": reco_origin,
            }
        )

    with create_db_connection() as connection:
        connection.execute(
            text(
                """
                INSERT INTO public.past_recommended_offers (userid, offerid, date, group_id, reco_origin)
                VALUES (:user_id, :offer_id, :date, :group_id, :reco_origin)
                """
            ),
            rows,
        )
    log_duration(f"save_recommendations for {user_id}", start)
