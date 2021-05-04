import collections
from typing import List, Dict, Any, Tuple

from sqlalchemy import create_engine, engine

env = "dev"
# psswd = "f7a903089f01829c"
psswd = "b5e07b43bafb0cbd"

MACRO_CATEGORIES_TYPE_MAPPING = {
    "cinema": ["EventType.CINEMA", "ThingType.CINEMA_CARD", "ThingType.CINEMA_ABO"],
    "audiovisuel": ["ThingType.AUDIOVISUEL"],
    "jeux_videos": ["ThingType.JEUX_VIDEO_ABO", "ThingType.JEUX_VIDEO"],
    "livre": ["ThingType.LIVRE_EDITION", "ThingType.LIVRE_AUDIO"],
    "musees_patrimoine": [
        "EventType.MUSEES_PATRIMOINE",
        "ThingType.MUSEES_PATRIMOINE_ABO",
    ],
    "musique": ["EventType.MUSIQUE", "ThingType.MUSIQUE_ABO", "ThingType.MUSIQUE"],
    "pratique_artistique": [
        "EventType.PRATIQUE_ARTISTIQUE",
        "ThingType.PRATIQUE_ARTISTIQUE_ABO",
    ],
    "spectacle_vivant": [
        "EventType.SPECTACLE_VIVANT",
        "ThingType.SPECTACLE_VIVANT_ABO",
    ],
    "instrument": ["ThingType.INSTRUMENT"],
    "presse": ["ThingType.PRESSE_ABO"],
    "autre": ["EventType.CONFERENCE_DEBAT_DEDICACE"],
}

SQL_CONNECTION_NAME = f"passculture-data-ehp:europe-west1:cloudsql-recommendation-{env}"

query_string = dict(
    {"unix_sock": "/cloudsql/{}/.s.PGSQL.5432".format(SQL_CONNECTION_NAME)}
)

engine = create_engine(
    f"postgresql://cloudsql-recommendation-{'stg' if env == 'staging' else env}:{psswd}@localhost:5432/cloudsql-recommendation-{'stg' if env == 'staging' else env}"
)


def create_db_connection():
    return engine.connect().execution_options(autocommit=True)


def get_cold_start_status(user_id: int, connection) -> bool:
    cold_start_query = f"""
        SELECT count(*)
        FROM booking
        WHERE user_id = '{user_id}';
    """
    query_result = connection.execute(cold_start_query).fetchall()

    user_cold_start_status = query_result[0][0] < 3

    return user_cold_start_status


def get_cold_start_types(user_id: int, connection) -> list:
    qpi_answers_categories = [
        "cinema",
        "audiovisuel",
        "jeux_videos",
        "livre",
        "musees_patrimoine",
        "musique",
        "pratique_artistique",
        "spectacle_vivant",
        "instrument",
        "presse",
        "autre",
    ]
    cold_start_query = f"""
        SELECT {', '.join(qpi_answers_categories)}
        FROM qpi_answers
        WHERE user_id = '{user_id}';
    """
    query_result = connection.execute(cold_start_query).fetchall()

    cold_start_types = []
    if len(query_result) == 0:
        return []
    for category_index, category in enumerate(query_result[0]):
        if category:
            cold_start_types.extend(
                MACRO_CATEGORIES_TYPE_MAPPING[qpi_answers_categories[category_index]]
            )

    return cold_start_types


def _get_offers_grouped_by_type_and_onlineless(offers: List[Dict[str, Any]]) -> Dict:
    offers_by_type = dict()
    for offer in offers:
        offer_type_and_onlineness = _get_offer_type_and_onlineness(offer)
        if offer_type_and_onlineness in offers_by_type.keys():
            offers_by_type[offer_type_and_onlineness].append(offer)
        else:
            offers_by_type[offer_type_and_onlineness] = [offer]
    return offers_by_type


def _get_number_of_offers_and_max_score_by_type(type_and_offers: Tuple) -> Tuple:
    return (
        len(type_and_offers[1]),
        max([offer["score"] for offer in type_and_offers[1]]),
    )


def _get_offer_type_and_onlineness(offer: Dict[str, Any]) -> str:
    return (
        str(offer["type"]) + "_DIGITAL"
        if offer["url"]
        else str(offer["type"]) + "_PHYSICAL"
    )


def order_offers_by_score_and_diversify_types(
    offers: List[Dict[str, Any]]
) -> List[int]:
    """
    Group offers by type.
    Order offer groups by decreasing number of offers in each group and decreasing maximal score.
    Order each offers within a group by increasing score.
    Sort offers by taking the last offer of each group (maximum score), by decreasing size of group.
    Return only the ids of these sorted offers.
    """
    offers_by_type = _get_offers_grouped_by_type_and_onlineless(offers)
    print("OFFER BY TYPE")
    print(offers_by_type)

    offers_by_type_ordered_by_frequency = collections.OrderedDict(
        sorted(
            offers_by_type.items(),
            key=_get_number_of_offers_and_max_score_by_type,
            reverse=True,
        )
    )

    for offer_type in offers_by_type_ordered_by_frequency:
        offers_by_type_ordered_by_frequency[offer_type] = sorted(
            offers_by_type_ordered_by_frequency[offer_type],
            key=lambda k: k["score"],
            reverse=False,
        )
        print("HERE")
        print(offer_type)
        print(offers_by_type_ordered_by_frequency[offer_type][:10])

    diversified_offers = []

    while len(diversified_offers) != len(offers):
        for offer_type in offers_by_type_ordered_by_frequency.keys():
            if offers_by_type_ordered_by_frequency[offer_type]:
                diversified_offers.append(
                    offers_by_type_ordered_by_frequency[offer_type].pop()
                )
    print("KO???")
    print([offer["score"] for offer in diversified_offers][:10])
    print([offer["id"] for offer in diversified_offers][:10])
    print()
    return [offer["id"] for offer in diversified_offers]


def get_cold_start_ordered_recommendations(
    recommendations: List[Dict[str, Any]],
    cold_start_types: List[str],
    number_of_recommendations: int,
):
    cold_start_types_recommendation = [
        recommendation
        for recommendation in recommendations
        if recommendation["type"] in cold_start_types
    ]
    other_recommendations = [
        recommendation
        for recommendation in recommendations
        if recommendation["type"] not in cold_start_types
    ]
    if len(cold_start_types_recommendation) >= number_of_recommendations:
        print("ENOUGH")
        print()
        return order_offers_by_score_and_diversify_types(
            cold_start_types_recommendation
        )[:number_of_recommendations]

    missing_recommendations = number_of_recommendations - len(
        cold_start_types_recommendation
    )
    return (
        order_offers_by_score_and_diversify_types(cold_start_types_recommendation)
        + order_offers_by_score_and_diversify_types(other_recommendations)[
            :missing_recommendations
        ]
    )


def get_intermediate_recommendations_for_user(
    user_id: int,
    user_iris_id: int,
    is_cold_start: bool,
    cold_start_types: list,
    connection,
) -> List[Dict[str, Any]]:

    recommendations_query = get_recommendations_query(
        user_id, user_iris_id, is_cold_start, cold_start_types
    )
    query_result = connection.execute(recommendations_query).fetchall()

    user_recommendation = [
        {"id": row[0], "type": row[1], "url": row[2]} for row in query_result
    ]

    return user_recommendation


def get_recommendations_query(
    user_id: int, user_iris_id: int, is_cold_start: bool, cold_start_types: list
) -> str:
    if is_cold_start:
        if cold_start_types:
            order_query = f"""
                ORDER BY 
                    (type in ({', '.join([f"'{offer_type}'" for offer_type in cold_start_types])})) DESC, 
                    booking_number DESC
                 """
        else:
            order_query = "ORDER BY booking_number DESC"
    else:
        order_query = "ORDER BY RANDOM()"

    if not user_iris_id:
        query = f"""
            SELECT offer_id, type, url
            FROM recommendable_offers
            WHERE is_national = True or url IS NOT NULL
            AND offer_id NOT IN
                (
                SELECT offer_id
                FROM non_recommendable_offers
                WHERE CAST(user_id AS BIGINT) = {user_id}
                )
            {order_query};
        """
    else:
        query = f"""
            SELECT offer_id, type, url
            FROM recommendable_offers
            WHERE
                (
                venue_id IN
                    (
                    SELECT "venue_id"
                    FROM iris_venues_mv
                    WHERE CAST("iris_id" AS BIGINT) = {user_iris_id}
                    )
                OR is_national = True
                )
            AND offer_id NOT IN
                (
                SELECT offer_id
                FROM non_recommendable_offers
                WHERE CAST(user_id AS BIGINT) = {user_id}
                )
            {order_query};
        """
    return query


def function(user_id):
    connection = create_db_connection()

    ab_testing_table = "ab_testing_202104_v0_v0bis"
    request_response = connection.execute(
        f"""SELECT groupid FROM {ab_testing_table} WHERE CAST(userid AS BIGINT)={user_id}"""
    ).scalar()

    group_id = request_response[0]

    is_cold_start = get_cold_start_status(user_id, connection)
    user_iris_id = 0

    if group_id == "A" and is_cold_start:
        cold_start_types = get_cold_start_types(user_id, connection)
        recommendations_for_user = get_intermediate_recommendations_for_user(
            user_id, user_iris_id, True, cold_start_types, connection
        )
        scored_recommendation_for_user = [
            {**recommendation, "score": len(recommendations_for_user) - i}
            for i, recommendation in enumerate(recommendations_for_user)
        ]
        final_recommendations = get_cold_start_ordered_recommendations(
            recommendations=scored_recommendation_for_user,
            cold_start_types=cold_start_types,
            number_of_recommendations=10,
        )
    else:
        recommendations_for_user = get_intermediate_recommendations_for_user(
            user_id, user_iris_id, False, [], connection
        )
        scored_recommendation_for_user = [
            {**reco, "score": 1} for reco in recommendations_for_user
        ]
        final_recommendations = order_offers_by_score_and_diversify_types(
            scored_recommendation_for_user
        )[:10]

    return final_recommendations


# A - cold start - musique : ok
# A - cold start - aucun type : ok
# A pas cold start
# B - cold start ok
# B - pas cold start ok
function("2")
