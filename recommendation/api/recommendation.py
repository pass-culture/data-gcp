import os
import collections
from typing import Any, Dict, List, Tuple

from google.api_core.client_options import ClientOptions
from googleapiclient import discovery
import psycopg2

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
SQL_CONNECTION_NAME = os.environ.get("SQL_CONNECTION_NAME")
SQL_BASE = os.environ.get("SQL_BASE")
SQL_BASE_USER = os.environ.get("SQL_BASE_USER")
SQL_BASE_PASSWORD = os.environ.get("SQL_BASE_PASSWORD")
GCP_MODEL_REGION = os.environ.get("GCP_MODEL_REGION")


def get_recommendations_for_user(
    user_id: int, user_iris_id: int, connection=None
) -> List[Dict[str, Any]]:
    if connection is None:
        connection = psycopg2.connect(
            user=SQL_BASE_USER,
            password=SQL_BASE_PASSWORD,
            database=SQL_BASE,
            host=f"/cloudsql/{SQL_CONNECTION_NAME}",
        )

    recommendations_query = get_recommendations_query(user_id, user_iris_id)

    cursor = connection.cursor()
    cursor.execute(recommendations_query)

    user_recommendation = [
        {"id": row[0], "type": row[1], "url": row[2]} for row in cursor.fetchall()
    ]

    cursor.close()
    connection.close()

    return user_recommendation


def get_recommendations_query(user_id: int, user_iris_id: int) -> str:
    if not user_iris_id:
        query = f"""
            SELECT id, type, url
            FROM recommendable_offers
            WHERE is_national = True
            AND id NOT IN
                (
                SELECT offer_id
                FROM non_recommendable_offers
                WHERE user_id = {user_id}
                )
            ORDER BY RANDOM();
        """
    else:
        query = f"""
            SELECT id, type, url
            FROM recommendable_offers
            WHERE
                (
                venue_id IN
                    (
                    SELECT "venueId"
                    FROM iris_venues
                    WHERE "irisId" = {user_iris_id}
                    )
                OR is_national = True
                )
            AND id NOT IN
                (
                SELECT offer_id
                FROM non_recommendable_offers
                WHERE user_id = {user_id}
                )
            ORDER BY RANDOM();
        """
    return query


def get_scored_recommendation_for_user(
    user_recommendations: List[Dict[str, Any]], model_name: str, version: str
) -> List[Dict[str, int]]:
    offers_ids = [recommendation["id"] for recommendation in user_recommendations]
    predicted_scores = predict_score(
        GCP_MODEL_REGION, GCP_PROJECT_ID, model_name, offers_ids, version
    )
    return [
        {
            **recommendation,
            "score": predicted_scores[i],
        }
        for i, recommendation in enumerate(user_recommendations)
    ]


def predict_score(region, project, model, instances, version):
    endpoint = f"https://{region}-ml.googleapis.com"
    client_options = ClientOptions(api_endpoint=endpoint)
    service = discovery.build("ml", "v1", client_options=client_options)
    name = "projects/{}/models/{}".format(project, model)

    if version is not None:
        name += "/versions/{}".format(version)

    response = (
        service.projects().predict(name=name, body={"instances": instances}).execute()
    )

    if "error" in response:
        raise RuntimeError(response["error"])

    return response["predictions"]


def order_offers_by_score_and_diversify_types(
    offers: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Group offers by type.
    Order offer groups by decreasing number of offers in each group and decreasing maximal score.
    Order each offers within a group by increasing score.
    Sort offers by taking the last offer of each group (maximum score), by decreasing size of group.
    Return only the ids of these sorted offers.
    """
    offers_by_type = _get_offers_grouped_by_type_and_onlineless(offers)

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
            key=lambda k: (k["score"], k["id"]),
            reverse=False,
        )

    diversified_offers = []

    while len(diversified_offers) != len(offers):
        for offer_type in offers_by_type_ordered_by_frequency.keys():
            if offers_by_type_ordered_by_frequency[offer_type]:
                diversified_offers.append(
                    offers_by_type_ordered_by_frequency[offer_type].pop()
                )

    return [offer["id"] for offer in diversified_offers]


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
