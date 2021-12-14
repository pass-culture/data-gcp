import time
from typing import Any, Dict, List

from sqlalchemy import text

from google.api_core.client_options import ClientOptions
from googleapiclient import discovery
from utils import (
    create_db_connection,
    log_duration,
    GCP_PROJECT,
    MODEL_REGION,
    MODEL_NAME_A,
    MODEL_NAME_B,
    MODEL_NAME_C,
)


def get_intermediate_recommendations_for_user_eac(
    user_id: int, user_iris_id: int
) -> List[Dict[str, Any]]:
    if not user_iris_id:
        query = text(
            """
            SELECT offer_id, category, subcategory_id, url, item_id, product_id
            FROM recommendable_offers_eac_16_17
            WHERE is_national = True or url IS NOT NULL
            AND offer_id NOT IN
                (
                SELECT offer_id
                FROM non_recommendable_offers
                WHERE user_id = :user_id
                )
            AND booking_number > 0
            ORDER BY RANDOM();
            """
        )

        with create_db_connection() as connection:
            query_result = connection.execute(query, user_id=str(user_id)).fetchall()

    else:
        query = text(
            f"""
            SELECT offer_id, category, subcategory_id, url, item_id, product_id
            FROM recommendable_offers_eac_16_17
            WHERE
                (
                venue_id IN
                    (
                    SELECT "venue_id"
                    FROM iris_venues_mv
                    WHERE "iris_id" = :user_iris_id
                    )
                OR is_national = True
                OR url IS NOT NULL
                )
            AND offer_id NOT IN
                (
                SELECT offer_id
                FROM non_recommendable_offers
                WHERE user_id = :user_id
                )
            AND booking_number > 0
            ORDER BY RANDOM();
            """
        )

        with create_db_connection() as connection:
            query_result = connection.execute(
                query, user_id=str(user_id), user_iris_id=str(user_iris_id)
            ).fetchall()

    user_recommendation = [
        {
            "id": row[0],
            "category": row[1],
            "subcategory_id": row[2],
            "url": row[3],
            "item_id": row[4],
            "product_id": row[5],
        }
        for row in query_result
    ]

    return user_recommendation


def get_scored_recommendation_for_user_eac(
    user_id: int, group_id: str, user_recommendations: List[Dict[str, Any]]
) -> List[Dict[str, int]]:
    """
    Depending on the user group, prepare the data to send to the model, and make the call.
    """

    start = time.time()
    user_to_rank = [user_id] * len(user_recommendations)

    if group_id == "A":
        # 29/10/2021 : A = Algo v1
        model_name = MODEL_NAME_A
        offers_ids = [
            recommendation["item_id"] if recommendation["item_id"] else ""
            for recommendation in user_recommendations
        ]
        instances = [{"input_1": user_to_rank, "input_2": offers_ids}]
        # Format = dict with 2 inputs: arrays of users and offers

    elif group_id == "B":
        # 29/10/2021 : B = Algo v2 : Deep Reco
        model_name = MODEL_NAME_B
        offers_ids = [
            recommendation["item_id"] if recommendation["item_id"] else ""
            for recommendation in user_recommendations
        ]
        offers_subcategories = [
            recommendation["subcategory_id"] if recommendation["subcategory_id"] else ""
            for recommendation in user_recommendations
        ]

        instances = [
            {
                "input_1": user_to_rank,
                "input_2": offers_ids,
                "input_3": offers_subcategories,
            }
        ]
        # Format = dict with 3 inputs: arrays of users, offers and subcategories

    elif group_id == "C":
        # 29/10/2021 : C = Algo v2 : Matrix Factorization
        model_name = MODEL_NAME_C
        offers_ids = [
            recommendation["item_id"] if recommendation["item_id"] else ""
            for recommendation in user_recommendations
        ]
        instances = [{"input_1": user_to_rank, "input_2": offers_ids}]

    else:
        instances = []

    predicted_scores = predict_score(MODEL_REGION, GCP_PROJECT, model_name, instances)

    recommendations = [
        {**recommendation, "score": predicted_scores[i][0]}
        for i, recommendation in enumerate(user_recommendations)
    ]

    log_duration(
        f"get_scored_recommendation_for_user for {user_id} - {model_name}", start
    )
    return recommendations


def predict_score(region, project, model, instances):
    """
    Calls the AI Platform endpoint for the given model and instances and retrieves the scores.
    """
    start = time.time()
    endpoint = f"https://{region}-ml.googleapis.com"
    client_options = ClientOptions(api_endpoint=endpoint)
    service = discovery.build(
        "ml", "v1", client_options=client_options, cache_discovery=False
    )
    name = f"projects/{project}/models/{model}"

    response = (
        service.projects().predict(name=name, body={"instances": instances}).execute()
    )

    if "error" in response:
        raise RuntimeError(response["error"])

    log_duration(f"predict_score", start)
    return response["predictions"]
