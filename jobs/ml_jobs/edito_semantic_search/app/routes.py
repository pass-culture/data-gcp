import time

import pandas as pd
from flask import Blueprint, Response, jsonify, request
from loguru import logger
from pydantic import ValidationError

from app.constants import (
    DATABASE_URI,
    K_RETRIEVAL,
    MAX_OFFERS,
    SCALAR_TABLE,
    VECTOR_TABLE,
)
from app.llm.llm_tools import llm_thematic_filtering
from app.models.predictions import PredictionRequest, PredictionResult
from app.post_process.panachage import panachage_sort
from app.search.client import SearchClient

api = Blueprint("api", __name__)

search_client = SearchClient(
    database_uri=DATABASE_URI, vector_table=VECTOR_TABLE, scalar_table=SCALAR_TABLE
)


@api.route("/isalive", methods=["GET"])
def is_alive() -> Response:
    """Health check endpoint."""
    return Response(status=200)


@api.route("/predict", methods=["POST"])
def predict():
    """Predict endpoint."""
    # Warning instances Handling
    try:
        ### Input parsing and validation
        data = request.get_json()
        if not data:
            return jsonify({"error": "Invalid JSON body"}), 400
        logger.info(f"Received prediction request: {data}")
        instances = data.get("instances")
        logger.info(f"Instances: {instances}")
        if not instances or not isinstance(instances, list):
            return jsonify(
                {
                    "error": "Invalid request format: 'instances' list is required and cannot be empty"
                }
            ), 400
        logger.info(f"Processing {len(instances)} instances.")
        input_json = instances[0]
        prediction_request = PredictionRequest(**input_json)
        logger.info(f"Parsed PredictionRequest: {prediction_request}")

        ### Vector search
        start_time = time.time()
        query_vector = search_client.embedding_model.embed_query(
            prediction_request.search_query
        )
        logger.info(f"Query vector generated in {time.time() - start_time} s")
        start_time = time.time()
        vector_search_results = search_client.vector_search(
            query_vector=query_vector, k=K_RETRIEVAL, filters=None
        )
        logger.info(f"Vector search perform in {time.time() - start_time} s")
        logger.info(f"Vector search retrieved {len(vector_search_results)} results.")

        ### LLM item thematic filtering
        llm_df = llm_thematic_filtering(
            search_query=prediction_request.search_query,
            vector_search_results=vector_search_results,
        )
        logger.info(f"LLM output DataFrame: {llm_df.head()}")
        if llm_df.empty:
            return jsonify({"error": "No offers found after LLM filtering"}), 400
        item_ids = llm_df["id"].tolist()
        ### Scalar search to retrieve full offer details
        filters = prediction_request.filters_list or []
        # Here keep only id for products if embedding metadata is complete
        # Until then keep all ids to enriched the data on venue etc..
        # item_ids = [item_id for item_id in item_ids if not item_id.startswith("offer-")]
        if item_ids != []:
            logger.info(f"Filtered item IDs for scalar search: {item_ids[:5]}")
            filters.append({"column": "item_id", "operator": "in", "value": item_ids})
            logger.info(f"Filters applied for scalar search: {filters}")
            start_time = time.time()
            scalar_search_results = search_client.scalar_search(
                filters=filters, k=MAX_OFFERS
            )
            scalar_search_results_df = pd.DataFrame(scalar_search_results)
            logger.info(f"Scalar search perform in {time.time() - start_time} s")
            if not scalar_search_results_df.empty:
                # return jsonify({"error": "No offers found in scalar search"}), 400

                # logger.info(
                #     f"Scalar search results DataFrame head: {scalar_search_results_df.head()}"
                # )
                # Check if scalar_search_results_df['item_id'] matches item_ids
                missing_ids = set(item_ids) - set(scalar_search_results_df["item_id"])
                extra_ids = set(scalar_search_results_df["item_id"]) - set(item_ids)
                logger.info(
                    f"Item IDs missing from scalar search results: {missing_ids}"
                )
                logger.info(f"Extra item IDs in scalar search results: {extra_ids}")
                # logger.info(
                #     f"Scalar search retrieved {len(scalar_search_results_df)} items."
                # )
                # logger.info(f"Prediction results: {scalar_search_results_df.head()}")
                # Structure the prediction result
                prediction_result_df = pd.merge(
                    llm_df,
                    scalar_search_results_df,
                    left_on="id",
                    right_on="item_id",
                    how="left",
                )
                # Needed until we have full metadata in the embedding table
                if "offer_id" in prediction_result_df.columns:
                    prediction_result_df["offer_id"] = prediction_result_df[
                        "offer_id"
                    ].fillna(prediction_result_df["id"])
        else:
            logger.warning("No offers found in scalar search")
            prediction_result_df = llm_df
            prediction_result_df.rename(columns={"id": "offer_id"}, inplace=True)
        ### Post-processing: Panachage sorting
        logger.info("Sorting results using panachage_sort")
        sorted_results = panachage_sort(prediction_result_df)
        sorted_results_parsed = PredictionResult(
            offers=sorted_results.to_dict(orient="records")
        )
        return jsonify(sorted_results_parsed.dict()), 200

    except ValidationError as ve:
        return jsonify({"error": "Invalid input", "details": ve.errors()}), 400
    except Exception as e:
        return jsonify(
            {"error": "An error occurred during prediction", "details": str(e)}
        ), 500
