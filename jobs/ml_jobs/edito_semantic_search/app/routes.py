import pandas as pd
from flask import Blueprint, Response, jsonify, request
from loguru import logger

from app.constants import (
    DATABASE_URI,
    K_RETRIEVAL,
    MAX_OFFERS,
    PERFORM_VECTOR_SEARCH,
    SCALAR_TABLE,
    VECTOR_TABLE,
)
from app.llm.llm_tools import llm_thematic_filtering
from app.models.validators import (
    PredictionRequest,
    PredictionResult,
    SearchResult,
)
from app.post_process.panachage import panachage_sort
from app.search.client import SearchClient

api = Blueprint("api", __name__)

search_client = SearchClient(
    database_uri=DATABASE_URI, vector_table=VECTOR_TABLE, scalar_table=SCALAR_TABLE
)

logger.info("SearchClient initialized successfully.")


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
            raise ValueError("Invalid JSON body")
        instances = data.get("instances")
        if not instances or not isinstance(instances, list):
            return jsonify(
                {"error": "Invalid request format: 'instances' list is required"}
            ), 400
        prediction_request = PredictionRequest(**instances[0])

        # Here we decide to either do vector search or LLM filtering
        # When Vector search is disabled, we directly go to scalar search with filters
        item_ids = None
        if PERFORM_VECTOR_SEARCH:
            # Here we embed the query
            query_vector = search_client.embedding_model.embed_query(
                prediction_request.search_query
            )
            vector_search_results = search_client.vector_search(
                query_vector=query_vector, k=K_RETRIEVAL
            )
            # Here we do the LLM thematic filtering
            llm_df = llm_thematic_filtering(
                search_query=prediction_request.search_query,
                vector_search_results=vector_search_results,
            )
            item_ids = llm_df["item_id"].tolist()

        # Here we handle filters to perform scalar search
        filters = prediction_request.filters_list or []
        if item_ids:
            filters.append({"column": "item_id", "operator": "in", "value": item_ids})
        # Here we do the scalar search
        scalar_search_results = search_client.table_query(filters=filters, k=MAX_OFFERS)
        scalar_search_results_df = pd.DataFrame(scalar_search_results)

        # Here we match the item_ids from LLM with offers in catalog and apply filters
        if not scalar_search_results_df.empty:
            if PERFORM_VECTOR_SEARCH:
                prediction_result_df = pd.merge(
                    llm_df,
                    scalar_search_results_df,
                    on="item_id",
                    how="inner",
                )
                prediction_result_df = prediction_result_df[
                    ["offer_id", "pertinence", "rank"]
                ]
            else:
                prediction_result_df = scalar_search_results_df
                prediction_result_df["rank"] = [1] * len(prediction_result_df)
                prediction_result_df["pertinence"] = "pas de pertinence (dev)"
            # Post-processing: Panachage sorting
            sorted_results = panachage_sort(prediction_result_df)
            search_results = SearchResult(
                offers=sorted_results.to_dict(orient="records")
            )
            # Output parsing
            search_result_parsed = PredictionResult(predictions=search_results)
            return jsonify(search_result_parsed.dict()), 200
        else:
            logger.warning("No offers found in scalar search")
            raise ValueError("No offers found matching the criteria")
    except Exception as e:
        return jsonify(
            {"error": "An error occurred during prediction", "details": str(e)}
        ), 500
