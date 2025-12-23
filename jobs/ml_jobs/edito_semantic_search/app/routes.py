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
    PERFORM_VECTOR_SEARCH,
)
from app.llm.llm_tools import llm_thematic_filtering
from app.models.predictions import PredictionRequest, PredictionResult
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

        # Vector search and LLM filtering (if enabled)

        llm_df = pd.DataFrame()
        item_ids = []
        if PERFORM_VECTOR_SEARCH:
            try:
                start_time = time.time()
                query_vector = search_client.embedding_model.embed_query(
                    prediction_request.search_query
                )
                logger.info(f"Query vector generated in {time.time() - start_time} s")
            except Exception as e:
                logger.error(f"Error during embedding model query: {e}")
                return jsonify({"error": "Embedding model error", "details": str(e)}), 500

            try:
                start_time = time.time()
                vector_search_results = search_client.vector_search(
                    query_vector=query_vector, k=K_RETRIEVAL, filters=None
                )
                logger.info(f"Vector search perform in {time.time() - start_time} s")
                logger.info(f"Vector search retrieved {len(vector_search_results)} results.")
            except Exception as e:
                logger.error(f"Error during vector search: {e}")
                return jsonify({"error": "Vector search error", "details": str(e)}), 500

            try:
                llm_df = llm_thematic_filtering(
                    search_query=prediction_request.search_query,
                    vector_search_results=vector_search_results,
                )
                item_ids = llm_df["id"].tolist()
                logger.info(f"LLM output DataFrame: {llm_df.head()}")
            except Exception as e:
                logger.error(f"Error during LLM thematic filtering: {e}")
                return jsonify({"error": "LLM thematic filtering error", "details": str(e)}), 500
        else:
            logger.info("Skipping vector search as PERFORM_VECTOR_SEARCH is False")

        filters = prediction_request.filters_list or None
        if item_ids!=[]:
            if filters is None:
                filters = []
            logger.info(f"Filtered item IDs for scalar search: {item_ids[:5]}")
            filters.append({"column": "item_id", "operator": "in", "value": item_ids})
            logger.info(f"Filters applied for scalar search: {filters}")

        logger.info(f"Performing scalar search with filters: {filters}")
        try:
            start_time = time.time()
            scalar_search_results = search_client.scalar_search(
                filters=filters, k=MAX_OFFERS
            )
            scalar_search_results_df = pd.DataFrame(scalar_search_results)
            scalar_search_results_df['offer_creation_date'] = scalar_search_results_df['offer_creation_date'].apply(
                lambda x: x.timetuple() if pd.notna(x) else None
                )
            scalar_search_results_df['stock_beginning_date'] = scalar_search_results_df['stock_beginning_date'].apply(
                lambda x: x.timetuple() if pd.notna(x) else None
                )
            logger.info(f"Scalar search perform in {time.time() - start_time} s")
            logger.info(f"Scalar search retrieved {len(scalar_search_results_df)} results.")
            logger.info(f"Scalar search results preview:\n{scalar_search_results_df.head(2)}")
        except Exception as e:
            logger.error(f"Error during scalar search: {e}")
            return jsonify({"error": "Scalar search error", "details": str(e)}), 500

        # Handle empty scalar search results
        if scalar_search_results_df.empty:
            logger.warning("No offers found in scalar search")
            if llm_df.empty:
                return jsonify(
                    {"error": "No offers found", "details": "No offers found in scalar search and LLM output is empty"}
                ), 400
            prediction_result_df = llm_df.rename(columns={"id": "offer_id"})
        else:
            missing_ids = set(item_ids) - set(scalar_search_results_df["item_id"])
            extra_ids = set(scalar_search_results_df["item_id"]) - set(item_ids)
            logger.info(f"Item IDs missing from scalar search results: {missing_ids}")
            logger.info(f"Extra item IDs in scalar search results: {extra_ids}")
            if llm_df.empty:
                logger.info("LLM output is empty, using scalar search results only")
                prediction_result_df = scalar_search_results_df
                logger.info("Assigning default rank of 1 to all scalar search results")
                prediction_result_df["rank"] = [1] * len(prediction_result_df)
            else:
                logger.info("Merging LLM output with scalar search results")
                prediction_result_df = pd.merge(
                    llm_df,
                    scalar_search_results_df,
                    left_on="id",
                    right_on="item_id",
                    how="inner",
                )
                logger.info(f"Merged results preview:\n{prediction_result_df.head(2)}")
            logger.info("checking offer_id assignment")
            if "offer_id" in prediction_result_df.columns:
                prediction_result_df["offer_id"] = prediction_result_df["offer_id"].fillna(prediction_result_df["item_id"])
                logger.info("offer_id assignment completed")

        # Post-processing: Panachage sorting
        #fix NaTType does not support timetuple
        logger.info("Starting post-processing with panachage_sort")
        sorted_results = panachage_sort(prediction_result_df)
        sorted_results_parsed = PredictionResult(
            offers=sorted_results.to_dict(orient="records")
        )
        logger.info("Sorting results using panachage_sort")
        return jsonify(sorted_results_parsed.dict()), 200

    except ValidationError as ve:
        return jsonify({"error": "Invalid input", "details": ve.errors()}), 400
    except Exception as e:
        return jsonify(
            {"error": "An error occurred during prediction", "details": str(e)}
        ), 500
