import threading
import time
from datetime import datetime

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

# Warmup state tracking
_warmup_state = {
    "last_warmup": None,
    "warmup_count": 0,
    "is_warm": False,
    "background_thread": None,
}
WARMUP_INTERVAL_MINUTES = 10  # Keep connections warm every 10 minutes


@api.route("/isalive", methods=["GET"])
def is_alive() -> Response:
    """Health check endpoint."""
    return Response(status=200)


def _perform_warmup():
    """Internal warmup logic that can be called from endpoint or background thread."""
    try:
        logger.info("Starting warmup cycle...")
        warmup_start = time.time()

        # 1. Warmup GCS/Parquet connection
        logger.info("Warming up GCS connection...")
        gcs_start = time.time()
        _ = search_client.table_query(k=1)
        gcs_time = time.time() - gcs_start
        logger.info(f"✓ GCS warmup: {gcs_time:.2f}s")

        # 2. Warmup vector search (LanceDB)
        if PERFORM_VECTOR_SEARCH:
            logger.info("Warming up vector search...")
            vector_start = time.time()
            dummy_query_vector = search_client.embedding_model.embed_query("warmup")
            _ = search_client.vector_search(query_vector=dummy_query_vector, k=1)
            vector_time = time.time() - vector_start
            logger.info(f"✓ Vector search warmup: {vector_time:.2f}s")

            # 3. Warmup LLM connection
            logger.info("Warming up LLM connection...")
            llm_start = time.time()
            dummy_results = [
                {
                    "id": "1",
                    "offer_name": "Warmup",
                    "offer_description": "Test",
                    "offer_subcategory_id": "TEST",
                }
            ]
            _ = llm_thematic_filtering("warmup test", dummy_results)
            llm_time = time.time() - llm_start
            logger.info(f"✓ LLM warmup: {llm_time:.2f}s")

        total_time = time.time() - warmup_start

        # Update warmup state
        _warmup_state["last_warmup"] = datetime.now()
        _warmup_state["warmup_count"] += 1
        _warmup_state["is_warm"] = True

        logger.info(
            f"✅ Warmup cycle #{_warmup_state['warmup_count']} completed in {total_time:.2f}s"
        )
        return True, total_time

    except Exception as e:
        logger.error(f"Warmup cycle failed: {e}")
        _warmup_state["is_warm"] = False
        return False, 0


def _background_warmup_loop():
    """Background thread that periodically warms up connections."""
    logger.info(
        f"Background warmup thread started (interval: {WARMUP_INTERVAL_MINUTES}min)"
    )

    while True:
        try:
            time.sleep(WARMUP_INTERVAL_MINUTES * 60)
            logger.info("Running scheduled warmup...")
            _perform_warmup()
        except Exception as e:
            logger.error(f"Background warmup error: {e}")


@api.route("/warmup", methods=["GET", "POST"])
def warmup() -> Response:
    """
    Warmup endpoint to initialize all connections and caches.
    Automatically starts background warmup thread on first call.
    """
    try:
        success, duration = _perform_warmup()

        # Start background warmup thread if not already running
        if _warmup_state["background_thread"] is None:
            logger.info("Starting background warmup thread...")
            thread = threading.Thread(target=_background_warmup_loop, daemon=True)
            thread.start()
            _warmup_state["background_thread"] = thread
            logger.info("Background warmup thread started")

        if success:
            return jsonify(
                {
                    "status": "warm",
                    "duration_seconds": round(duration, 2),
                    "warmup_count": _warmup_state["warmup_count"],
                    "last_warmup": _warmup_state["last_warmup"].isoformat(),
                    "next_warmup_in_minutes": WARMUP_INTERVAL_MINUTES,
                }
            ), 200
        else:
            return jsonify({"status": "error", "message": "Warmup failed"}), 500

    except Exception as e:
        logger.error(f"Warmup endpoint error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@api.route("/warmup/status", methods=["GET"])
def warmup_status() -> Response:
    """Check current warmup status."""
    if _warmup_state["last_warmup"]:
        time_since_warmup = (
            datetime.now() - _warmup_state["last_warmup"]
        ).total_seconds() / 60
        is_stale = time_since_warmup > WARMUP_INTERVAL_MINUTES * 1.5
    else:
        time_since_warmup = None
        is_stale = True

    return jsonify(
        {
            "is_warm": _warmup_state["is_warm"] and not is_stale,
            "warmup_count": _warmup_state["warmup_count"],
            "last_warmup": _warmup_state["last_warmup"].isoformat()
            if _warmup_state["last_warmup"]
            else None,
            "minutes_since_last_warmup": round(time_since_warmup, 1)
            if time_since_warmup
            else None,
            "background_thread_active": _warmup_state["background_thread"] is not None
            and _warmup_state["background_thread"].is_alive(),
        }
    ), 200


@api.route("/predict", methods=["POST"])
def predict():
    """Predict endpoint."""
    # Warning instances Handling
    try:
        ### Input parsing and validation
        data = request.get_json()
        start_time = time.time()
        logger.info(f"Received prediction request: {data}")
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
            llm_time = time.time()
            llm_df = llm_thematic_filtering(
                search_query=prediction_request.search_query,
                vector_search_results=vector_search_results,
            )
            logger.info(
                f"LLM thematic filtering completed in {time.time() - llm_time:.2f} seconds"
            )
            item_ids = llm_df["item_id"].tolist()

        # Here we handle filters to perform scalar search
        filters = prediction_request.filters_list or []
        if item_ids:
            filters.append({"column": "item_id", "operator": "in", "value": item_ids})
        # Here we do the scalar search
        table_time = time.time()
        scalar_search_results = search_client.table_query(filters=filters, k=MAX_OFFERS)
        logger.info(
            f"Scalar search completed in {time.time() - table_time:.2f} seconds"
        )
        scalar_search_results_df = pd.DataFrame(scalar_search_results)

        # Here we match the item_ids from LLM with offers in catalog and apply filters
        if not scalar_search_results_df.empty:
            if PERFORM_VECTOR_SEARCH:
                merge_time = time.time()
                prediction_result_df = pd.merge(
                    llm_df,
                    scalar_search_results_df,
                    on="item_id",
                    how="inner",
                )
                prediction_result_df = prediction_result_df[
                    ["offer_id", "pertinence", "rank"]
                ]
                logger.info(
                    f"Merging LLM results with scalar search completed in {time.time() - merge_time:.2f} seconds"
                )
            else:
                prediction_result_df = scalar_search_results_df
                prediction_result_df["rank"] = [1] * len(prediction_result_df)
                prediction_result_df["pertinence"] = "pas de pertinence (dev)"
            # Post-processing: Panachage sorting
            sorted_results = panachage_sort(prediction_result_df)
            search_results = SearchResult(
                offers=sorted_results.to_dict(orient="records")
            )
            logger.info(
                f"Prediction completed in {time.time() - start_time:.2f} seconds with {len(search_results.offers)} offers returned"
            )
            return jsonify(PredictionResult(predictions=search_results).dict()), 200
        else:
            logger.warning("No offers found in scalar search")
            search_results = SearchResult(offers=[])
            return (jsonify(PredictionResult(predictions=search_results).dict()), 200)
    except Exception as e:
        logger.error(f"Error during prediction: {e}")
        return jsonify(
            {"error": "An error occurred during prediction", "details": str(e)}
        ), 500
