import time

import pandas as pd
from fastapi import APIRouter
from fastapi.responses import JSONResponse, Response
from loguru import logger
from pydantic import BaseModel, Field

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
from app.utils.timing import log_timing

api = APIRouter()

search_client = SearchClient(
    database_uri=DATABASE_URI, vector_table=VECTOR_TABLE, scalar_table=SCALAR_TABLE
)

logger.info("SearchClient initialized successfully.")


def _startup_warmup():
    """One-shot warmup at startup to prime all connections."""
    try:
        logger.info("Running startup warmup...")

        # 1. Warmup GCS/Parquet connection
        with log_timing("GCS warmup"):
            _ = search_client.table_query(k=1)

        # 2. Warmup vector search (LanceDB) + LLM
        if PERFORM_VECTOR_SEARCH:
            with log_timing("Vector search warmup"):
                dummy_query_vector = search_client.embedding_model.encode(
                    "warmup", prompt_name="query"
                ).tolist()
                _ = search_client.vector_search(query_vector=dummy_query_vector, k=1)

            # NOTE: LLM warmup is skipped intentionally.
            # Using asyncio.run() here would create and close a temporary event loop,
            # causing the GoogleProvider/httpx client to cache a reference to that
            # closed loop. When Uvicorn later serves requests on its own loop,
            # the agent would fail with "Event loop is closed".
            # The agent and model are already initialized at import time.
            logger.info("LLM warmup skipped (agent initialized at import time)")

        logger.info("Startup warmup done.")
    except Exception as e:
        logger.warning(f"Startup warmup failed (non-fatal): {e}")


_startup_warmup()


@api.get("/isalive")
def is_alive() -> Response:
    """Health check endpoint."""
    return Response(status_code=200)


class VertexAIPredictRequest(BaseModel):
    """Vertex AI online prediction envelope."""

    instances: list[PredictionRequest] = Field(
        ..., min_length=1, description="List of prediction instances."
    )


@api.post("/predict")
async def predict(data: VertexAIPredictRequest):
    """Predict endpoint."""
    try:
        ### Input parsing and validation
        start_time = time.time()
        logger.info(
            f"Received prediction request for query: {data.instances[0].search_query!r}"
        )
        prediction_request = data.instances[0]
        # Here we decide to either do vector search or LLM filtering
        # When Vector search is disabled, we directly go to scalar search with filters
        item_ids = None
        if PERFORM_VECTOR_SEARCH:
            # Here we embed the query
            full_query = (
                f"task: search result | query: {prediction_request.search_query}"
            )
            logger.info(f"Embedding search query for vector search: '{full_query}'")
            with log_timing("Query embedding"):
                query_vector = search_client.embedding_model.encode(
                    full_query, prompt_name="query"
                ).tolist()

            with log_timing("Vector search"):
                vector_search_results = search_client.vector_search(
                    query_vector=query_vector, k=K_RETRIEVAL
                )
            logger.info(f"Vector search returned {len(vector_search_results)} results")

            # Here we do the LLM thematic filtering
            with log_timing("LLM thematic filtering"):
                llm_df = await llm_thematic_filtering(
                    search_query=prediction_request.search_query,
                    vector_search_results=vector_search_results,
                )

            if llm_df.empty:
                logger.warning("LLM thematic filtering returned no results")
                search_results = SearchResult(offers=[])
                logger.info(
                    f"Prediction completed in {time.time() - start_time:.2f} seconds "
                    f"with {len(search_results.offers)} offers returned"
                )
                return PredictionResult(predictions=search_results).model_dump()
            item_ids = llm_df["item_id"].tolist()

        # Here we handle filters to perform scalar search
        filters = prediction_request.filters_list or []
        if item_ids:
            filters.append({"column": "item_id", "operator": "in", "value": item_ids})

        # Derive partition hints from vector results to avoid full GCS scan.
        # We already know which subcategories our selected items belong to.
        # Only apply this optimization when vector search was actually performed
        if PERFORM_VECTOR_SEARCH and item_ids:
            has_user_subcategory_filter = any(
                f["column"] == "offer_subcategory_id" for f in filters
            )
            if not has_user_subcategory_filter:
                subcategory_by_id = {
                    str(r["id"]): r["offer_subcategory_id"]
                    for r in vector_search_results
                    if "offer_subcategory_id" in r
                    and r["offer_subcategory_id"] not in (None, "None")
                }
                selected_subcategories = list(
                    {
                        subcategory_by_id[iid]
                        for iid in item_ids
                        if iid in subcategory_by_id
                    }
                )
                if selected_subcategories:
                    filters.append(
                        {
                            "column": "offer_subcategory_id",
                            "operator": "in",
                            "value": selected_subcategories,
                        }
                    )
                    logger.info(
                        f"Injected partition hint: {len(selected_subcategories)} "
                        "subcategories from vector results"
                    )

        # Here we do the scalar search
        with log_timing("Scalar search"):
            scalar_search_results = search_client.table_query(
                filters=filters, k=MAX_OFFERS
            )
        scalar_search_results_df = pd.DataFrame(scalar_search_results)
        logger.info(f"Scalar search returned {len(scalar_search_results_df)} results")

        # Here we match the item_ids from LLM with offers in catalog and apply filters
        if len(scalar_search_results_df) > 0:
            if PERFORM_VECTOR_SEARCH:
                with log_timing("Merge LLM + scalar results"):
                    prediction_result_df = pd.merge(
                        llm_df,
                        scalar_search_results_df,
                        on="item_id",
                        how="inner",
                    )
                    prediction_result_df = prediction_result_df[
                        ["offer_id", "pertinence", "rank"]
                    ]
                logger.info(f"Merge produced {len(prediction_result_df)} matches")
            else:
                prediction_result_df = scalar_search_results_df
                prediction_result_df["rank"] = [1] * len(prediction_result_df)
                prediction_result_df["pertinence"] = "pas de pertinence (dev)"

            # Check if results remain after merge
            if len(prediction_result_df) == 0:
                logger.warning("No offers remaining after merge")
                search_results = SearchResult(offers=[])
                logger.info(
                    f"Prediction completed in {time.time() - start_time:.2f} seconds "
                    f"with {len(search_results.offers)} offers returned"
                )
                return PredictionResult(predictions=search_results).model_dump()

            # Post-processing: Panachage sorting
            prediction_result_df.drop_duplicates(subset=["offer_id"], inplace=True)
            sorted_results = panachage_sort(prediction_result_df)
            logger.info(
                f"Panachage sorting completed. "
                f"Final result count: {len(sorted_results)}"
            )
            search_results = SearchResult(
                offers=sorted_results.to_dict(orient="records")
            )
            logger.info(
                f"Prediction completed in {time.time() - start_time:.2f} seconds "
                f"with {len(search_results.offers)} offers returned"
            )
            return PredictionResult(predictions=search_results).model_dump()
        else:
            logger.warning("No offers found in scalar search")
            search_results = SearchResult(offers=[])
            logger.info(
                f"Prediction completed in {time.time() - start_time:.2f} seconds "
                f"with {len(search_results.offers)} offers returned"
            )
            return PredictionResult(predictions=search_results).model_dump()
    except Exception as e:
        logger.error(f"Error during prediction: {e}", exc_info=True)
        return JSONResponse(
            content={"error": "An internal error occurred during prediction."},
            status_code=500,
        )
