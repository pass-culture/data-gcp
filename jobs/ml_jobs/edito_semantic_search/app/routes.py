import time

import pandas as pd
from fastapi import APIRouter
from fastapi.responses import JSONResponse, Response
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

api = APIRouter()

search_client = SearchClient(
    database_uri=DATABASE_URI, vector_table=VECTOR_TABLE, scalar_table=SCALAR_TABLE
)

logger.info("SearchClient initialized successfully.")


def _startup_warmup():
    """One-shot warmup at startup to prime all connections."""
    try:
        logger.info("Running startup warmup...")
        warmup_start = time.time()

        # 1. Warmup GCS/Parquet connection
        _ = search_client.table_query(k=1)
        logger.info(f"GCS warmup: {time.time() - warmup_start:.2f}s")

        # 2. Warmup vector search (LanceDB) + LLM
        if PERFORM_VECTOR_SEARCH:
            vector_start = time.time()
            dummy_query_vector = search_client.embedding_model.embed_query("warmup")
            _ = search_client.vector_search(query_vector=dummy_query_vector, k=1)
            logger.info(f"Vector search warmup: {time.time() - vector_start:.2f}s")

            # NOTE: LLM warmup is skipped intentionally.
            # Using asyncio.run() here would create and close a temporary event loop,
            # causing the GoogleProvider/httpx client to cache a reference to that
            # closed loop. When Uvicorn later serves requests on its own loop,
            # the agent would fail with "Event loop is closed".
            # The agent and model are already initialized at import time.
            logger.info("LLM warmup skipped (agent initialized at import time)")

        logger.info(f"Startup warmup completed in {time.time() - warmup_start:.2f}s")
    except Exception as e:
        logger.warning(f"Startup warmup failed (non-fatal): {e}")


_startup_warmup()


@api.get("/isalive")
def is_alive() -> Response:
    """Health check endpoint."""
    return Response(status_code=200)


@api.post("/predict")
async def predict(data: dict):
    """Predict endpoint."""
    # Warning instances Handling
    try:
        ### Input parsing and validation
        start_time = time.time()
        logger.info(f"Received prediction request: {data}")
        instances = data.get("instances")
        if not instances or not isinstance(instances, list):
            return JSONResponse(
                content={
                    "error": "Invalid request format: 'instances' list is required"
                },
                status_code=400,
            )
        prediction_request = PredictionRequest(**instances[0])
        # Here we decide to either do vector search or LLM filtering
        # When Vector search is disabled, we directly go to scalar search with filters
        item_ids = None
        if PERFORM_VECTOR_SEARCH:
            # Here we embed the query
            query_encoding_time = time.time()
            full_query = (
                f"task: search result | query: {prediction_request.search_query}"
            )
            logger.info(f"Embedding search query for vector search: '{full_query}'")
            query_vector = search_client.embedding_model.embed_query(full_query)
            logger.info(
                f"Query embedding completed "
                f"in {time.time() - query_encoding_time:.2f} seconds"
            )
            vector_search_time = time.time()
            vector_search_results = search_client.vector_search(
                query_vector=query_vector, k=K_RETRIEVAL
            )
            logger.info(
                f"Vector search completed in {time.time() - vector_search_time:.2f} "
                f"seconds with {len(vector_search_results)} results"
            )
            # Here we do the LLM thematic filtering
            llm_time = time.time()
            llm_df = await llm_thematic_filtering(
                search_query=prediction_request.search_query,
                vector_search_results=vector_search_results,
            )
            logger.info(
                f"LLM thematic filtering completed "
                f"in {time.time() - llm_time:.2f} seconds"
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
        # logger.info(f"Performing scalar search with filters: {filters}")
        table_time = time.time()
        scalar_search_results = search_client.table_query(filters=filters, k=MAX_OFFERS)
        logger.info(
            f"Scalar search completed in {time.time() - table_time:.2f} seconds"
        )
        scalar_search_results_df = pd.DataFrame(scalar_search_results)
        logger.info(f"Scalar search returned {len(scalar_search_results_df)} results")

        # Here we match the item_ids from LLM with offers in catalog and apply filters
        if len(scalar_search_results_df) > 0:
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
                    f"Merging LLM results with scalar search completed in "
                    f"{time.time() - merge_time:.2f} seconds with "
                    f"{len(prediction_result_df)} matches"
                )
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
        logger.error(f"Error during prediction: {e}")
        return JSONResponse(
            content={"error": "An error occurred during prediction", "details": str(e)},
            status_code=500,
        )
