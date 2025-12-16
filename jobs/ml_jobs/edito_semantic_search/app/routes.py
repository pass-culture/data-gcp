from flask import Blueprint, Response, jsonify, request
from pydantic import ValidationError
from app.models.predictions import PredictionRequest, PredictionResult
from app.search.client import SearchClient
from app.constants import DATABASE_URI, VECTOR_TABLE, SCALAR_TABLE,K_RETRIEVAL, MAX_OFFERS
from app.llm.llm_tools import LLMOutput ,build_prompt, get_llm_agent
from app.post_process.panachage import panachage_sort
from loguru import logger
import pandas as pd
import time
api = Blueprint("api", __name__)

search_client = SearchClient(
    database_uri=DATABASE_URI,
    vector_table=VECTOR_TABLE,
    scalar_table=SCALAR_TABLE
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
        query_vector = search_client.embedding_model.embed_query(prediction_request.search_query)
        logger.info(f'Query vector generated in {time.time() - start_time} s')
        start_time = time.time()
        vector_search_results = search_client.vector_search(
            query_vector=query_vector,
            k=K_RETRIEVAL,
            filters=None
        )
        logger.info(f"Vector search perform in {time.time() - start_time} s")
        logger.info(f"Vector search retrieved {len(vector_search_results)} results.")

        ### LLM item thematic filtering
        prompt = build_prompt(prediction_request.search_query, vector_search_results, custom_prompt=None)
        logger.info(
            f"Prompt length: {len(prompt)} | retrieved={len(vector_search_results)} | k={K_RETRIEVAL}"
        )
        start_time = time.time()
        llm_result = get_llm_agent().run_sync(prompt)
        llm_output = llm_result.output
        logger.info(f"LLM call perform in {time.time() - start_time} s")
        # logger.info(f"LLM output: {llm_output}")
        offers = getattr(llm_output, "offers", None) or []
        # logger.info(f"Offers extracted: {offers}")
        if offers:
            llm_df = pd.DataFrame(
            [offer.dict() if hasattr(offer, "dict") else offer for offer in offers]
            )
            logger.info(f"LLM offers DataFrame len: {len(llm_df)}")
            llm_df["rank"] = range(1, len(llm_df) + 1)
            logger.info(f"LLM output DataFrame: {llm_df.head()}")
            item_ids =llm_df["id"].tolist()
        else:
            return jsonify({"error": "Invalid input", "details": "No offers found in LLM output"}), 400

        ### Scalar search to retrieve full offer details

        filters=prediction_request.filters_list or []
        #Here keep only id for products 
        item_ids = [item_id for item_id in item_ids if not item_id.startswith("offer-")]
        if item_ids != []:
            logger.info(f"Filtered item IDs for scalar search: {item_ids[:5]}")
            filters.append({
                "column": "item_id",
                "operator": "in",
                "value": item_ids
            })
            logger.info(f"Filters applied for scalar search: {filters}")
            start_time = time.time()
            scalar_search_results = search_client.scalar_search(
                filters=filters,k=MAX_OFFERS
            )
            scalar_search_results_df = pd.DataFrame(scalar_search_results)
            logger.info(f"Scalar search perform in {time.time() - start_time} s")
            if not scalar_search_results_df.empty:
                # return jsonify({"error": "No offers found in scalar search"}), 400
                
                logger.info(f"Scalar search results DataFrame head: {scalar_search_results_df.head()}")
                # Check if scalar_search_results_df['item_id'] matches item_ids
                missing_ids = set(item_ids) - set(scalar_search_results_df["item_id"])
                extra_ids = set(scalar_search_results_df["item_id"]) - set(item_ids)
                logger.info(f"Item IDs missing from scalar search results: {missing_ids}")
                logger.info(f"Extra item IDs in scalar search results: {extra_ids}")
                logger.info(f"Scalar search retrieved {len(scalar_search_results_df)} items.")
                logger.info(f"Prediction results: {scalar_search_results_df.head()}")  
                # Structure the prediction result
                prediction_result_df=pd.merge(llm_df,scalar_search_results_df,left_on="id",right_on="item_id",how="left")
                if "offer_id" in prediction_result_df.columns:
                    prediction_result_df["offer_id"] = prediction_result_df["offer_id"].fillna(prediction_result_df["id"])
                prediction_result = PredictionResult(offers=prediction_result_df.to_dict(orient="records"))
                logger.info(f"PredictionResult DataFrame: {prediction_result_df.head()}")
                # logger.info(f"Structured PredictionResult: {len(prediction_result_df)}")
                # logger.info(f"Structured PredictionResult: {prediction_result}")
        else: 
            logger.warning("No offers found in scalar search")
            prediction_result_df=llm_df
            prediction_result_df.rename(columns={"id":"offer_id"},inplace=True)
            prediction_result = PredictionResult(offers=prediction_result_df.to_dict(orient="records"))
            # logger.info(f"PredictionResult DataFrame without scalar details: {prediction_result_df.head()}")
            # logger.info(f"Structured PredictionResult without scalar details: {len(prediction_result_df)}")
        ### Post-processing: Panachage sorting
        logger.info("Sorting results using panachage_sort")
        sorted_results = panachage_sort(prediction_result_df)
        sorted_results_parsed = PredictionResult(offers=sorted_results.to_dict(orient="records"))
        # logger.info(f"Final sorted PredictionResult DataFrame: {sorted_results.head()}")
        # logger.info(f"Final sorted PredictionResult DataFrame: {sorted_results.tail()}")
        return jsonify(sorted_results_parsed.dict()), 200

    except ValidationError as ve:
        return jsonify({"error": "Invalid input", "details": ve.errors()}), 400
    except Exception as e:
        return jsonify({"error": "An error occurred during prediction", "details": str(e)}), 500
