from custom_logging import logger
from flask import Flask, Response, jsonify, request
from flask_cors import CORS
from model import DiversificationPipeline, PredictPipeline

app = Flask(__name__)
CORS(app)

model = PredictPipeline()


@app.route("/isalive")
def is_alive():
    status_code = Response(status=200)
    return status_code


@app.route("/predict", methods=["POST"])
def predict():
    input_json = None
    results = None
    try:
        input_json = request.get_json()["instances"]
        print(f"input_json: {input_json}")
        results = model.predict(input_json)
        ##HERE add offer_embedding to results
        # for result in results:
        #     result["offer_embedding"] = offer_embeddings.get(result["offer_id"], [0.0])
        # Create a mapping from offer_id to its embedding to ensure correct alignment
        offer_embeddings = {
            item["offer_id"]: item.get(
                "offer_semantic_embedding", [0.0]
            )  # Fixed typo and added default
            for item in input_json
            if "offer_id" in item
        }

        # Add embeddings to each result
        # for result in results:
        #     result["offer_embedding"] = offer_embeddings.get(result["offer_id"], [0.0])

        # Use embeddings aligned with the results
        aligned_embeddings = [
            offer_embeddings.get(result["offer_id"], [0.0])  # Default if missing
            for result in results
        ]

        results_diverisified = [
            result
            for result in results
            if result["offer_id"]
            in DiversificationPipeline(
                item_semantic_embeddings=aligned_embeddings,
                ids=[item["offer_id"] for item in results],
                scores=[item["score"] for item in results],
            ).get_sampled_ids()
        ]

        return jsonify({"predictions": results_diverisified}), 200

    except Exception as e:
        log_data = {"event": "predict", "result": results, "request": request.json()}

        log_data = {"event": "error", "exception": str(e)}
        logger.error(log_data)
        logger.exception(e)

        return jsonify({"predictions": []}), 500


if __name__ == "__main__":
    app.run()
