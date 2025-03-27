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
        results = model.predict(input_json)
        results_diverisified = [
            result
            for result in results
            if result["offer_id"]
            in DiversificationPipeline(
                [item["offer_semantic_embeding"] for item in input_json],
                [item["score"] for item in results],
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
