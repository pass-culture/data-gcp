from flask import Flask, request, Response, jsonify
from flask_cors import CORS
from loguru import logger
from model import PredictPipeline

app = Flask(__name__)
CORS(app)


model = PredictPipeline()


@app.route("/isalive")
def is_alive():
    status_code = Response(status=200)
    return status_code


@app.route("/predict", methods=["POST"])
def predict():
    input_json = request.get_json()["instances"][0]

    try:
        results = model.predict(input_json)
        return jsonify(
            {"predictions": sorted(results, key=lambda x: x["score"], reverse=True)}
        )

    except Exception as e:
        logger.info(e)
        logger.info("Error")

    return jsonify({"predictions": []})


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=8080)
