from flask import Flask, request, jsonify
from flask_cors import CORS
from loguru import logger
from model import RecoClient, SemanticClient
import json

app = Flask(__name__)
CORS(app)

MODEL_TYPE = {
    "recommendation": RecoClient,
    "semantic": SemanticClient,
}


def load_model() -> RecoClient:
    with open("./metadata/model_type.json", "r") as file:
        desc = json.load(file)
        return MODEL_TYPE[desc["type"]](
            type=desc["type"],
            metric=desc["metric"],
            default_token=desc["default_token"],
            n_dim=desc["n_dim"],
            ascending=desc["ascending"],
        )


model = load_model()


def input_size(size):
    try:
        return int(size)
    except:
        return 10


@app.route("/predict", methods=["POST"])
def predict():
    input_json = request.get_json()["instances"][0]
    debug = bool(input_json.get("debug", 0))
    selected_params = input_json.get("params", {})
    size = input_size(input_json.get("size", 500))

    try:
        if model.type == "recommendation":
            input = str(input_json["user_id"])
        elif model.type == "semantic":
            input = str(input_json["text"])
    except Exception as e:
        logger.info(e)
        logger.info("Wrong model")
        return jsonify({"predictions": []})

    try:
        vector = model.vector(input)
        results = model.search(
            vector=vector, n=size, query_filter=selected_params, details=debug
        )
        return jsonify({"predictions": results})
    except Exception as e:
        logger.info(e)
        logger.info("error")
        return jsonify({"predictions": []})


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=8080)
