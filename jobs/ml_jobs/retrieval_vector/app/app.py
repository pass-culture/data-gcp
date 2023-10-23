from flask import Flask, request, Response, jsonify
from flask_cors import CORS
from pythonjsonlogger import jsonlogger
import logging
import sys
from model import DefaultClient, RecoClient, TextClient
from docarray import Document
import json
import uuid


def load_model() -> DefaultClient:
    with open("./metadata/model_type.json", "r") as file:
        desc = json.load(file)
        if desc["type"] == "recommendation":
            return RecoClient(
                default_token=desc["default_token"],
            )
        if desc["type"] == "semantic":
            return TextClient(
                transformer=desc["transformer"],
            )
        else:
            raise Exception("Model desc not found.")


model = load_model()
model.load()


logger = logging.getLogger(__name__)
stdout = logging.StreamHandler(stream=sys.stdout)
fmt = jsonlogger.JsonFormatter(
    "%(name)s %(asctime)s %(levelname)s %(filename)s %(lineno)s %(process)d %(message)s",
    rename_fields={"levelname": "severity", "asctime": "timestamp"},
)

stdout.setFormatter(fmt)
logger.addHandler(stdout)
logger.setLevel(logging.INFO)

app = Flask(__name__)
CORS(app)

log_data = {"event": "startup", "response": "ready"}
logger.info("startup", extra=log_data)


def input_size(size):
    try:
        return int(size)
    except:
        return 10


def filter(selected_params, size: int, debug: bool, call_id, prefilter: bool):
    try:
        results = model.filter(
            selected_params, details=debug, n=size, prefilter=prefilter
        )
        return jsonify({"predictions": results})
    except Exception as e:
        logger.exception(e)
        logger.error(
            "error",
            extra={
                "uuid": call_id,
                "params": selected_params,
                "size": size,
            },
        )
        return jsonify({"predictions": []})


def search_vector(
    vector: Document,
    size: int,
    selected_params,
    debug,
    call_id,
    prefilter: bool,
    item_id=None,
):
    try:
        if vector is not None:
            results = model.search(
                vector=vector,
                n=size,
                query_filter=selected_params,
                details=debug,
                item_id=item_id,
                prefilter=prefilter,
            )
            return jsonify({"predictions": results})
        else:
            logger.info("item not found")
            return jsonify({"predictions": []})
    except Exception as e:
        logger.exception(e)
        logger.error(
            "error",
            extra={
                "uuid": call_id,
                "params": selected_params,
                "size": size,
            },
        )
        return jsonify({"predictions": []})


@app.route("/isalive")
def is_alive():
    status_code = Response(status=200)
    return status_code


@app.route("/predict", methods=["POST"])
def predict():
    call_id = input_json.get("call_id", str(uuid.uuid4()))
    input_json = request.get_json()["instances"][0]
    model_type = input_json["model_type"]
    debug = bool(input_json.get("debug", 0))
    prefilter = bool(input_json.get("prefilter", 1))
    selected_params = input_json.get("params", {})
    size = input_size(input_json.get("size", 500))

    try:
        if isinstance(model, RecoClient):
            if model_type == "recommendation":
                input_str = str(input_json["user_id"])
                logger.info(
                    f"recommendation",
                    extra={
                        "uuid": call_id,
                        "user_id": input_str,
                        "params": selected_params,
                        "size": size,
                    },
                )
                vector = model.user_vector(input_str)
                return search_vector(
                    vector,
                    size,
                    selected_params,
                    debug,
                    call_id=call_id,
                    prefilter=prefilter,
                )
        if isinstance(model, TextClient):
            if model_type == "semantic":
                input_str = str(input_json["text"])
                logger.info(
                    f"semantic",
                    extra={
                        "uuid": call_id,
                        "text": input_str,
                        "params": selected_params,
                        "size": size,
                    },
                )
                vector = model.text_vector(input_str)
                return search_vector(
                    vector,
                    size,
                    selected_params,
                    debug,
                    call_id=call_id,
                    prefilter=prefilter,
                )

        if model_type == "similar_offer":
            input_str = str(input_json["offer_id"])
            logger.info(
                f"similar_offer",
                extra={
                    "uuid": call_id,
                    "item_id": input_str,
                    "params": selected_params,
                    "size": size,
                },
            )
            vector = model.offer_vector(input_str)
            return search_vector(
                vector,
                size,
                selected_params,
                debug,
                item_id=input_str,
                call_id=call_id,
                prefilter=prefilter,
            )

        if model_type == "filter":
            logger.info(
                f"filter",
                extra={
                    "uuid": call_id,
                    "params": selected_params,
                    "size": size,
                },
            )
            return filter(
                selected_params, size, debug, call_id=call_id, prefilter=prefilter
            )

    except Exception as e:
        logger.exception(e)

    return jsonify({"predictions": []})


if __name__ == "__main__":
    app.run()
