from flask import Blueprint, Response, jsonify, request
from pydantic import ValidationError

from app.factory.handler_factory import PredictionHandlerFactory
from app.logger import logger
from app.models import PredictionRequest
from app.retrieval.model_config import load_model

api = Blueprint("api", __name__)

model = load_model()
model.load()


@api.route("/isalive", methods=["GET"])
def is_alive() -> Response:
    """Health check endpoint."""
    return Response(status=200)


@api.route("/predict", methods=["POST"])
def predict():
    """Handle prediction requests using a factory pattern."""
    try:
        # Parse the incoming request using Pydantic
        input_json = request.get_json().get("instances", [{}])[0]
        request_data = PredictionRequest.model_validate(input_json)
    except ValidationError as e:
        logger.error(str(e))
        return jsonify({"error": str(e.errors())}), 400
    except Exception as e:
        logger.exception(e)
        return jsonify({"predictions": []}), 400

    try:
        # Get the appropriate handler based on model_type
        handler = PredictionHandlerFactory.get_handler(request_data.model_type)
        result = handler.handle(model, request_data)
        return jsonify(result)
    except ValueError as e:
        logger.error(str(e))
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.exception(e)
        return jsonify({"predictions": []}), 500
