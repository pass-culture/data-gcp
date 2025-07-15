from flask import Blueprint, Response, jsonify, request
from pydantic import ValidationError

from app.factory.handler_factory import PredictionHandlerFactory
from app.logging.logger import logger
from app.models.prediction_request import PredictionRequest
from app.models.prediction_result import PredictionResult
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
        input_json = request.get_json().get("instances", [{}])[0]
        request_data = PredictionRequest.model_validate(input_json)
    except ValidationError as e:
        # wrong input validation
        logger.error(str(e))
        return jsonify({"error": str(e.errors())}), 403
    except Exception as e:
        # wrong expected input format (e.g. missing instances key)
        logger.error(e)
        return jsonify({"error": str(e)}), 400

    try:
        handler = PredictionHandlerFactory.get_handler(request_data.model_type)
        result: PredictionResult = handler.handle(model, request_data)
        return jsonify(result), 200
    except ValueError as e:
        # wrong logic in the request (e.g. missing user_id for recommendation or items for similar_offer)
        logger.error(str(e))
        return jsonify({"error": str(e)}), 403
    except Exception as e:
        # other errors
        logger.exception(e)
        return jsonify({"error": str(e.errors())}), 500
