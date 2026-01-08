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
        data = request.get_json()
        if not data:
            return jsonify({"error": "Invalid JSON body"}), 400

        instances = data.get("instances")
        if not instances or not isinstance(instances, list):
            return jsonify(
                {
                    "error": "Invalid request format: 'instances' list is required and cannot be empty"
                }
            ), 400

        input_json = instances[0]
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
        handler = PredictionHandlerFactory.get_handler(
            request_type=request_data.model_type,
            embedding_model_type=model.EMBEDDING_MODEL_TYPE,
        )
        result: PredictionResult = handler.handle(model, request_data)
        return jsonify(result), 200
    except ValueError as e:
        # wrong logic in the request (e.g. missing user_id for recommendation or items for similar_offer)
        logger.error(str(e))
        return jsonify({"error": str(e)}), 403
    except Exception as e:
        # other errors
        logger.exception(e)
        return jsonify({"error": str(e)}), 500
