from typing import Dict

from app.factory.handler import PredictionHandler
from app.logger import logger
from app.models import PredictionRequest
from app.retrieval.client import DefaultClient
from app.services import search_by_tops


class SearchByTopsHandler(PredictionHandler):
    """
    Handler for filter predictions.
    """

    def handle(self, model: DefaultClient, request_data: PredictionRequest) -> Dict:
        logger.debug(
            "filter",
            extra={
                "uuid": request_data.call_id,
                "params": request_data.params,
                "size": request_data.size,
            },
        )
        return search_by_tops(
            model=model,
            selected_params=request_data.params,
            size=request_data.size,
            debug=request_data.debug,
            call_id=request_data.call_id,
            prefilter=request_data.is_prefilter,
            vector_column_name=request_data.vector_column_name,
            re_rank=request_data.re_rank,
            user_id=request_data.user_id,
        )
