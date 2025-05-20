import traceback
from abc import ABC, abstractmethod
from typing import Dict, List

from docarray import Document

from app.logging.logger import logger
from app.models.prediction_request import PredictionRequest
from app.models.prediction_result import PredictionResult, SearchType
from app.retrieval.client import DefaultClient


class PredictionHandler(ABC):
    """
    Abstract base class for prediction handlers.
    Each handler must implement the handle method.
    """

    @abstractmethod
    def handle(
        self, model: DefaultClient, request_data: PredictionRequest
    ) -> PredictionResult:
        pass

    def _handle_exception(
        self, e: Exception, call_id: str, selected_params: Dict, size: int
    ) -> PredictionResult:
        """Handle exceptions and log errors."""
        tb = traceback.format_exc()
        logger.error(
            f"error:{self.__class__.__name__}",
            extra={
                "uuid": call_id,
                "params": selected_params,
                "size": size,
                "content": {"error": e.__class__.__name__, "trace": tb},
            },
        )
        return PredictionResult(predictions=[], search_type=SearchType.ERROR)

    def search_by_tops(
        self,
        model: DefaultClient,
        request_data: PredictionRequest,
    ) -> PredictionResult:
        """Search using tops."""
        try:
            results = model.search_by_tops(
                query_filter=request_data.params,
                n=request_data.size,
                details=request_data.debug,
                prefilter=request_data.is_prefilter,
                vector_column_name=request_data.vector_column_name,
                similarity_metric=request_data.similarity_metric,
                re_rank=request_data.re_rank,
                user_id=request_data.user_id,
            )
            return PredictionResult(predictions=results, search_type=SearchType.TOPS)
        except Exception as e:
            return self._handle_exception(
                e, request_data.call_id, request_data.params, request_data.size
            )

    def search_by_vector(
        self,
        model: DefaultClient,
        request_data: PredictionRequest,
        vector: Document,
        excluded_items: List[str] = [],
    ) -> PredictionResult:
        """Search predictions using a vector."""
        try:
            results = model.search_by_vector(
                vector=vector,
                similarity_metric=request_data.similarity_metric,
                n=request_data.size,
                query_filter=request_data.params,
                details=request_data.debug,
                excluded_items=excluded_items,
                prefilter=request_data.prefilter,
                vector_column_name=request_data.vector_column_name,
                re_rank=request_data.re_rank,
                user_id=request_data.user_id,
                item_ids=request_data.items,
            )
            return PredictionResult(predictions=results, search_type=SearchType.VECTOR)
        except Exception as e:
            return self._handle_exception(
                e, request_data.call_id, request_data.params, request_data.size
            )
