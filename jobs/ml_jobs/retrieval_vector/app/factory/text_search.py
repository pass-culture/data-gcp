from app.factory.handler import PredictionHandler
from app.logging.logger import logger
from app.models.prediction_request import PredictionRequest
from app.models.prediction_result import PredictionResult, SearchType
from app.retrieval.constants import SEARCH_TYPE_COLUMN_NAME
from app.retrieval.semantic_client import SemanticClient


class TextSearchHandler(PredictionHandler):
    """Handler for keyword full-text search over item textual metadata."""

    def handle(
        self,
        model: SemanticClient,
        request_data: PredictionRequest,
    ) -> PredictionResult:
        """Handle a text_search prediction request.

        Args:
            model (SemanticClient): The semantic client performing the FTS query.
            request_data (PredictionRequest): The request; ``text`` is required.

        Returns:
            PredictionResult: The matched items (ids + optional metadata).
        """
        logger.debug(
            "text_search",
            extra={
                "uuid": request_data.call_id,
                "text": request_data.text,
                "params": request_data.params,
                "size": request_data.size,
            },
        )
        if not request_data.text:
            raise ValueError("text is required for text_search predictions.")

        try:
            results = model.search_by_text(
                text=request_data.text,
                n=request_data.size,
                query_filter=request_data.params,
                details=request_data.debug,
                excluded_items=request_data.excluded_items,
                prefilter=request_data.is_prefilter,
            )
        except Exception as e:
            return self._handle_exception(
                e, request_data.call_id, request_data.params, request_data.size
            )

        return PredictionResult(
            predictions=[
                {**result, SEARCH_TYPE_COLUMN_NAME: SearchType.TEXT}
                for result in results
            ]
        )
