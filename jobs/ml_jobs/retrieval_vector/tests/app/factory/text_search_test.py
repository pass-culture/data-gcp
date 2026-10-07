"""Tests for text_search / semantic_search routing and the TextSearchHandler."""

from unittest.mock import MagicMock

import pytest

from app.factory.handler_factory import PredictionHandlerFactory
from app.factory.similar_offer import SimilarOfferHandler
from app.factory.text_search import TextSearchHandler
from app.models.prediction_request import PredictionRequest
from app.models.prediction_result import SearchType
from app.retrieval.constants import SEARCH_TYPE_COLUMN_NAME, EmbeddingModelTypes


def test_factory_routes_text_search_for_semantic_model():
    handler = PredictionHandlerFactory.get_handler(
        request_type="text_search",
        embedding_model_type=EmbeddingModelTypes.SEMANTIC,
    )
    assert isinstance(handler, TextSearchHandler)


def test_factory_rejects_text_search_for_non_semantic_model():
    with pytest.raises(ValueError):
        PredictionHandlerFactory.get_handler(
            request_type="text_search",
            embedding_model_type=EmbeddingModelTypes.TWO_TOWER,
        )


def test_factory_routes_semantic_search_without_fallback():
    handler = PredictionHandlerFactory.get_handler(
        request_type="semantic_search",
        embedding_model_type=EmbeddingModelTypes.SEMANTIC,
    )
    assert isinstance(handler, SimilarOfferHandler)
    assert handler.fallback_client is None


def test_prediction_request_accepts_text_search():
    request = PredictionRequest.model_validate(
        {"model_type": "text_search", "text": "roman policier", "size": 10}
    )
    assert request.model_type == "text_search"
    assert request.text == "roman policier"


def test_prediction_request_semantic_search_rejects_invalid_vector_column():
    # The semantic table has no `raw_embeddings` column.
    with pytest.raises(ValueError):
        PredictionRequest.model_validate(
            {
                "model_type": "semantic_search",
                "items": ["item-1"],
                "vector_column_name": "raw_embeddings",
            }
        )


def test_text_search_handler_requires_text():
    handler = TextSearchHandler()
    request = PredictionRequest.model_validate({"model_type": "text_search"})
    with pytest.raises(ValueError):
        handler.handle(model=MagicMock(), request_data=request)


def test_text_search_handler_tags_search_type():
    handler = TextSearchHandler()
    model = MagicMock()
    model.search_by_text.return_value = [{"idx": 0, "item_id": "item-1"}]
    request = PredictionRequest.model_validate(
        {"model_type": "text_search", "text": "jazz", "size": 5}
    )

    result = handler.handle(model=model, request_data=request)

    model.search_by_text.assert_called_once()
    assert result.predictions == [
        {"idx": 0, "item_id": "item-1", SEARCH_TYPE_COLUMN_NAME: SearchType.TEXT}
    ]
