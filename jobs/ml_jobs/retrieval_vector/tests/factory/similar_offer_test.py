import pytest

from app.factory.similar_offer import SimilarOfferHandler
from app.models.prediction_request import PredictionRequest
from app.models.prediction_result import SearchType
from app.retrieval.constants import DISTANCE_COLUMN_NAME, SEARCH_TYPE_COLUMN_NAME


@pytest.fixture
def request_data_single_item() -> PredictionRequest:
    """Fixture to generate request data with a single item."""
    return PredictionRequest(
        model_type="similar_offer",
        offer_id="item_1",
        size=5,
        params={},
        call_id="test-call-id",
        debug=True,
        is_prefilter=False,
        vector_column_name="vector",
        re_rank=True,
        user_id="user_1",
    )


@pytest.fixture
def request_data_single_item_rerank() -> PredictionRequest:
    """Fixture to generate request data with a single item."""
    return PredictionRequest(
        model_type="similar_offer",
        items=["item_1"],
        size=5,
        params={},
        call_id="test-call-id",
        debug=True,
        is_prefilter=False,
        vector_column_name="vector",
        re_rank=True,
        user_id="xxx",
    )


@pytest.fixture
def request_data_multiple_items() -> PredictionRequest:
    """Fixture to generate request data with multiple items."""
    return PredictionRequest(
        model_type="similar_offer",
        items=["item_1", "item_2", "item_10"],
        size=10,
        params={},
        call_id="test-call-id-multiple",
        debug=True,
        is_prefilter=False,
        vector_column_name="vector",
        re_rank=False,
        user_id="user_1",
    )


@pytest.mark.parametrize(
    "request_data_fixture",
    [
        "request_data_single_item",
        "request_data_multiple_items",
        "request_data_single_item_rerank",
    ],
)
def test_similar_offer_handler(
    mock_connect_db,
    mock_user_document_loading,
    mock_generate_fake_load_item_document,
    request_data_fixture,
    request,
    reco_client,
):
    """Test SimilarOfferHandler with different request_data scenarios."""

    # Get the specific request_data fixture dynamically
    request_data: PredictionRequest = request.getfixturevalue(request_data_fixture)

    # Initialize the handler
    handler = SimilarOfferHandler()

    # Call the handler
    result = handler.handle(reco_client, request_data)

    # Assertions
    assert len(result.predictions) == request_data.size

    # Ensure no items in request_data.items are present in the predictions
    for prediction in result.predictions:
        assert prediction["item_id"] not in request_data.items

    # Assert that the expected detail columns are present in the predictions
    for prediction in result.predictions:
        for column in reco_client.detail_columns:
            assert column in prediction

    # Ensure the predictions are sorted by _distance in increasing order
    distances = [prediction[DISTANCE_COLUMN_NAME] for prediction in result.predictions]
    assert distances == sorted(
        distances
    ), f"Predictions are not sorted by {DISTANCE_COLUMN_NAME} in increasing order"

    # Ensure we are using fallback search type
    for prediction in result.predictions:
        if len(request_data.items) == 1:
            assert prediction[SEARCH_TYPE_COLUMN_NAME] == SearchType.VECTOR
        else:
            assert prediction[SEARCH_TYPE_COLUMN_NAME] == SearchType.AGGREGATED_VECTORS


def test_similar_offer_fallback_handler(
    mock_connect_db,
    mock_user_document_loading,
    mock_generate_fake_load_item_document,
    request,
    reco_client,
):
    """Test SimilarOfferHandler for fallback scenario."""

    # Get the specific request_data fixture dynamically
    request_data = PredictionRequest(
        model_type="similar_offer",
        items=["unknown_item_x"],
        size=5,
        params={},
        call_id="test-call-id",
        debug=True,
        is_prefilter=False,
        vector_column_name="vector",
        re_rank=True,
        user_id="unknown_user_1",
    )

    # Initialize the handler
    handler = SimilarOfferHandler()

    # Call the handler
    result = handler.handle(reco_client, request_data)

    # Assertions
    assert len(result.predictions) == request_data.size

    # Assert that the expected detail columns are present in the predictions
    for prediction in result.predictions:
        for column in reco_client.detail_columns:
            assert column in prediction

    # Ensure the predictions are sorted by DISTANCE_COLUMN_NAME in increasing order
    distances = [prediction[DISTANCE_COLUMN_NAME] for prediction in result.predictions]
    assert distances == sorted(
        distances
    ), f"Predictions are not sorted by {DISTANCE_COLUMN_NAME} in increasing order"

    # Ensure we are using fallback search type
    for prediction in result.predictions:
        assert prediction[SEARCH_TYPE_COLUMN_NAME] == SearchType.TOPS
