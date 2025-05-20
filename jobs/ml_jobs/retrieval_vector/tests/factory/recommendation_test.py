import pytest

from app.factory.recommendation import RecommendationHandler
from app.models.prediction_request import PredictionRequest
from app.models.prediction_result import SearchType


@pytest.fixture
def request_data_single_item() -> PredictionRequest:
    """Fixture to generate request data with a single item."""
    return PredictionRequest(
        model_type="recommendation",
        size=5,
        params={},
        call_id="test-call-id",
        debug=True,
        is_prefilter=False,
        vector_column_name="vector",
        similarity_metric="dot",
        re_rank=False,
        user_id="user_1",
    )


@pytest.mark.parametrize(
    "request_data_fixture",
    ["request_data_single_item"],
)
def test_recommendation_handler(
    mock_connect_db,
    mock_user_document_loading,
    mock_generate_fake_load_item_document,
    request_data_fixture,
    request,
    reco_client,
):
    """Test RecommendationHandler."""

    # Get the specific request_data fixture dynamically
    request_data = request.getfixturevalue(request_data_fixture)

    # Initialize the handler
    handler = RecommendationHandler()

    # Call the handler
    result = handler.handle(reco_client, request_data)

    # Assertions
    assert len(result.predictions) == request_data.size

    # Assert that the expected detail columns are present in the predictions
    for prediction in result.predictions:
        for column in reco_client.detail_columns:
            assert column in prediction

    # Ensure the predictions are sorted by _distance in increasing order
    distances = [prediction["_distance"] for prediction in result.predictions]
    assert distances == sorted(
        distances
    ), "Predictions are not sorted by _distance in increasing order"
    assert result.search_type == SearchType.VECTOR


def test_recommendation_fallback_handler(
    mock_connect_db,
    mock_user_document_loading,
    mock_generate_fake_load_item_document,
    request,
    reco_client,
):
    """Test RecommendationHandler for fallback scenario."""

    # Get the specific request_data fixture dynamically
    request_data = PredictionRequest(
        model_type="recommendation",
        size=5,
        params={},
        call_id="test-call-id",
        debug=True,
        is_prefilter=False,
        vector_column_name="vector",
        similarity_metric="cosine",
        re_rank=True,
        user_id="unknown_user_1",
    )

    # Initialize the handler
    handler = RecommendationHandler()

    # Call the handler
    result = handler.handle(reco_client, request_data)

    # Assertions
    assert len(result.predictions) == request_data.size

    # Assert that the expected detail columns are present in the predictions
    for prediction in result.predictions:
        for column in reco_client.detail_columns:
            assert column in prediction

    # Ensure the predictions are sorted by _distance in increasing order
    distances = [prediction["_distance"] for prediction in result.predictions]
    assert distances == sorted(
        distances
    ), "Predictions are not sorted by _distance in increasing order"
    assert result.search_type == SearchType.TOPS
