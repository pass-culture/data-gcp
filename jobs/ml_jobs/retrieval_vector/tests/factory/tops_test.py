import pytest

from app.factory.tops import SearchByTopsHandler
from app.models.prediction_request import PredictionRequest


@pytest.fixture
def request_data_default() -> PredictionRequest:
    """Fixture to generate request data with a single item."""
    return PredictionRequest(
        model_type="tops",
        size=5,
        params={},
        call_id="test-call-id",
        similarity_metric="dot",
        debug=True,
        vector_column_name="booking_number_desc",
        re_rank=False,
    )


@pytest.fixture
def request_data_rerank() -> PredictionRequest:
    """Fixture to generate request data with a single item."""
    return PredictionRequest(
        model_type="tops",
        size=5,
        params={},
        call_id="test-call-id",
        debug=True,
        vector_column_name="booking_number_desc",
        similarity_metric="l2",
        re_rank=True,
        user_id="user_1",
    )


@pytest.mark.parametrize(
    "request_data_fixture",
    ["request_data_default", "request_data_rerank"],
)
def test_similar_offer_handler(
    mock_connect_db,
    mock_user_document_loading,
    mock_generate_fake_load_item_document,
    request_data_fixture,
    request,
    reco_client,
):
    """Test SearchByTopsHandler."""

    request_data = request.getfixturevalue(request_data_fixture)

    handler = SearchByTopsHandler()

    result = handler.handle(reco_client, request_data)

    # Assertions
    assert len(result.predictions) == request_data.size

    for prediction in result.predictions:
        for column in reco_client.detail_columns:
            assert column in prediction

    distances = [prediction["_distance"] for prediction in result.predictions]
    assert distances == sorted(
        distances
    ), "Predictions are not sorted by _distance in increasing order"
