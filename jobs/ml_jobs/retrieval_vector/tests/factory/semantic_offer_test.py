import pytest

from app.factory.semantic import SemanticHandler
from app.models.prediction_request import PredictionRequest
from app.retrieval.constants import DISTANCE_COLUMN_NAME
from app.retrieval.text_client import TextClient

TRANSFORMER = "hf-internal-testing/tiny-random-camembert"
DETAIL_COLUMNS = ["vector", "booking_number_desc"]


@pytest.fixture
def request_data_text() -> PredictionRequest:
    """Fixture to generate request data with an input text."""
    return PredictionRequest(
        model_type="semantic",
        text="The Best World",
        size=5,
        params={},
        call_id="test-call-id",
        debug=True,
        vector_column_name="vector",
    )


@pytest.mark.parametrize(
    "request_data_fixture",
    ["request_data_text"],
)
def test_semantic_handler(
    request_data_fixture,
    request,
    mock_semantic_connect_db,
    mock_semantic_load_item_document,
    mock_user_document_loading,
):
    """Test SemanticHandler."""

    request_data = request.getfixturevalue(request_data_fixture)

    text_client = TextClient(transformer=TRANSFORMER, detail_columns=DETAIL_COLUMNS)
    text_client.load()

    handler = SemanticHandler()
    result = handler.handle(text_client, request_data)

    # Assertions
    assert len(result.predictions) == request_data.size

    for prediction in result.predictions:
        for column in text_client.detail_columns:
            assert column in prediction

    distances = [prediction[DISTANCE_COLUMN_NAME] for prediction in result.predictions]
    assert distances == sorted(
        distances
    ), f"Predictions are not sorted by {DISTANCE_COLUMN_NAME} in increasing order"
