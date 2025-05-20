from unittest.mock import Mock

import pytest

from app.factory.handler import PredictionHandler
from app.models.prediction_request import PredictionRequest
from app.models.prediction_result import PredictionResult, SearchType
from app.retrieval.client import DefaultClient


class TestPredictionHandler(PredictionHandler):
    """
    Concrete class implementing the abstract PredictionHandler for testing.
    This class mocks the behavior of making a prediction.
    """

    def handle(
        self, model: DefaultClient, request_data: PredictionRequest
    ) -> PredictionResult:
        """
        A mock implementation that returns a dummy prediction.

        Args:
            model (DefaultClient): Mocked model used for predictions.
            request_data (PredictionRequest): Mocked request data.

        Returns:
            PredictionResult: A mock prediction result.
        """
        return PredictionResult(
            predictions=[{"item_id": "test"}], search_type=SearchType.VECTOR
        )


@pytest.fixture
def mock_default_client() -> Mock:
    """Fixture for mocking DefaultClient."""
    return Mock(spec=DefaultClient)


@pytest.fixture
def mock_prediction_request() -> Mock:
    """Fixture for mocking PredictionRequest."""
    return Mock(spec=PredictionRequest)


@pytest.fixture
def test_handler() -> TestPredictionHandler:
    """Fixture for creating a test instance of the PredictionHandler."""
    return TestPredictionHandler()


def test_handle_returns_correct_prediction(
    test_handler: TestPredictionHandler,
    mock_default_client: Mock,
    mock_prediction_request: Mock,
) -> None:
    """
    Test that the handle method returns the expected prediction.

    Args:
        test_handler (TestPredictionHandler): The handler to test.
        mock_default_client (Mock): Mocked DefaultClient instance.
        mock_prediction_request (Mock): Mocked PredictionRequest instance.
    """
    result = test_handler.handle(mock_default_client, mock_prediction_request)
    assert result == PredictionResult(
        predictions=[{"item_id": "test"}], search_type=SearchType.VECTOR
    )
