import pytest
from typing import Any
from unittest.mock import patch
from pcreco.core.user import User
from pcreco.core.model_selection.model_configuration import ModelFork
from pcreco.core.model_selection import recommendation_endpoints


@pytest.mark.parametrize(
    ["user_id", "expected_status"],
    [
        ("111", "algo"),
        ("112", "cold_start"),
        ("113", "cold_start"),
        ("000", "unknown"),
    ],
)
def test_get_cold_start_status(
    setup_database: Any,
    user_id: str,
    expected_status: bool,
):
    with patch("pcreco.utils.db.db_connection.__get_session") as connection_mock:
        # Given
        connection_mock.return_value = setup_database
        user = User(user_id)
        _, model_status = ModelFork(
            cold_start_model=recommendation_endpoints.retrieval_reco,
            warm_start_model=recommendation_endpoints.retrieval_reco,
        ).get_user_status(user)
        assert model_status == expected_status
