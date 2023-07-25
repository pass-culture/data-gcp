import pytest
from typing import Any
from unittest.mock import patch
from pcreco.core.user import User
from pcreco.core.model_selection.model_configuration import ModelFork
from pcreco.core.model_selection import recommendation_endpoints


@pytest.mark.parametrize(
    ["user_id", "expected_status"],
    [
        ("111", "cold_start"),
        ("112", "algo"),
        ("113", "algo"),
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
            cold_start_model=recommendation_endpoints.default,
            warm_start_model=recommendation_endpoints.top_offers,
        ).get_user_status(user)
        assert not model_status == expected_status
