import pytest
from typing import Any
from unittest.mock import patch
from pcreco.core.user import User
from pcreco.core.model_selection.recommendation import ModelFork


@pytest.mark.parametrize(
    ["user_id", "cold_start_status"],
    [
        ("111", False),
        ("112", True),
        ("113", True),
    ],
)
def test_get_cold_start_status(
    setup_database: Any,
    user_id: str,
    cold_start_status: bool,
):
    with patch("pcreco.utils.db.db_connection.__get_session") as connection_mock:
        # Given
        connection_mock.return_value = setup_database
        user = User(user_id)
        model_status = ModelFork().get_model_status(user)
        assert not model_status == cold_start_status
