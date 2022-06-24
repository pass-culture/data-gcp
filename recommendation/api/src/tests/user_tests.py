import os
from unittest.mock import Mock, patch
import pytest
import random
from typing import Any
from pcreco.core.user import User
from pcreco.core.scoring import Scoring
from pcreco.models.reco.recommendation import RecommendationIn

ENV_SHORT_NAME = os.getenv("ENV_SHORT_NAME")


class UserTest:
    @pytest.mark.parametrize(
        ["user_id", "expected_age", "expected_deposit"],
        [
            ("115", 15, 20),
            ("116", 16, 30),
            ("117", 17, 30),
            ("118", 18, 300)
        ],
    )
    def test_get_user_profile(
        self, setup_database: Any, user_id, expected_age, expected_deposit
    ):
        with patch(
            "pcreco.utils.db.db_connection.__create_db_connection"
        ) as connection_mock:
            connection_mock.return_value = setup_database
            user = User(user_id)
            assert user.age == expected_age, f"age is right"
            assert (
                user.user_deposit_initial_amount == expected_deposit
            ), f"deposit is right"
