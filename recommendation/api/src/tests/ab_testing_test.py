import os
from unittest.mock import Mock, patch
import pytest
import random
from typing import Any
from pcreco.core.user import User
from pcreco.core.scoring import Scoring
from pcreco.models.reco.recommendation import RecommendationIn
import pandas as pd
from pcreco.utils.env_vars import ENV_SHORT_NAME


class ABtestingTest:
    # test_model_attribution
    # cf. conftest to find ab groups for users in the test db
    @pytest.mark.parametrize(
        ["user_id", "expected_group", "expected_model_name"],
        [
            (
                "111",
                "A",
                f"tf_model_reco_{ENV_SHORT_NAME}",
            ),
            (
                "112",
                "B",
                f"deep_reco_{ENV_SHORT_NAME}",
            ),
            (
                "113",
                "C",
                f"MF_reco_{ENV_SHORT_NAME}",
            ),
        ],
    )
    def test_model_attribution(
        self,
        setup_database: Any,
        user_id,
        expected_group,
        expected_model_name,
    ):
        with patch(
            "pcreco.utils.db.db_connection.__create_db_connection"
        ) as connection_mock:
            connection_mock.return_value = setup_database

            user = User(user_id)
            scoring = Scoring(user)
            assert (
                user.group_id == expected_group
            ), " AB test group attribution is correct"
            assert (
                scoring.model_name == expected_model_name
            ), "Model attribution is correct"
