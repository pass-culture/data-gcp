from unittest.mock import patch
import pytest
from typing import Any
from pcreco.core.user import User
from pcreco.core.recommendation import Recommendation
from pcreco.models.reco.playlist_params import PlaylistParamsIn
from pcreco.utils.env_vars import ENV_SHORT_NAME, AB_TESTING


class ABtestingTest:
    # test_model_attribution
    # cf. conftest to find ab_test groups for users in the test db
    @pytest.mark.skipif(AB_TESTING == False, reason="AB test is currently OFF")
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
        with patch("pcreco.utils.db.db_connection.__get_session") as connection_mock:
            connection_mock.return_value = setup_database
            user = User(user_id)
            input_reco = PlaylistParamsIn()
            scoring = Recommendation(user, input_reco)

            assert (
                user.group_id == expected_group
            ), "AB test group attribution is correct"
            assert (
                scoring.model_name == expected_model_name
            ), "Model attribution is correct"
