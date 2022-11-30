import os
import pytest
from pcreco.models.reco.playlist_params import PlaylistParamsIn
from pcreco.utils.env_vars import (
    NUMBER_OF_RECOMMENDATIONS,
    SHUFFLE_RECOMMENDATION,
    MIXING_RECOMMENDATION,
    MIXING_FEATURE,
    DEFAULT_RECO_RADIUS,
)

ENV_SHORT_NAME = os.getenv("ENV_SHORT_NAME")


class PlaylistParamsTest:
    @pytest.mark.parametrize(
        ["input_params", "has_conditions"],
        [
            ({}, False),
            ({"fake_params": "fake_var"}, False),
            ({"startDate": "2022-10-01"}, True),
            ({"offerIsDuo": True}, True),
        ],
    )
    def test_has_conditions(self, input_params, has_conditions):

        input_params = PlaylistParamsIn(input_params)
        assert input_params.has_conditions == has_conditions

    @pytest.mark.parametrize(
        ["input_params", "is_reco_mixed"],
        [
            ({}, MIXING_RECOMMENDATION),
            ({"isRecoMixed": True}, True),
            ({"isRecoMixed": False}, False),
        ],
    )
    def test_is_reco_mixed(self, input_params, is_reco_mixed):

        input_params = PlaylistParamsIn(input_params)
        assert input_params.is_reco_mixed == is_reco_mixed

    @pytest.mark.parametrize(
        ["input_params", "nb_reco_display"],
        [
            ({}, NUMBER_OF_RECOMMENDATIONS),
            ({"nbRecoDisplay": 100}, 100),
            ({"nbRecoDisplay": NUMBER_OF_RECOMMENDATIONS}, NUMBER_OF_RECOMMENDATIONS),
        ],
    )
    def test_number_of_recommendations(self, input_params, nb_reco_display):

        input_params = PlaylistParamsIn(input_params)
        assert input_params.nb_reco_display == nb_reco_display

    @pytest.mark.parametrize(
        ["input_params", "reco_radius"],
        [
            ({}, DEFAULT_RECO_RADIUS),
            ({"recoRadius": 100}, DEFAULT_RECO_RADIUS),
            ({"recoRadius": 10_000}, ["0_25KM"]),
            ({"recoRadius": 100_000}, ["0_25KM", "25_50KM", "50_100KM"]),
        ],
    )
    def test_reco_radius(self, input_params, reco_radius):

        input_params = PlaylistParamsIn(input_params)
        assert input_params.reco_radius == reco_radius
