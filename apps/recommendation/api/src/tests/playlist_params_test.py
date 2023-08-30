import os
import pytest
from pcreco.models.reco.playlist_params import PlaylistParamsIn
from pcreco.utils.env_vars import (
    NUMBER_OF_RECOMMENDATIONS,
    MIXING_FEATURE_LIST,
)
from pcreco.models.reco.input_params import KeyValueInput

ENV_SHORT_NAME = os.getenv("ENV_SHORT_NAME")


class PlaylistParamsTest:
    @pytest.mark.parametrize(
        ["input_params", "has_conditions"],
        [
            ({}, False),
            ({"fake_params": "fake_var"}, False),
            ({"startDate": "2022-10-01"}, True),
            ({"isDuo": True}, True),
        ],
    )
    def test_has_conditions(self, input_params, has_conditions):

        input_params = PlaylistParamsIn(input_params)
        assert input_params.has_conditions == has_conditions

    @pytest.mark.parametrize(
        ["input_params", "is_reco_mixed"],
        [
            ({}, None),
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
        ["input_params", "mixing_features"],
        [
            ({}, None),
            ({"mixingFeatures": "other"}, None),
            ({"mixingFeatures": MIXING_FEATURE_LIST[0]}, MIXING_FEATURE_LIST[0]),
            ({"mixingFeatures": MIXING_FEATURE_LIST[1]}, MIXING_FEATURE_LIST[1]),
        ],
    )
    def test_mixing_features(self, input_params, mixing_features):
        input_params = PlaylistParamsIn(input_params)
        assert input_params.mixing_features == mixing_features

    @pytest.mark.parametrize(
        ["input_params", "include_digital"],
        [
            ({}, True),
            ({"isDigital": True}, True),
            ({"isDigital": False}, False),
            ({"isEvent": True}, False),
            ({"isEvent": True, "isDigital": True}, False),
        ],
    )
    def test_include_digital(self, input_params, include_digital):
        input_params = PlaylistParamsIn(input_params)
        assert input_params.include_digital == include_digital

    @pytest.mark.parametrize(
        ["input_params", "price_min", "price_max"],
        [
            ({}, None, None),
            ({"priceMax": 0}, None, 0),
            ({"priceMin": 5.5}, 5.5, None),
            ({"priceMax": 0}, None, 0),
            ({"priceMin": 1.5}, 1.5, None),
        ],
    )
    def test_price(self, input_params, price_min, price_max):
        input_params = PlaylistParamsIn(input_params)
        assert input_params.price_min == price_min
        assert input_params.price_max == price_max

    @pytest.mark.parametrize(
        ["input_params", "start_date", "end_date"],
        [
            ({"startDate": None, "endDate": None}, None, None),
            (
                {"startDate": "2022-01-01", "endDate": "2022-02-01"},
                "2022-01-01T00:00:00",
                "2022-02-01T00:00:00",
            ),
            (
                {"startDate": "2022-01-01 10:00:00", "endDate": None},
                "2022-01-01T10:00:00",
                None,
            ),
            (
                {
                    "beginningDatetime": "2022-01-01T10:00:00",
                    "endingDatetime": "2022-01-01T10:00:00",
                },
                "2022-01-01T10:00:00",
                "2022-01-01T10:00:00",
            ),
            (
                {
                    "startDate": "2022-02-01 10:00:00",
                    "endDate": None,
                    "beginningDatetime": "2022-01-01 10:00:00",
                    "endingDatetime": "2022-03-01 10:00:00",
                },
                "2022-02-01T10:00:00",
                "2022-03-01T10:00:00",
            ),
        ],
    )
    def test_date(self, input_params, start_date, end_date):
        input_params = PlaylistParamsIn(input_params)
        if input_params.end_date is not None:
            assert input_params.end_date.isoformat() == end_date
        else:
            assert input_params.end_date == end_date
        if input_params.start_date is not None:
            assert input_params.start_date.isoformat() == start_date
        else:
            assert input_params.start_date == start_date

    @pytest.mark.parametrize(
        ["input_params", "output_params"],
        [
            (
                {
                    "offerTypeList": [
                        {"key": "MOVIE", "value": "BOOLYWOOD"},
                        {"key": "BOOK", "value": "Art"},
                        {"key": "BOOK", "value": "Droit"},
                        {"key": "SHOW", "value": "Danse"},
                        {"key": "MUSIC", "value": "Blues"},
                    ]
                },
                [
                    KeyValueInput(key="MOVIE", value="BOOLYWOOD"),
                    KeyValueInput(key="BOOK", value="Art"),
                    KeyValueInput(key="BOOK", value="Droit"),
                    KeyValueInput(key="SHOW", value="Danse"),
                    KeyValueInput(key="MUSIC", value="Blues"),
                ],
            ),
            (
                {
                    "offerTypeList": [
                        {"key": "NOT_A_MOVIE", "value": 123},
                        {"not_a_key": "BOOK", "value": "Art"},
                        {"key": "BOOK", "not_a_value": "Droit"},
                    ]
                },
                None,
            ),
            (
                {
                    "offerTypeList": [
                        {"key": "MOVIE", "value": "BOOLYWOOD"},
                        {"not_a_key": "", "not_a_value": ""},
                    ]
                },
                None,
            ),
        ],
    )
    def test_offer_type_list(self, input_params, output_params):
        input_params = PlaylistParamsIn(input_params)
        assert input_params.offer_type_list == output_params
