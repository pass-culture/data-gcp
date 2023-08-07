from pcreco.utils.env_vars import (
    NUMBER_OF_RECOMMENDATIONS,
    MIXING_FEATURE_LIST,
)
import pcreco.models.reco.input_params as input_params

INPUT_PARAMS = [
    "modelEndpoint",
    "startDate",
    "endDate",
    "beginningDatetime",
    "endingDatetime",
    "isEvent",
    "isDuo",
    "categories",
    "subcategories",
    "offerTypeList",
    "priceMax",
    "priceMin",
    "nbRecoDisplay",
    "isRecoMixed",
    "isRecoShuffled",
    "isDigital",
    "mixingFeatures",
]


class PlaylistParamsIn:
    def __init__(self, json={}, geo_located=True):

        json = {k: v for k, v in json.items() if k in INPUT_PARAMS}
        self.json_input = json
        self.has_conditions = False
        self.model_endpoint = json.get("modelEndpoint")

        if geo_located:
            self.start_date = input_params.parse_date(json.get("startDate"))
            self.end_date = input_params.parse_date(json.get("endDate"))
            if self.start_date is None:
                self.start_date = input_params.parse_date(json.get("beginningDatetime"))
            if self.end_date is None:
                self.end_date = input_params.parse_date(json.get("endingDatetime"))

            self.is_event = input_params.parse_bool(json.get("isEvent"))
        else:
            self.is_event = False
            self.end_date = None
            self.start_date = None

        self.offer_is_duo = input_params.parse_bool(json.get("isDuo"))
        self.search_group_names = input_params.parse_to_list(json.get("categories"))
        self.subcategories_id = input_params.parse_to_list(json.get("subcategories"))
        self.offer_type_list = input_params.parse_to_list_of_dict(
            json.get("offerTypeList")
        )

        self.price_min = input_params.parse_float(json.get("priceMin"))
        self.price_max = input_params.parse_float(json.get("priceMax"))

        self.include_digital = input_params.parse_bool(json.get("isDigital"))
        # reco params
        self.nb_reco_display = input_params.parse_int(json.get("nbRecoDisplay"))

        self.is_reco_shuffled = input_params.parse_bool(json.get("isRecoShuffled"))
        self.is_reco_mixed = input_params.parse_bool(json.get("isRecoMixed"))
        self.mixing_features = json.get("mixingFeatures")

        if len(self.json_input) > 0:
            self.has_conditions = True

        self.setup_defaults()

    def setup_defaults(self):
        if self.nb_reco_display is None or self.nb_reco_display <= 0:
            self.nb_reco_display = NUMBER_OF_RECOMMENDATIONS
        if (
            self.mixing_features is None
            or self.mixing_features not in MIXING_FEATURE_LIST
        ):
            self.mixing_features = None
        if self.include_digital is None:
            self.include_digital = True
        # no digital offers when is_event=True
        if self.is_event == True:
            self.include_digital = False
