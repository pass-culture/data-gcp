from pcreco.utils.env_vars import (
    NUMBER_OF_RECOMMENDATIONS,
    SHUFFLE_RECOMMENDATION,
    MIXING_RECOMMENDATION,
    MIXING_FEATURE,
    MIXING_FEATURE_LIST,
    DEFAULT_RECO_RADIUS,
    DEFAULT_RECO_MODEL,
)
from pcreco.utils.geolocalisation import distance_to_radius_bucket
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
    "recoRadius",
    "isRecoMixed",
    "isRecoShuffled",
    "isSortByDistance",
    "isDigital",
    "mixingFeatures",
]


class PlaylistParamsIn:
    def __init__(self, json={}):

        json = {k: v for k, v in json.items() if k in INPUT_PARAMS}
        self.json_input = json
        self.has_conditions = False
        self.model_endpoint = json.get("modelEndpoint")

        # TODO : deprecated
        self.start_date = input_params.parse_date(json.get("startDate"))
        self.end_date = input_params.parse_date(json.get("endDate"))
        if self.start_date is None:
            self.start_date = input_params.parse_date(json.get("beginningDatetime"))
        if self.end_date is None:
            self.end_date = input_params.parse_date(json.get("endingDatetime"))

        self.is_event = input_params.parse_bool(json.get("isEvent"))
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
        self.reco_radius = input_params.parse_int(json.get("recoRadius"))

        self.is_reco_shuffled = input_params.parse_bool(json.get("isRecoShuffled"))
        self.is_sort_by_distance = input_params.parse_bool(json.get("isSortByDistance"))
        self.is_reco_mixed = input_params.parse_bool(json.get("isRecoMixed"))
        self.mixing_features = json.get("mixingFeatures")

        if len(self.json_input) > 0:
            self.has_conditions = True

        self.setup_defaults()

    def setup_defaults(self):
        if self.model_endpoint is None:
            self.model_endpoint = DEFAULT_RECO_MODEL
        if self.is_reco_shuffled is None:
            self.is_reco_shuffled = SHUFFLE_RECOMMENDATION
        if self.is_sort_by_distance is None:
            self.is_sort_by_distance = False
        if self.nb_reco_display is None or self.nb_reco_display <= 0:
            self.nb_reco_display = NUMBER_OF_RECOMMENDATIONS
        if self.reco_radius is None or self.reco_radius < 1000:
            self.reco_radius = DEFAULT_RECO_RADIUS
        else:
            self.reco_radius = distance_to_radius_bucket(self.reco_radius)
        if self.is_reco_mixed is None:
            self.is_reco_mixed = MIXING_RECOMMENDATION
        if (
            self.mixing_features is None
            or self.mixing_features not in MIXING_FEATURE_LIST
        ):
            self.mixing_features = MIXING_FEATURE
        if self.include_digital is None:
            self.include_digital = True
        # no digital offers when is_event=True
        if self.is_event == True:
            self.include_digital = False

    def _get_conditions(self) -> str:
        condition = ""
        if self.start_date:
            if self.is_event:
                column = "stock_beginning_date"
            else:
                column = "offer_creation_date"
            #
            if self.end_date:
                condition += f"""AND ({column} > '{self.start_date.isoformat()}' AND {column} < '{self.end_date.isoformat()}') \n"""
            else:
                condition += f"""AND ({column} > '{self.start_date.isoformat()}') \n"""
        if self.search_group_names is not None and len(self.search_group_names) > 0:
            # we filter by search_group_name to be iso with contentful categories
            condition += (
                f"AND ( search_group_name in {tuple(self.search_group_names)})\n"
            )
        if self.subcategories_id is not None and len(self.subcategories_id) > 0:
            # we filter by subcategory_id to be iso with contentful categories
            condition += f"AND ( subcategory_id in {tuple(self.subcategories_id)})\n"
        if self.price_max is not None and self.price_max >= 0:
            condition += f"AND stock_price<={self.price_max} \n"

        if self.price_min is not None and self.price_min >= 0:
            condition += f"AND stock_price>={self.price_min} \n"

        if self.offer_is_duo is not None:
            condition += f"AND (offer_is_duo={self.offer_is_duo}) \n"

        # if self.offer_type_list is not None and len(self.offer_type_list) > 0:
        #     condition += (
        #         "AND ("
        #         + " OR ".join(
        #             [
        #                 f"( offer_type_domain='{offer_type.key}' AND offer_type_label='{offer_type.value}' ) "
        #                 for offer_type in self.offer_type_list
        #             ]
        #         )
        #         + ")\n"
        #     )

        return condition
