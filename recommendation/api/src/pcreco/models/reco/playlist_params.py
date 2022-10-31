# rename to specific api route , and put in models dir
from pcreco.utils.env_vars import (
    NUMBER_OF_RECOMMENDATIONS,
    SHUFFLE_RECOMMENDATION,
    MIXING_RECOMMENDATION,
)


class PlaylistParamsIn:
    def __init__(self, json):
        self.model_name = json.get("model_name", None)
        self.start_date = json.get("start_date", None)
        self.end_date = json.get("end_date", None)
        self.is_event = json.get("isEvent", None)
        self.offer_is_duo = json.get("offer_is_duo", None)
        self.reco_is_shuffle = json.get("reco_is_shuffle", SHUFFLE_RECOMMENDATION)
        self.is_sort_by_distance = json.get("is_sort_by_distance", False)
        self.search_group_names = json.get("categories", None)
        self.subcategories_id = json.get("subcategories", None)
        self.movie_type = json.get("movie_type", None)
        self.offer_type_label = json.get("offer_type_label", None)
        self.offer_sub_type_label = json.get("offer_sub_type_label", None)
        self.macro_rayon = json.get("macro_rayon", None)
        self.price_max = json.get("price_max", None)
        self.nb_reco_display = json.get("nb_reco_display", NUMBER_OF_RECOMMENDATIONS)
        self.iris_radius = json.get("iris_radius", None)
        self.is_reco_mixed = json.get("is_reco_mixed", MIXING_RECOMMENDATION)
        self.mixing_features = json.get("mixing_features", "subcategory_id")

        if (
            self.is_event is not None
            or self.search_group_names is not None
            or self.subcategories_id is not None
            or self.price_max is not None
            or self.offer_is_duo is not None
            or self.movie_type is not None
            or self.offer_type_label is not None
            or self.offer_sub_type_label is not None
            or self.macro_rayon is not None
            or self.nb_reco_display is not NUMBER_OF_RECOMMENDATIONS
            or self.iris_radius is not None
            or self.reco_is_shuffle is not SHUFFLE_RECOMMENDATION
            or self.is_sort_by_distance is not False
        ):
            self.has_conditions = True
            self.json_input = json
        else:
            self.has_conditions = False
            self.json_input = None

    def _get_conditions(self) -> str:
        condition = ""
        if self.start_date:
            if self.is_event:
                column = "stock_beginning_date"
            else:
                column = "offer_creation_date"
            #
            if self.end_date:
                condition += f"""AND ({column} > '{self.start_date}' AND {column} < '{self.end_date}') \n"""
            else:
                condition += f"""AND ({column} > '{self.start_date}') \n"""
        if self.search_group_names is not None and len(self.search_group_names) > 0:
            # we filter by search_group_name to be iso with contentful categories
            condition += (
                "AND ("
                + " OR ".join(
                    [f"search_group_name='{cat}'" for cat in self.search_group_names]
                )
                + ")\n"
            )
        if self.subcategories_id is not None and len(self.subcategories_id) > 0:
            # we filter by subcategory_id to be iso with contentful categories
            condition += (
                "AND ("
                + " OR ".join(
                    [f"subcategory_id='{cat}'" for cat in self.subcategories_id]
                )
                + ")\n"
            )
        if self.price_max is not None and self.price_max >= 0:
            condition += f"AND stock_price<={self.price_max} \n"

        if self.offer_is_duo is not None:
            condition += f"AND ( offer_is_duo={self.offer_is_duo}) \n"

        if self.movie_type is not None and len(self.movie_type) > 0:
            condition += (
                "AND ("
                + " OR ".join([f"movie_type='{cat}'" for cat in self.movie_type])
                + ")\n"
            )

        if self.offer_type_label is not None and len(self.offer_type_label) > 0:
            condition += (
                "AND ("
                + " OR ".join(
                    [f"offer_type_label='{cat}'" for cat in self.offer_type_label]
                )
                + ")\n"
            )

        if self.offer_sub_type_label is not None and len(self.offer_sub_type_label) > 0:
            condition += (
                "AND ("
                + " OR ".join(
                    [
                        f"offer_sub_type_label='{cat}'"
                        for cat in self.offer_sub_type_label
                    ]
                )
                + ")\n"
            )

        if self.macro_rayon is not None and len(self.macro_rayon) > 0:
            condition += (
                "AND ("
                + " OR ".join([f"macro_rayon='{cat}'" for cat in self.macro_rayon])
                + ")\n"
            )

        return condition
