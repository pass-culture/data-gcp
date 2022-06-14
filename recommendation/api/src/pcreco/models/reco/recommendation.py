# rename to specific api route , and put in models dir
class RecommendationIn:
    def __init__(self, json):
        self.start_date = json.get("start_date", None)
        self.end_date = json.get("end_date", None)
        self.is_event = json.get("isEvent", None)
        self.search_group_names = json.get("categories", None)
        self.price_max = json.get("price_max", None)
        self.model_name = json.get("model_name", None)

    def _get_conditions(self) -> str:
        condition = ""
        if self.start_date:
            if self.is_event:
                column = "stock_beginning_date"
            else:
                column = "offer_creation_date"
            condition += f"""AND ({column} > '{self.start_date}' AND {column} < '{self.end_date}') \n"""
        if self.search_group_names:
            # we filter by search_group_name to be iso with contentful categories
            condition += (
                "AND ("
                + " OR ".join(
                    [f"search_group_name='{cat}'" for cat in self.search_group_names]
                )
                + ")\n"
            )
        if self.price_max:
            condition += f"AND stock_price<={self.price_max} \n"
        return condition
