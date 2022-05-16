class InputApi:
    def __init__(self, json):
        self.start_date = json.get("start_date", None)
        self.end_date = json.get("end_date", None)
        self.isevent = json.get("isevent", None)
        self.categories = json.get("categories", None)
        self.subcategories = json.get("subcategories", None)
        self.price_max = json.get("price_max", None)
        self.model_name = json.get("model_name", None)

    def _get_conditions(self):
        condition = ""
        if self.start_date:
            if self.isevent:
                column = "stock_beginning_date"
            else:
                column = "offer_creation_date"
            condition += f"""AND ({column} > '{self.start_date}' AND {column} < '{self.end_date}') \n"""
        if self.categories:
            condition += (
                "AND ("
                + " OR ".join([f"category='{cat}'" for cat in self.categories])
                + ")\n"
            )
        if self.subcategories:
            condition += (
                "AND ("
                + " OR ".join([f"subcategory_id='{cat}'" for cat in self.subcategories])
                + ")\n"
            )
        if self.price_max:
            condition += f"AND stock_price<={self.price_max} \n"
        return condition
