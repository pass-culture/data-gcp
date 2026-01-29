class NativeCard:
    def __init__(self, card_id, metabase_api):
        self.card_id = card_id
        self.metabase_api = metabase_api
        self.card_info = self.metabase_api.get_cards(self.card_id)
        self.query = self.card_info["legacy_query"]["query"]["native"]["query"]

    def replace_table_name(self, legacy_table_name, new_table_name):
        self.query = self.query.replace(legacy_table_name, new_table_name)

        self.card_info["legacy_query"]["query"]["native"]["query"] = self.query

    def replace_schema_name(
        self, legacy_schema_name, new_schema_name, legacy_table_name, new_table_name
    ):
        self.query = self.query.replace(
            f"{legacy_schema_name}.{legacy_table_name}",
            f"{new_schema_name}.{new_table_name}",
        )

        self.card_info["legacy_query"]["query"]["native"]["query"] = self.query

    def replace_column_names(self, column_mapping):
        new_query = ""
        for line in self.query.splitlines():
            if "[[" in line and "]]" in line:
                new_query += line + "\n"
            else:
                for column_key_map, mapped_column_key in column_mapping.items():
                    line = line.replace(column_key_map, mapped_column_key)
                new_query += line + "\n"

        self.card_info["legacy_query"]["query"]["native"]["query"] = new_query

    def update_filters(self, metabase_field_mapping):
        for _, d in self.card_info["legacy_query"]["query"]["native"][
            "template-tags"
        ].items():
            if d["type"] == "dimension":
                d["dimension"][1] = metabase_field_mapping.get(
                    d["dimension"][1], d["dimension"][1]
                )

    def update_query(self):
        self.metabase_api.put_card(self.card_id, self.card_info)
