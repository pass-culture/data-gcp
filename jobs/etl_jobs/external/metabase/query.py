import json


class QueryCard:
    def __init__(self, card_id, metabase_api):
        self.card_id = card_id
        self.metabase_api = metabase_api
        self.card_info = self.metabase_api.get_cards(self.card_id)

    def update_table_id(self, new_table_id):
        self.card_info["table_id"] = new_table_id

    def update_dataset_query(self, mapped_fields_dict, legacy_table_id, new_table_id):
        old_dict = self.card_info["dataset_query"]["query"]

        new_stringify_dict = json.dumps(old_dict)

        for index_to_map, mapped_index in mapped_fields_dict.items():
            new_stringify_dict = new_stringify_dict.replace(
                f'"field", {index_to_map}', f'"field", {mapped_index}'
            )
            new_stringify_dict = new_stringify_dict.replace(
                f'"source-field": {index_to_map}', f'"source-field": {mapped_index}'
            )

        new_stringify_dict = new_stringify_dict.replace(
            f'"source-table": {legacy_table_id}', f'"source-table": {new_table_id}'
        )

        self.card_info["dataset_query"]["query"] = json.loads(new_stringify_dict)

    def update_viz_settings(self, mapped_fields_dict):
        if "table.columns" in self.card_info["visualization_settings"]:
            for column in self.card_info["visualization_settings"]["table.columns"]:
                if "field_ref" in column and column.get("field_ref")[0] == "field":
                    column["field_ref"][1] = mapped_fields_dict.get(
                        column["field_ref"][1], column["field_ref"][1]
                    )
                elif "fieldRef" in column and column.get("fieldRef")[0] == "field":
                    column["fieldRef"][1] = mapped_fields_dict.get(
                        column["fieldRef"][1], column["fieldRef"][1]
                    )

    def update_result_metadata(self, mapped_fields_dict):

        if self.card_info["result_metadata"]:
            for result in self.card_info["result_metadata"]:
                if (
                    "field_ref" in result
                    and result["field_ref"][0] == "field"
                    and isinstance(result["field_ref"][1], int)
                ):
                    result["field_ref"][1] = mapped_fields_dict.get(
                        result["field_ref"][1], result["field_ref"][1]
                    )
                    result["id"] = mapped_fields_dict.get(result["id"], result["id"])

    def update_card(self):
        self.metabase_api.put_card(card_id=self.card_id, card_dict=self.card_info)
