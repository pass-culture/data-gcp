import json
import re

class QueryCard:
    def __init__(self, card_id, metabase_api):
        self.card_id = card_id
        self.metabase_api = metabase_api
        self.card_info = self.metabase_api.get_cards(self.card_id)

    def update_table_id(self, new_table_id):
        self.card_info["table_id"] = new_table_id

    def update_dataset_query(self, mapped_fields_dict, legacy_table_id, new_table_id):
        old_dict = self.card_info

        new_stringify_dict = json.dumps(old_dict)

        for index_to_map, mapped_index in mapped_fields_dict.items():
            new_stringify_dict = re.sub(rf"\b{index_to_map}\b", f"{mapped_index}", new_stringify_dict)

        new_stringify_dict = new_stringify_dict.replace(
            f'"source-table": {legacy_table_id}', f'"source-table": {new_table_id}'
        )

        self.card_info = json.loads(new_stringify_dict)

    def update_card(self):
        self.metabase_api.put_card(card_id=self.card_id, card_dict=self.card_info)
