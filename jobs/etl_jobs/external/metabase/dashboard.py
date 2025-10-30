import json
import re


class Dashboard:
    def __init__(self, dashboard_id, metabase_api):
        self.dashboard_id = dashboard_id
        self.metabase_api = metabase_api
        self.dashboard_info = self.metabase_api.get_dashboards(self.dashboard_id)

    def update_dashboard_filters(self, mapped_fields_dict):
        old_dict = self.dashboard_info

        new_stringify_dict = json.dumps(old_dict)

        for index_to_map, mapped_index in mapped_fields_dict.items():
            new_stringify_dict = re.sub(
                rf"\b{index_to_map}\b", f"{mapped_index}", new_stringify_dict
            )

        self.dashboard_info = json.loads(new_stringify_dict)

    def update_dashboard(self):
        self.metabase_api.put_dashboard(
            dashboard_id=self.dashboard_id, dashboard_dict=self.dashboard_info
        )
