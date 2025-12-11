import json
from time import sleep

import requests
from google.auth.transport.requests import Request
from google.oauth2 import id_token
from tqdm import tqdm


class MetabaseAPI:
    def get_open_id(self, client_id):
        return id_token.fetch_id_token(Request(), client_id)

    def __init__(self, username, password, host, client_id):
        self.host = host
        self.bearer_token = f"Bearer {self.get_open_id(client_id)}"

        url = f"{host}/api/session"
        response = requests.post(
            url,
            headers={
                "Content-Type": "application/json",
                "Authorization": self.bearer_token,
            },
            data=json.dumps({"username": username, "password": password}),
        )
        response.raise_for_status()  # raises exception when not a 2xx response
        if response.status_code != 204:
            token_json = response.json()
            if "id" not in token_json:
                raise Exception(f"Error login to {host}, error: {token_json}")
            self.headers = {
                "Content-Type": "application/json",
                "X-Metabase-Session": token_json["id"],
                "Authorization": self.bearer_token,
            }

    def get_users(self):
        response = requests.get(f"{self.host}/api/user/", headers=self.headers)
        return response.json()["data"]

    def put_card(self, _id, _dict):
        response = requests.put(
            f"{self.host}/api/card/{_id}", data=json.dumps(_dict), headers=self.headers
        )
        return response.json()

    def update_card_collections(self, card_ids, collection_id):
        params = {"card_ids": card_ids, "collection_id": collection_id}

        response = requests.post(
            f"{self.host}/api/card/collections",
            data=json.dumps(params),
            headers=self.headers,
        )
        return response.json()

    def get_cards(self, _id=None):
        if _id:
            response = requests.get(f"{self.host}/api/card/{_id}", headers=self.headers)
        else:
            response = requests.get(
                f"{self.host}/api/card/?legacy-mbql=true", headers=self.headers
            )
        return response.json()

    def get_table(self, table_id=None):
        if table_id:
            response = requests.get(
                f"{self.host}/api/table/{table_id}", headers=self.headers
            )
        else:
            response = requests.get(f"{self.host}/api/table/", headers=self.headers)
        return response.json()

    def format_cards(self, cards):
        export_cards = []
        for i, _c in enumerate(cards):
            card_dict = {}
            if "dataset_query" in _c:
                _dataset_type = _c["dataset_query"]["type"]
                creator_id = (
                    _c["creator"]["id"] if "creator" in _c else _c["creator_id"]
                )

                card_dict = {
                    "card_id": _c["id"],
                    "card_name": _c["name"],
                    "card_description": _c["description"],
                    "card_archived": _c["archived"],
                    "card_creator_id": creator_id,
                    "card_updated_at": _c["updated_at"],
                    "card_database_id": _c["database_id"],
                    "card_dataset_query": (
                        _c["dataset_query"][_dataset_type]["query"]
                        if _dataset_type == "native"
                        else None
                    ),
                }
                if "last-edit-info" in _c:
                    edit_dict = {
                        "card_last_edit_user_id": _c["last-edit-info"]["id"],
                        "card_last_edit_email": _c["last-edit-info"]["email"],
                        "card_last_edit_ts": _c["last-edit-info"]["timestamp"],
                    }
                    card_dict = dict(**card_dict, **edit_dict)

                export_cards.append(card_dict)

        return export_cards

    def get_collections(self, _id=None):
        if _id:
            response = requests.get(
                f"{self.host}/api/collection/{_id}", headers=self.headers
            )
        else:
            response = requests.get(
                f"{self.host}/api/collection/", headers=self.headers
            )
        return response.json()

    def get_dashboards(self, _id=None):
        if _id:
            response = requests.get(
                f"{self.host}/api/dashboard/{_id}", headers=self.headers
            )
        else:
            response = requests.get(f"{self.host}/api/dashboard/", headers=self.headers)
        return response.json()

    def get_bookmarks(self):
        response = requests.get(f"{self.host}/api/bookmark/", headers=self.headers)
        return response.json()

    def export_dashboards(self, dashboards, timeout_sleep=1):
        export_dashboard_cards = []
        for dash in tqdm(dashboards):
            dash_id = dash["id"]
            creator_id = (
                dash["creator"]["id"] if "creator" in dash else dash["creator_id"]
            )
            dash_details = self.get_dashboards(dash_id)
            last_edit_dict = {}
            dash_dict = {
                "dashboard_id": dash_details["id"],
                "creator_id": creator_id,
                "dashboard_name": dash_details["name"],
                "dashboard_archived": dash_details["archived"],
                "dashboard_collection_position": dash_details["collection_position"],
                "dashboard_created_at": dash_details["created_at"],
                "dashboard_cache_ttl": dash_details["cache_ttl"],
                "query_average_duration": dash.get("query_average_duration", None),
            }
            if "last-edit-info" in dash_details:
                last_edit_dict = {
                    "dashboard_last_edit_at": dash_details["last-edit-info"][
                        "timestamp"
                    ],
                    "dashboard_last_edit_email": dash_details["last-edit-info"][
                        "email"
                    ],
                    "dashboard_last_edit_user_id": dash_details["last-edit-info"]["id"],
                }
            cards = dash_details["ordered_cards"]
            formatted_cards = self.format_cards([c["card"] for c in cards])
            for card_dict in formatted_cards:
                export_dashboard_cards.append(
                    dict(**dash_dict, **card_dict, **last_edit_dict)
                )
            sleep(timeout_sleep)
        return export_dashboard_cards
