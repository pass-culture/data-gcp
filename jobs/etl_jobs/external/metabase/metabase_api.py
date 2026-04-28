import json

import requests

from authent import _sign_jwt


class MetabaseAPI:
    def __init__(self, username, password, host, client_id):
        self.host = host
        self.bearer_token = f"Bearer {_sign_jwt(client_id)}"

        url = f"{self.host}/api/session"
        response = requests.post(
            url,
            headers={
                "Content-Type": "application/json",
                "Authorization": self.bearer_token,
            },
            data=json.dumps(
                {"username": username, "password": password.get_secret_value()}
            ),
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

    def put_card(self, card_id, card_dict):
        response = requests.put(
            f"{self.host}/api/card/{card_id}",
            data=json.dumps(card_dict),
            headers=self.headers,
        )
        return response.json()

    def get_cards(self, card_id=None):
        if card_id:
            response = requests.get(
                f"{self.host}/api/card/{card_id}", headers=self.headers
            )
        else:
            response = requests.get(f"{self.host}/api/card/", headers=self.headers)
        return response.json()

    def get_table(self, table_id=None):
        if table_id:
            response = requests.get(
                f"{self.host}/api/table/{table_id}", headers=self.headers
            )
        else:
            response = requests.get(f"{self.host}/api/table/", headers=self.headers)
        return response.json()

    def get_table_metadata(self, table_id=None):
        if table_id:
            response = requests.get(
                f"{self.host}/api/table/{table_id}/query_metadata", headers=self.headers
            )
        return response.json()
