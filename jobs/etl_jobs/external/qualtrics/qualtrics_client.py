import time

import requests


class QualtricsClient:
    def __init__(self, api_token: str, data_center: str):
        self.api_token = api_token
        self.data_center = data_center
        self.base_url = f"https://{data_center}.qualtrics.com/API/v3"
        self.headers = {
            "X-API-TOKEN": api_token,
            "Content-Type": "application/json",
        }

    def post_contacts_to_mailing_list(
        self,
        directory_id: str,
        mailing_list_id: str,
        contacts: list[dict],
    ) -> str:
        url = f"{self.base_url}/directories/{directory_id}/mailinglists/{mailing_list_id}/contactimports"
        payload = {"contacts": contacts}
        response = requests.post(url, headers=self.headers, json=payload)
        response.raise_for_status()
        data = response.json()
        import_id = data["result"]["id"]
        print(f"Contact import submitted: {import_id}")
        return import_id

    def wait_for_import(
        self,
        directory_id: str,
        mailing_list_id: str,
        import_id: str,
        poll_interval: int = 10,
        timeout: int = 600,
    ) -> dict:
        url = f"{self.base_url}/directories/{directory_id}/mailinglists/{mailing_list_id}/contactimports/{import_id}"
        elapsed = 0
        while elapsed < timeout:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            data = response.json()
            status = data["result"]["status"]
            print(f"Import {import_id} status: {status}")
            if status == "complete":
                return data["result"]
            if status == "failed":
                raise RuntimeError(f"Contact import {import_id} failed: {data}")
            time.sleep(poll_interval)
            elapsed += poll_interval
        raise TimeoutError(f"Contact import {import_id} timed out after {timeout}s")
