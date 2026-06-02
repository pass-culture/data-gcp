import io
import time
import zipfile
from typing import Any

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DEFAULT_CHUNK_SIZE = 5000
DEFAULT_POLL_INTERVAL = 5
DEFAULT_TIMEOUT = 600
REQUEST_TIMEOUT = 30


class QualtricsClient:
    def __init__(self, api_token: str, data_center: str):
        self.base_url = f"https://{data_center}.qualtrics.com/API/v3"
        retry = Retry(total=3, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
        self.session = requests.Session()
        self.session.mount("https://", HTTPAdapter(max_retries=retry))
        self.session.headers.update(
            {
                "X-API-TOKEN": api_token,
                "Content-Type": "application/json",
            }
        )

    def _request(self, method: str, url: str, **kwargs: Any) -> dict:
        response = self.session.request(method, url, timeout=REQUEST_TIMEOUT, **kwargs)
        if not response.ok:
            raise RuntimeError(
                f"Qualtrics {method} {url} failed: "
                f"{response.status_code} {response.text}"
            )
        return response.json()

    def list_surveys(self) -> list[dict]:
        url = f"{self.base_url}/surveys"
        elements: list[dict] = []
        next_page: str | None = url
        while next_page:
            data = self._request("GET", next_page)
            elements.extend(data["result"]["elements"])
            next_page = data["result"].get("nextPage")
        return elements

    def download_survey_responses(
        self,
        survey_id: str,
        poll_interval: int = DEFAULT_POLL_INTERVAL,
    ) -> pd.DataFrame:
        base_url = f"{self.base_url}/surveys/{survey_id}/export-responses/"

        data = self._request("POST", base_url, json={"format": "csv"})
        progress_id = data["result"]["progressId"]

        percent = 0
        while percent < 100:
            data = self._request("GET", base_url + progress_id)
            percent = data["result"]["percentComplete"]
            if percent < 100:
                time.sleep(poll_interval)
        file_id = data["result"]["fileId"]

        file_url = f"{self.base_url}/surveys/{survey_id}/export-responses/{file_id}/file"
        response = self.session.request("GET", file_url, timeout=REQUEST_TIMEOUT)
        if not response.ok:
            raise RuntimeError(
                f"Qualtrics GET {file_url} failed: {response.status_code} {response.text}"
            )
        zf = zipfile.ZipFile(io.BytesIO(response.content))
        print(f"Downloaded survey {survey_id}")
        return pd.read_csv(zf.open(zf.namelist()[0]))

    def fetch_opt_out_contacts(self, directory_id: str) -> list[dict]:
        url = f"{self.base_url}/directories/{directory_id}/contacts/optedOutContacts"
        elements: list[dict] = []
        next_page: str | None = url
        page = 0
        while next_page:
            print(f"Page {page}")
            data = self._request("GET", next_page)
            elements.extend(data["result"]["elements"])
            next_page = data["result"].get("nextPage")
            page += 1
        return elements

    def list_mailing_lists(self, directory_id: str) -> list[dict]:
        url = f"{self.base_url}/directories/{directory_id}/mailinglists"
        elements: list[dict] = []
        next_page: str | None = url
        while next_page:
            data = self._request("GET", next_page)
            elements.extend(data["result"]["elements"])
            next_page = data["result"].get("nextPage")
        return elements

    def import_contacts(
        self,
        directory_id: str,
        mailing_list_id: str,
        contacts: list[dict],
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        poll_interval: int = DEFAULT_POLL_INTERVAL,
        timeout: int = DEFAULT_TIMEOUT,
    ) -> list[dict]:
        if not contacts:
            print("No contacts to import")
            return []

        url = (
            f"{self.base_url}/directories/{directory_id}"
            f"/mailinglists/{mailing_list_id}/transactioncontacts"
        )
        results: list[dict] = []
        for chunk_index, chunk in enumerate(_chunked(contacts, chunk_size)):
            data = self._request("POST", url, json={"contacts": chunk})
            result = data["result"]
            tracking_url = result["tracking"]["url"]
            print(
                f"Chunk {chunk_index + 1}: submitted {len(chunk)} contacts "
                f"(progressId={result['id']})"
            )
            results.append(self._wait_for_import(tracking_url, poll_interval, timeout))
        return results

    def _wait_for_import(
        self, tracking_url: str, poll_interval: int, timeout: int
    ) -> dict:
        elapsed = 0
        while elapsed < timeout:
            data = self._request("GET", tracking_url)
            result = data["result"]
            status = result.get("status")
            if status == "complete":
                unprocessed = result.get("contacts", {}).get("unprocessed", [])
                if unprocessed:
                    print(
                        f"Import complete with {len(unprocessed)} unprocessed contacts"
                    )
                else:
                    print("Import complete")
                return result
            if status == "failed":
                raise RuntimeError(f"Qualtrics import failed: {data}")
            time.sleep(poll_interval)
            elapsed += poll_interval
        raise TimeoutError(f"Qualtrics import did not complete in {timeout}s")


def _chunked(items: list[dict], size: int):
    for start in range(0, len(items), size):
        yield items[start : start + size]
