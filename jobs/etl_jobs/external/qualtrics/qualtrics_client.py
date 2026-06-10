import time
from typing import Any

import requests

DEFAULT_CHUNK_SIZE = 5000
DEFAULT_POLL_INTERVAL = 5
DEFAULT_TIMEOUT = 600
REQUEST_TIMEOUT = 30


class QualtricsClient:
    def __init__(self, api_token: str, data_center: str):
        self.base_url = f"https://{data_center}.qualtrics.com/API/v3"
        self.session = requests.Session()
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
