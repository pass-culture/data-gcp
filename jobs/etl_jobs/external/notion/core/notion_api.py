"""Notion HTTP client for the dashboard-docs export.

Only the network layer lives here (`NotionClient`). The pure block/property
rendering helpers live in `core.parsing` so they can be unit-tested without
network or credentials.
"""

import httpx

from core.parsing import block_line

API_BASE = "https://api.notion.com/v1"


class NotionClient:
    def __init__(self, token: str, notion_version: str, timeout: int = 60):
        self._headers = {
            "Authorization": f"Bearer {token}",
            "Notion-Version": notion_version,
            "Content-Type": "application/json",
        }
        self._timeout = timeout

    def get_database(self, db_id: str) -> dict:
        r = httpx.get(
            f"{API_BASE}/databases/{db_id}",
            headers=self._headers,
            timeout=self._timeout,
        )
        r.raise_for_status()
        return r.json()

    def query_db(self, db_id: str):
        cursor = None
        while True:
            body = {"page_size": 50}
            if cursor:
                body["start_cursor"] = cursor
            r = httpx.post(
                f"{API_BASE}/databases/{db_id}/query",
                headers=self._headers,
                json=body,
                timeout=self._timeout,
            )
            r.raise_for_status()
            j = r.json()
            yield from j.get("results", [])
            if not j.get("has_more"):
                break
            cursor = j.get("next_cursor")

    def _children(self, block_id: str) -> list:
        items, cursor = [], None
        while True:
            params = {"page_size": 100}
            if cursor:
                params["start_cursor"] = cursor
            r = httpx.get(
                f"{API_BASE}/blocks/{block_id}/children",
                headers=self._headers,
                params=params,
                timeout=self._timeout,
            )
            r.raise_for_status()
            j = r.json()
            items.extend(j.get("results", []))
            if not j.get("has_more"):
                break
            cursor = j.get("next_cursor")
        return items

    def _render_block(self, b: dict, depth: int = 0) -> str:
        pad = "  " * depth
        line = block_line(b)
        out = [f"{pad}{line}"] if line else []
        if b.get("has_children"):
            for child in self._children(b["id"]):
                sub = self._render_block(child, depth + 1)
                if sub:
                    out.append(sub)
        return "\n".join(out)

    def page_markdown(self, page_id: str) -> str:
        return "\n\n".join(
            filter(None, (self._render_block(b) for b in self._children(page_id)))
        )
