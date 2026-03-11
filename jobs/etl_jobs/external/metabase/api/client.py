"""Metabase API client with retry logic and type-safe returns.

This is the only class in the codebase — it holds HTTP session state
(auth token, connection pool), which justifies a class.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from api.models import Card, MetabaseField, Table

logger = logging.getLogger(__name__)

# Retry config: 3 attempts, 2–10s exponential backoff
_RETRY = retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    reraise=True,
)


class MetabaseClient:
    """HTTP client for the Metabase API.

    Supports two authentication modes:
    - Session token (for production with GCP OAuth2)
    - Username/password (for local development with Docker)
    """

    def __init__(self, host: str, session_token: str) -> None:
        """Initialize with a Metabase host and session token.

        Args:
            host: Metabase base URL (e.g., "http://localhost:3000").
            session_token: Metabase session token from /api/session.
        """
        self.host = host.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Content-Type": "application/json",
                "X-Metabase-Session": session_token,
            }
        )

    @classmethod
    def from_credentials(
        cls,
        host: str,
        username: str,
        password: str,
        bearer_token: str | None = None,
    ) -> MetabaseClient:
        """Create a client by authenticating with username/password.

        Args:
            host: Metabase base URL.
            username: Metabase username (email).
            password: Metabase password.
            bearer_token: Optional GCP OAuth2 bearer token for IAP.

        Returns:
            An authenticated MetabaseClient.
        """
        host = host.rstrip("/")
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if bearer_token:
            headers["Authorization"] = f"Bearer {bearer_token}"

        response = requests.post(
            f"{host}/api/session",
            headers=headers,
            data=json.dumps({"username": username, "password": password}),
        )
        response.raise_for_status()
        token_data = response.json()

        if "id" not in token_data:
            raise ValueError(f"Failed to authenticate with Metabase: {token_data}")

        session_token = token_data["id"]
        client = cls(host=host, session_token=session_token)

        # Also set bearer token if provided (for GCP IAP)
        if bearer_token:
            client.session.headers["Authorization"] = f"Bearer {bearer_token}"

        return client

    @_RETRY
    def get_card(self, card_id: int) -> Card:
        """Fetch a single card by ID.

        Args:
            card_id: The Metabase card ID.

        Returns:
            A Card model.
        """
        response = self.session.get(f"{self.host}/api/card/{card_id}")
        response.raise_for_status()
        return Card.model_validate(response.json())

    @_RETRY
    def put_card(self, card_id: int, card: Card) -> Card:
        """Update a card via PUT.

        Args:
            card_id: The Metabase card ID.
            card: The updated Card model.

        Returns:
            The updated Card as returned by the API.
        """
        response = self.session.put(
            f"{self.host}/api/card/{card_id}",
            data=card.model_dump_json(by_alias=True, exclude_none=False),
        )
        response.raise_for_status()
        return Card.model_validate(response.json())

    @_RETRY
    def get_table_metadata(self, table_id: int) -> Table:
        """Fetch table metadata including fields.

        Args:
            table_id: The Metabase table ID.

        Returns:
            A Table model with fields populated.
        """
        response = self.session.get(
            f"{self.host}/api/table/{table_id}/query_metadata"
        )
        response.raise_for_status()
        return Table.model_validate(response.json())

    @_RETRY
    def list_tables(self) -> list[dict[str, Any]]:
        """List all tables known to Metabase.

        Returns:
            A list of table dicts with id, name, schema keys.
        """
        response = self.session.get(f"{self.host}/api/table")
        response.raise_for_status()
        return response.json()

    def find_table_id(
        self, table_name: str, schema_name: str
    ) -> int | None:
        """Find a table ID by name and schema.

        Args:
            table_name: The table name.
            schema_name: The schema name.

        Returns:
            The table ID if found, None otherwise.
        """
        tables = self.list_tables()
        for t in tables:
            if t.get("name") == table_name and t.get("schema") == schema_name:
                return t["id"]
        return None

    def get_table_fields(self, table_id: int) -> list[MetabaseField]:
        """Get fields for a table.

        Args:
            table_id: The Metabase table ID.

        Returns:
            A list of MetabaseField models.
        """
        table = self.get_table_metadata(table_id)
        return table.fields or []
