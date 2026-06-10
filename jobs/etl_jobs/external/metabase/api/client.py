"""Metabase API client with retry logic and type-safe returns.

This is the only class in the codebase — it holds HTTP session state
(auth token, connection pool), which justifies a class.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from api.models import Card, MetabaseField, Table

logger = logging.getLogger(__name__)

DATABASE_TABLES_CACHE_PATH = Path("data/cache_database_tables.json")
CARD_DEPENDENCIES_CACHE_PATH = Path("data/cache_card_dependencies.json")

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
        self.session.headers.update({"Content-Type": "application/json", "X-Metabase-Session": session_token})

    @classmethod
    def from_credentials(
        cls, host: str, username: str, password: str, bearer_token: str | None = None
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
            f"{host}/api/session", headers=headers, data=json.dumps({"username": username, "password": password})
        )
        if not response.ok:
            logger.error("Metabase /api/session failed: status=%s body=%s", response.status_code, response.text[:500])
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
            f"{self.host}/api/card/{card_id}", data=card.model_dump_json(by_alias=True, exclude_none=False)
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
        response = self.session.get(f"{self.host}/api/table/{table_id}/query_metadata")
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
        result: list[dict[str, Any]] = response.json()
        return result

    def find_table_id(self, table_name: str, schema_name: str) -> int | None:
        """Find a table ID by name and schema from the cached table catalog.

        Reads from the file cache built by build_table_catalog(), which
        includes both active and inactive (deleted) tables.

        Args:
            table_name: The table name.
            schema_name: The schema name.

        Returns:
            The table ID if found, None otherwise.

        Raises:
            FileNotFoundError: If the table catalog cache does not exist.
        """
        if not DATABASE_TABLES_CACHE_PATH.exists():
            raise FileNotFoundError(
                f"Table catalog not found at {DATABASE_TABLES_CACHE_PATH}. Call build_table_catalog() first."
            )
        with open(DATABASE_TABLES_CACHE_PATH) as f:
            catalog: dict[str, list[dict[str, Any]]] = json.load(f)
        for t in catalog.get(schema_name, []):
            if t["name"] == table_name:
                table_id: int = t["id"]
                return table_id
        return None

    @_RETRY
    def find_database_id(self, database_name: str) -> int | None:
        """Resolve a Metabase database name to its ID.

        Args:
            database_name: The display name of the database in Metabase.

        Returns:
            The database ID if found, None otherwise.
        """
        response = self.session.get(f"{self.host}/api/database")
        response.raise_for_status()
        data = response.json()
        databases: list[dict[str, Any]] = data["data"] if isinstance(data, dict) and "data" in data else data
        for db in databases:
            if db.get("name") == database_name:
                db_id: int = db["id"]
                return db_id
        return None

    @_RETRY
    def fetch_database_tables(self, database_id: int) -> list[dict[str, Any]]:
        """Fetch all tables (including inactive) for a database.

        Uses the /api/database/:id/metadata endpoint which returns
        both active and inactive tables, unlike /api/table which
        only returns active tables.

        Args:
            database_id: The Metabase database ID.

        Returns:
            A list of table dicts.
        """
        response = self.session.get(
            f"{self.host}/api/database/{database_id}/metadata",
            params={"skip_fields": "true", "remove_inactive": "false"},
        )
        response.raise_for_status()
        result: list[dict[str, Any]] = response.json().get("tables", [])
        return result

    def build_table_catalog(self, database_id: int) -> dict[str, list[dict[str, Any]]]:
        """Fetch all tables for a database and cache them to a file.

        The cache file is structured as schema → list of tables, where
        each table entry contains id, name, and active status.

        Args:
            database_id: The Metabase database ID.

        Returns:
            The catalog dict mapping schema → list of table entries.
        """
        tables = self.fetch_database_tables(database_id)
        catalog: dict[str, list[dict[str, Any]]] = {}
        for t in tables:
            schema = t.get("schema", "")
            entry: dict[str, Any] = {"id": t["id"], "name": t["name"], "active": t.get("active", True)}
            catalog.setdefault(schema, []).append(entry)
        DATABASE_TABLES_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(DATABASE_TABLES_CACHE_PATH, "w") as f:
            json.dump(catalog, f, indent=2)
        logger.info("Cached %d tables across %d schemas to %s", len(tables), len(catalog), DATABASE_TABLES_CACHE_PATH)
        return catalog

    def get_table_fields(self, table_id: int) -> list[MetabaseField]:
        """Get fields for a table.

        Args:
            table_id: The Metabase table ID.

        Returns:
            A list of MetabaseField models.
        """
        table = self.get_table_metadata(table_id)
        return table.fields or []

    @_RETRY
    def fetch_collection(self, collection_id: int) -> dict[str, Any]:
        """Fetch a single collection by ID.

        Args:
            collection_id: The Metabase collection ID.

        Returns:
            The collection dict from the API.
        """
        response = self.session.get(f"{self.host}/api/collection/{collection_id}")
        response.raise_for_status()
        result: dict[str, Any] = response.json()
        return result

    def resolve_collection_path(
        self,
        collection_id: int | None,
        collection_cache: dict[int, tuple[str, str]],
    ) -> tuple[str, str]:
        """Return (path_names, path_ids) for a collection.

        Uses the collection's ``location`` field (e.g. ``"/1/5/"``) plus
        the collection's own name to build the full path.

        Results are memoized in ``collection_cache`` (caller passes a
        shared dict so sibling cards in the same collection skip API calls).

        Args:
            collection_id: The Metabase collection ID, or None.
            collection_cache: Shared cache mapping collection ID to (path_names, path_ids).

        Returns:
            A tuple of (path_names, path_ids). Returns ("", "") when collection_id is None.
        """
        if collection_id is None:
            return ("", "")

        if collection_id in collection_cache:
            return collection_cache[collection_id]

        collection = self.fetch_collection(collection_id)
        name = collection.get("name", "")
        location = collection.get("location", "/")

        # Parse ancestor IDs from location (e.g. "/1/5/" → [1, 5])
        ancestor_ids = [int(x) for x in location.strip("/").split("/") if x]

        # Resolve each ancestor's name (fetch if not cached)
        ancestor_names: list[str] = []
        for ancestor_id in ancestor_ids:
            if ancestor_id in collection_cache:
                # Extract the last segment of the cached ancestor's path_names
                cached_names, _ = collection_cache[ancestor_id]
                ancestor_names.append(cached_names.rsplit("/", 1)[-1] if cached_names else "")
            else:
                ancestor = self.fetch_collection(ancestor_id)
                ancestor_name = ancestor.get("name", "")
                ancestor_names.append(ancestor_name)

        # Build full paths
        all_names = ancestor_names + [name]
        all_ids = ancestor_ids + [collection_id]
        path_names = "/".join(all_names)
        path_ids = "/".join(str(i) for i in all_ids)

        # Cache this collection and all ancestors opportunistically
        for idx in range(len(all_ids)):
            cid = all_ids[idx]
            if cid not in collection_cache:
                partial_names = "/".join(all_names[: idx + 1])
                partial_ids = "/".join(str(i) for i in all_ids[: idx + 1])
                collection_cache[cid] = (partial_names, partial_ids)

        return (path_names, path_ids)

    @_RETRY
    def fetch_all_cards(self) -> list[dict[str, Any]]:
        """Fetch all cards from the Metabase API.

        Returns:
            A list of card dicts from GET /api/card.
        """
        response = self.session.get(f"{self.host}/api/card")
        response.raise_for_status()
        result: list[dict[str, Any]] = response.json()
        return result
