import logging
import re

import pandas as pd

from core.utils import (
    ENVIRONMENT_SHORT_NAME,
    INT_METABASE_DATASET,
    PROJECT_NAME,
)

logger = logging.getLogger(__name__)


class ListArchive:
    def __init__(self, metabase_folder, rule_sql):
        self.metabase_folder = metabase_folder
        self.rule_sql = rule_sql

    def get_data_archiving(self):
        """Run SQL query and save data in a dataframe."""
        logger.info("Fetching archiving data for folder '%s'", self.metabase_folder)

        if not self.rule_sql:
            logger.warning("No archiving SQL rule defined, skipping")
            self.archive_df = pd.DataFrame()
            return

        query = f"SELECT * FROM `{INT_METABASE_DATASET}.activity` {self.rule_sql}"
        self.archive_df = pd.read_gbq(query)
        logger.info(
            "%d cards ready to be archived in folder '%s'",
            len(self.archive_df),
            self.metabase_folder,
        )

    def preprocess_data_archiving(self):
        if self.archive_df.empty:
            return []

        try:
            self.archive_df["destination_collection_id"] = (
                self.archive_df["archive_location_level_2"]
                .dropna()
                .str.split("/")
                .apply(lambda x: x[-1])
            )
            self.archive_df = self.archive_df[
                ~self.archive_df["archive_location_level_2"].isna()
            ]
        except Exception:
            logger.exception(
                "Error retrieving destination_collection_id in %s",
                ENVIRONMENT_SHORT_NAME,
            )
            self.archive_df["destination_collection_id"] = 29

        return (
            self.archive_df.drop("collection_id", axis=1)
            .rename(
                columns={
                    "card_id": "id",
                    "card_name": "name",
                    "card_collection_id": "collection_id",
                }
            )
            .assign(object_type="card")[
                [
                    "id",
                    "name",
                    "object_type",
                    "collection_id",
                    "destination_collection_id",
                    "total_users",
                    "total_views",
                    "nbr_dashboards",
                    "last_execution_date",
                    "last_execution_context",
                    "total_errors",
                    "parent_folder",
                    "days_since_last_execution",
                ]
            ]
            .assign(
                destination_collection_id=lambda _df: _df[
                    "destination_collection_id"
                ].astype(int)
            )
            .sort_values("id")
            .drop_duplicates()
            .to_dict(orient="records")
        )


class MoveToArchive:
    def __init__(self, movement, metabase):
        self.id = movement["id"]
        self.name = movement["name"]
        self.destination_collection = movement["destination_collection_id"]
        self.collection_id = movement["collection_id"]
        self.last_execution_date = movement.get("last_execution_date")
        self.last_execution_context = movement.get("last_execution_context")
        self.parent_folder = movement.get("parent_folder")
        self.metabase = metabase

    def rename_archive_object(self):
        """Add '[Archive] - ' prefix to card name if not already present."""
        if re.search("archive", self.name, re.IGNORECASE):
            archive_name = self.name
        else:
            archive_name = "[Archive] - " + self.name

        return self.metabase.put_card(self.id, {"name": archive_name})

    def move_object(self):
        """Move card to the archive collection. Returns log entry dict."""
        result = self.metabase.update_card_collections(
            [self.id], self.destination_collection
        )

        status = "success" if result.get("status") == "ok" else result.get("status")

        log_entry = {
            "id": self.id,
            "object_type": "card",
            "status": status,
            "new_collection_id": self.destination_collection,
            "previous_collection_id": self.collection_id,
            "archived_at": pd.Timestamp.now(),
            "last_execution_date": self.last_execution_date,
            "last_execution_context": self.last_execution_context,
            "parent_folder": self.parent_folder,
        }

        if status == "success":
            logger.info(
                "Moved card %s to collection %s", self.id, self.destination_collection
            )
        else:
            logger.error("Failed to move card %s: status=%s", self.id, status)

        return log_entry

    def save_logs_bq(self, log_entry):
        """Save the movement log in a BQ table."""
        pd.DataFrame([log_entry]).to_gbq(
            f"{PROJECT_NAME}.{INT_METABASE_DATASET}.archiving_log",
            project_id=PROJECT_NAME,
            if_exists="append",
        )
        logger.info("Logs saved to BQ for card %s", self.id)


def archive_dead_dashboards(metabase, root_collection_ids):
    """Archive dashboards that are empty or contain only archived cards.

    Scans collections recursively under the given roots.

    Returns:
        list of archived dashboard IDs
    """
    archived = []

    for root_id in root_collection_ids:
        archived.extend(_scan_dashboards_recursive(metabase, root_id))

    if archived:
        logger.info("Archived %d dead dashboard(s): %s", len(archived), archived)
    else:
        logger.info("No dead dashboards found")

    return archived


def _scan_dashboards_recursive(metabase, collection_id):
    """Scan a collection and its children for dead dashboards."""
    archived = []

    response = metabase.get_collection_children(collection_id)
    items = response.get("data", [])

    for item in items:
        if item.get("model") == "collection":
            archived.extend(_scan_dashboards_recursive(metabase, item["id"]))
        elif item.get("model") == "dashboard":
            if _is_dashboard_dead(metabase, item["id"]):
                logger.info("Archiving dead dashboard %s", item["id"])
                metabase.put_dashboard(item["id"], {"archived": True})
                archived.append(item["id"])

    return archived


def _is_dashboard_dead(metabase, dashboard_id):
    """A dashboard is dead if it has no cards or all its cards are archived."""
    dashboard = metabase.get_dashboards(dashboard_id)
    cards = dashboard.get("ordered_cards", [])

    if not cards:
        return True

    return all(
        card.get("card", {}).get("archived", False)
        for card in cards
        if card.get("card") is not None
    )


def _is_collection_empty(metabase, collection_id):
    """Check if a collection has no items (cards, dashboards, sub-collections)."""
    response = metabase.get_collection_children(collection_id)
    items = response.get("data", [])
    return len(items) == 0


def archive_empty_collections(metabase, root_collection_ids, exclude_ids=None):
    """Recursively archive empty collections under given root collections.

    Walks bottom-up: archives leaf empty collections first, then checks
    if their parent became empty too.

    Args:
        metabase: MetabaseAPI instance
        root_collection_ids: list of collection IDs to scan
        exclude_ids: set of collection IDs to never archive (roots themselves)

    Returns:
        list of archived collection IDs
    """
    if exclude_ids is None:
        exclude_ids = set(root_collection_ids)

    archived = []

    for root_id in root_collection_ids:
        archived.extend(_archive_empty_recursive(metabase, root_id, exclude_ids))

    if archived:
        logger.info("Archived %d empty collection(s): %s", len(archived), archived)
    else:
        logger.info("No empty collections found")

    return archived


def _archive_empty_recursive(metabase, collection_id, exclude_ids):
    """Depth-first: process children first, then check if this one is empty."""
    archived = []

    response = metabase.get_collection_children(collection_id, models=["collection"])
    children = response.get("data", [])

    for child in children:
        if child.get("model") == "collection":
            child_id = child["id"]
            archived.extend(_archive_empty_recursive(metabase, child_id, exclude_ids))

    if collection_id in exclude_ids:
        return archived

    if _is_collection_empty(metabase, collection_id):
        logger.info("Archiving empty collection %s", collection_id)
        metabase.put_collection(collection_id, {"archived": True})
        archived.append(collection_id)

    return archived
