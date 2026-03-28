import logging
from pathlib import Path

import yaml

from core.utils import ENVIRONMENT_SHORT_NAME

logger = logging.getLogger(__name__)

PERMISSIONS_CONFIG_DIR = Path(__file__).parent.parent / "config" / "permissions"

ENV_TO_CONFIG = {
    "dev": "staging.yaml",
    "stg": "staging.yaml",
    "prod": "production.yaml",
}


def get_config_path():
    """Resolve the permissions config file path based on ENV_SHORT_NAME."""
    config_file = ENV_TO_CONFIG.get(ENVIRONMENT_SHORT_NAME)
    if not config_file:
        raise ValueError(
            f"No permissions config for environment '{ENVIRONMENT_SHORT_NAME}'"
        )
    return PERMISSIONS_CONFIG_DIR / config_file


def load_permissions_config(yaml_path=None):
    """Load the permissions YAML config file."""
    if yaml_path is None:
        yaml_path = get_config_path()
    logger.info("Loading permissions config from %s", yaml_path)
    with open(yaml_path) as f:
        return yaml.safe_load(f)


def get_all_subcollections(metabase, collection_id):
    """Recursively list all sub-collection IDs under a given collection."""
    subcollection_ids = []
    response = metabase.get_collection_children(collection_id, models=["collection"])
    children = response.get("data", [])

    for child in children:
        if child.get("model") == "collection":
            child_id = child["id"]
            subcollection_ids.append(child_id)
            subcollection_ids.extend(get_all_subcollections(metabase, child_id))

    return subcollection_ids


def _apply_permissions_to_graph(graph, collection_id, permissions):
    """Apply a set of group permissions to a single collection in the graph.

    Returns the list of changes made.
    """
    changes = []
    str_collection_id = str(collection_id)

    for group_id, expected_permission in permissions.items():
        str_group_id = str(group_id)

        if str_group_id not in graph:
            raise ValueError(f"Group {group_id} not found in Metabase permission graph")

        current_permission = graph[str_group_id].get(str_collection_id)

        if current_permission != expected_permission:
            changes.append(
                {
                    "collection_id": collection_id,
                    "group_id": group_id,
                    "old": current_permission,
                    "new": expected_permission,
                }
            )
            graph[str_group_id][str_collection_id] = expected_permission

    return changes


def _process_collection(collection_name, collection_config, current_graph, metabase):
    """Process a single collection node and its children recursively.

    Order matters:
    1. Apply this collection's permissions
    2. enforce_on_children → blanket apply to ALL sub-collections via API
    3. Explicit children override → applied AFTER, so exceptions win
    """
    all_changes = []
    collection_id = collection_config["id"]
    permissions = collection_config.get("permissions", {})

    logger.info("Processing collection '%s' (id=%s)", collection_name, collection_id)

    # 1. Apply permissions to this collection
    changes = _apply_permissions_to_graph(current_graph, collection_id, permissions)
    all_changes.extend(changes)

    # 2. If enforce_on_children, apply same permissions to ALL sub-collections
    if collection_config.get("enforce_on_children", False):
        subcollections = get_all_subcollections(metabase, collection_id)
        logger.info(
            "Enforcing permissions on %d sub-collections of '%s'",
            len(subcollections),
            collection_name,
        )
        for sub_id in subcollections:
            changes = _apply_permissions_to_graph(current_graph, sub_id, permissions)
            all_changes.extend(changes)

    # 3. Process explicit children (overrides enforce_on_children for these)
    for child_name, child_config in collection_config.get("children", {}).items():
        changes = _process_collection(child_name, child_config, current_graph, metabase)
        all_changes.extend(changes)

    return all_changes


def build_expected_graph(config, current_graph, metabase):
    """Build the expected permission graph from the YAML config.

    Modifies current_graph in place and returns a list of all changes.
    """
    all_changes = []

    for collection_name, collection_config in config["collections"].items():
        changes = _process_collection(
            collection_name, collection_config, current_graph, metabase
        )
        all_changes.extend(changes)

    return all_changes


def sync_permissions(metabase, yaml_path=None, dry_run=False):
    """Synchronize Metabase collection permissions from the YAML config.

    Returns a summary dict with changes made.
    """
    config = load_permissions_config(yaml_path)

    logger.info("Fetching current collection permission graph...")
    graph = metabase.get_collection_graph()
    revision = graph.get("revision")
    permission_graph = graph.get("groups", {})

    changes = build_expected_graph(config, permission_graph, metabase)

    if not changes:
        logger.info("No permission changes needed.")
        return {"changes": [], "applied": False}

    logger.info("Found %d permission change(s):", len(changes))
    for change in changes:
        logger.info(
            "  Collection %s, Group %s: %s → %s",
            change["collection_id"],
            change["group_id"],
            change["old"],
            change["new"],
        )

    if dry_run:
        logger.info("Dry-run mode: no changes applied.")
        return {"changes": changes, "applied": False}

    logger.info("Applying permission changes...")
    updated_graph = {"revision": revision, "groups": permission_graph}
    metabase.put_collection_graph(updated_graph)
    logger.info("Permissions synchronized successfully.")

    return {"changes": changes, "applied": True}
