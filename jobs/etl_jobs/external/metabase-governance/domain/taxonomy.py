import logging
import re

import pandas as pd

from core.utils import PROJECT_NAME, RAW_METABASE_DATASET

logger = logging.getLogger(__name__)

COLLECTION_TABLE = "metabase_collection"
TAXONOMY_TABLE = "collection_taxonomy"


def _like_to_regex(pattern):
    """Translate a SQL-LIKE pattern (`%`, `_`) to a case-insensitive regex.

    Mirrors the helper used by the archiving rules so tier patterns behave
    identically to the archiving `ancestor_title_pattern` ones.
    """
    out = []
    for ch in pattern:
        if ch == "%":
            out.append(".*")
        elif ch == "_":
            out.append(".")
        else:
            out.append(re.escape(ch))
    return re.compile("".join(out), re.IGNORECASE)


def _parse_location_ids(location):
    """Return the ordered ancestor ids encoded in a Metabase `location` path.

    e.g. '/608/622/' -> [608, 622]; root collections have location '/' -> [].
    """
    if not isinstance(location, str):
        return []
    return [int(part) for part in location.strip("/").split("/") if part]


def load_collections(dataset=None, table=None):
    """Load the collection tree from the raw Metabase replication."""
    dataset = dataset or RAW_METABASE_DATASET
    table = table or COLLECTION_TABLE
    query = f"""
        SELECT collection_id, collection_name, location, archived, personal_owner_id
        FROM {dataset}.{table}
    """
    return pd.read_gbq(query, project_id=PROJECT_NAME, use_bqstorage_api=False)


def build_taxonomy(collections_df, config):
    """Resolve every collection to {squad, tier, certified, in_scope, is_excluded}
    by walking its ancestor chain (ids from `location` + the collection itself).

    - squad: the configured squad collection present in the ancestor chain
      (squads are the depth-1 children of the in-scope root).
    - tier: the *deepest* ancestor whose title matches a `tier_rules` pattern.
    - certified: the certified ancestor id is in the chain.
    - in_scope: root is in-scope (whitelist) AND collection is neither archived
      nor personal.
    - is_excluded: collection is archived or personal — the governance signal
      that an in-scope-rooted collection should still not be surfaced.
    """
    in_scope_roots = set(config.get("in_scope_root_collection_ids", []))
    certified_id = config.get("certified_ancestor_collection_id")
    squad_by_id = {s["collection_id"]: s["label"] for s in config.get("squads", [])}
    tier_matchers = [
        (rule["tier"], _like_to_regex(rule["ancestor_title_pattern"]))
        for rule in config.get("tier_rules", [])
    ]

    name_by_id = dict(
        zip(collections_df["collection_id"], collections_df["collection_name"])
    )

    rows = []
    for row in collections_df.itertuples(index=False):
        collection_id = int(row.collection_id)
        ancestor_ids = _parse_location_ids(row.location) + [collection_id]
        root_id = ancestor_ids[0]

        is_personal = pd.notna(row.personal_owner_id)
        is_archived = bool(row.archived)

        squad = next(
            (squad_by_id[aid] for aid in ancestor_ids if aid in squad_by_id), None
        )

        tier = None
        for aid in reversed(ancestor_ids):
            title = name_by_id.get(aid)
            if not isinstance(title, str):
                continue
            matched = next((t for t, rgx in tier_matchers if rgx.search(title)), None)
            if matched is not None:
                tier = matched
                break

        certified = certified_id is not None and certified_id in ancestor_ids
        in_scope = root_id in in_scope_roots and not is_archived and not is_personal
        is_excluded = is_archived or is_personal

        rows.append(
            {
                "collection_id": collection_id,
                "collection_name": row.collection_name,
                "location": row.location,
                "root_collection_id": root_id,
                "squad": squad,
                "tier": tier,
                "certified": certified,
                "in_scope": in_scope,
                "is_excluded": is_excluded,
            }
        )

    return pd.DataFrame(rows)


def run_taxonomy(
    config, dataset=None, table=None, destination_dataset=None, destination_table=None
):
    """Resolve the collection taxonomy and write it to BigQuery."""
    collections_df = load_collections(dataset=dataset, table=table)
    logger.info("Loaded %d collections from raw", len(collections_df))

    taxonomy_df = build_taxonomy(collections_df, config)
    logger.info(
        "Taxonomy resolved: %d in-scope, %d certified, %d tiered (of %d total)",
        int(taxonomy_df["in_scope"].sum()),
        int(taxonomy_df["certified"].sum()),
        int(taxonomy_df["tier"].notna().sum()),
        len(taxonomy_df),
    )

    destination_dataset = destination_dataset or RAW_METABASE_DATASET
    destination_table = destination_table or TAXONOMY_TABLE
    destination = f"{destination_dataset}.{destination_table}"
    taxonomy_df.to_gbq(destination, project_id=PROJECT_NAME, if_exists="replace")
    logger.info("Wrote %s.%s", PROJECT_NAME, destination)

    return "success"
