"""
Interactive tree of the GTL book hierarchy with item counts.

Item counts per GTL label path (queries/gtl_counts.sql, cached as CSV) are
turned into a nested tree, then written into a single self-contained HTML
page (templates/gtl_tree.html) with two views:

  #arbre    (default) node-link tree, root "Livres" linked to every level-1
            label, force-directed (elastic) layout; needs internet to load D3
  #tableau  collapsible indented table (works offline)

Each node shows its subtree total. Items labelled at a node itself (no deeper
label) appear as an explicit "(sans sous-genre)" child.
"""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

import pandas as pd
from loguru import logger

from src.config import GCP_PROJECT_ID, GTL_LEVELS, ML_INPUT_DATASET

GTL_COUNTS_QUERY = (Path(__file__).parent / "queries" / "gtl_counts.sql").read_text()
TEMPLATE = Path(__file__).parent / "templates" / "gtl_tree.html"
DATA_PLACEHOLDER = "__DATA__"

ROOT_NAME = "Livres"
OWN_ITEMS_NAME = "(sans sous-genre)"


def load_gtl_counts(
    cache: Path | None,
    project: str = GCP_PROJECT_ID,
    ml_input_dataset: str = ML_INPUT_DATASET,
    *,
    refresh: bool = False,
) -> pd.DataFrame:
    """Item counts per (titelive_gtl_id, GTL label path), cached as CSV
    (small: one row per GTL id)."""
    if cache and cache.exists() and not refresh:
        logger.info(f"Loading cached GTL counts from {cache}")
        return pd.read_csv(cache, dtype={"titelive_gtl_id": str})
    from google.cloud import bigquery

    logger.info(f"Querying GTL counts from {project}.{ml_input_dataset}")
    query = GTL_COUNTS_QUERY.format(project=project, ml_input_dataset=ml_input_dataset)
    df = bigquery.Client(project=project).query(query).to_dataframe()
    if cache:
        cache.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(cache, index=False)
    return df


def label_path(labels) -> tuple[str, ...]:
    """Labels from level 1 down, stopping at the first missing/empty one."""
    path = []
    for label in labels:
        if pd.isna(label) or str(label).strip() == "":
            break
        path.append(str(label))
    return tuple(path)


def build_tree(df: pd.DataFrame) -> dict:
    """Nested {name, total, own, paths, children[, gtl, pseudo]} tree, rooted
    at "Livres". `total` counts the whole subtree; items labelled at a
    non-leaf node itself become an OWN_ITEMS_NAME pseudo child."""
    own: dict[tuple, int] = defaultdict(int)
    gtl_ids: dict[tuple, set] = defaultdict(set)
    no_level_1 = 0
    for row in df.itertuples(index=False):
        path = label_path([getattr(row, level) for level in GTL_LEVELS])
        if path:
            own[path] += int(row.n_items)
            gtl_ids[path].add(str(row.titelive_gtl_id))
        else:
            no_level_1 += int(row.n_items)
    if no_level_1:
        logger.warning(f"{no_level_1} items have no level-1 label: not in the tree")

    nodes: dict[tuple, dict] = {(): {"name": ROOT_NAME, "own": 0, "children": {}}}
    for path in own:
        for depth in range(1, len(path) + 1):
            nodes.setdefault(
                path[:depth], {"name": path[depth - 1], "own": 0, "children": {}}
            )
            nodes[path[: depth - 1]]["children"][path[depth - 1]] = nodes[path[:depth]]
        nodes[path]["own"] = own[path]
        nodes[path]["gtl"] = sorted(gtl_ids[path])

    def finish(node: dict) -> dict:
        kids = [finish(child) for child in node["children"].values()]
        if kids and node["own"]:
            kids.append(
                {
                    "name": OWN_ITEMS_NAME,
                    "total": node["own"],
                    "own": node["own"],
                    "paths": 1,
                    "children": [],
                    "pseudo": True,
                    "gtl": node.get("gtl", []),
                }
            )
        out = {
            "name": node["name"],
            "total": sum(k["total"] for k in kids) if kids else node["own"],
            "own": node["own"],
            "paths": sum(k["paths"] for k in kids) if kids else 1,
            "children": kids,
        }
        if not kids and node.get("gtl"):
            out["gtl"] = node["gtl"]
        return out

    tree = finish(nodes[()])
    check_totals(tree, own)
    return tree


def check_totals(tree: dict, own: dict[tuple, int]) -> None:
    """Independent check: each node's total must equal the sum of all items
    whose label path starts with that node's path (all sublevels, incl. null
    sublevels). Raises ValueError otherwise."""
    expected: dict[tuple, int] = defaultdict(int)
    for path, n in own.items():
        for depth in range(len(path) + 1):
            expected[path[:depth]] += n
    errors = []

    def walk(node: dict, path: tuple) -> None:
        if expected[path] != node["total"]:
            name = " > ".join(path) or ROOT_NAME
            errors.append(f"{name}: shown {node['total']}, expected {expected[path]}")
        for child in node["children"]:
            if not child.get("pseudo"):
                walk(child, (*path, child["name"]))

    walk(tree, ())
    if errors:
        details = "; ".join(errors[:10])
        raise ValueError(f"{len(errors)} node totals are wrong: {details}")
    logger.info(f"All {len(expected)} node totals include their full subtree")


def render_html(tree: dict) -> str:
    """The page, with the tree embedded as JSON."""
    data = json.dumps(tree, ensure_ascii=False, separators=(",", ":"))
    data = data.replace("</", "<\\/")  # a label can't close the <script>
    return TEMPLATE.read_text(encoding="utf-8").replace(DATA_PLACEHOLDER, data)


def write_gtl_tree(df: pd.DataFrame, out: Path) -> dict:
    """Build the tree from GTL counts and write the page to `out`."""
    tree = build_tree(df)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(render_html(tree), encoding="utf-8")
    logger.info(
        f"Wrote {out}: {tree['total']} items, {len(tree['children'])} level-1 "
        f"labels, {tree['paths']} label paths"
    )
    return tree
