import argparse
import json
import os
import sys
from collections import defaultdict
from pathlib import Path

import yaml


def load_config(config_path):
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def get_tier_index(folder_path, tiers_config):
    best_score = -1
    best_tier_idx = -1

    for idx, tier in enumerate(tiers_config):
        patterns = tier.get("patterns", [])
        for p in patterns:
            # Handle both object and string patterns (legacy support if any)
            if isinstance(p, str):
                value = p
                type_ = "contains"
                priority = 0
            else:
                value = p.get("value", "")
                type_ = p.get("type", "contains")
                priority = p.get("priority", 0)

            matched = False
            type_bonus = 0

            if type_ == "exact":
                if folder_path == value:
                    matched = True
                    type_bonus = 100
            elif type_ == "startswith":
                if folder_path.startswith(value):
                    matched = True
                    type_bonus = 50
            elif type_ == "endswith":
                if folder_path.endswith(value):
                    matched = True
                    type_bonus = 40
            elif type_ == "contains":
                if value in folder_path:
                    matched = True
                    type_bonus = 0

            if matched:
                # Base score: priority + type bonus + length of pattern
                # Length ensures "src.raw" (7) beats "src." (4) if priorities equal
                score = priority + type_bonus + len(value)
                if score > best_score:
                    best_score = score
                    best_tier_idx = idx

    # Default to "Silver" (index 2) or middle tier if no match
    # Assuming standard tiers: Sources(0), Bronze(1), Silver(2), ML(3), Gold(4)
    if best_tier_idx == -1:
        # Fallback logic: try to find "Silver" or "Staging"
        for idx, tier in enumerate(tiers_config):
            if "Silver" in tier["label"] or "Staging" in tier["label"]:
                return idx
        return 2  # Default fallback

    return best_tier_idx


def get_node_color(folder_path, color_mapping):
    # Check for exact match first
    if folder_path in color_mapping:
        return color_mapping[folder_path]

    # Check for startswith (longest match wins)
    best_match_len = 0
    best_color = None

    for key, color in color_mapping.items():
        if folder_path.startswith(key):
            if len(key) > best_match_len:
                best_match_len = len(key)
                best_color = color

    if best_color:
        return best_color

    # Check for contains
    for key, color in color_mapping.items():
        if key in folder_path:
            return color

    return color_mapping.get("root", "#64748b")


def parse_manifest(manifest_path, config):
    print(f"Parsing manifest: {manifest_path}")
    with open(manifest_path, "r") as f:
        manifest = json.load(f)

    nodes = manifest.get("nodes", {})
    sources = manifest.get("sources", {})

    folder_nodes = {}  # id -> {id, tier, color, count, models: []}
    node_to_folder = {}

    # Process models/seeds/snapshots/sources
    all_nodes = {**nodes, **sources}

    print(f"Found {len(all_nodes)} nodes/sources in manifest")

    for node_id, node in all_nodes.items():
        resource_type = node.get("resource_type")
        if resource_type not in ["model", "seed", "snapshot", "source"]:
            continue

        # Determine folder path
        if resource_type == "source":
            source_name = node.get("source_name")
            folder_path = f"src.{source_name}"
        else:
            original_file_path = node.get("original_file_path", "")
            # Remove 'models/' prefix and filename
            # e.g. models/staging/customers/stg_customers.sql -> staging/customers
            parts = Path(original_file_path).parts
            if parts and parts[0] == "models":
                parts = parts[1:]

            if len(parts) > 1:
                folder_path = "/".join(parts[:-1])
            elif len(parts) == 1:
                # File directly in models/
                folder_path = "root"
            else:
                folder_path = "root"

            if not folder_path:
                folder_path = "root"

        node_to_folder[node_id] = folder_path

        # Initialize folder node if needed
        if folder_path not in folder_nodes:
            tier_idx = get_tier_index(folder_path, config["tiers"])
            color = get_node_color(folder_path, config["styling"]["colors"]["mapping"])
            folder_nodes[folder_path] = {
                "id": folder_path,
                "tier": tier_idx,
                "color": color,
                "count": 0,
                "models": [],
            }

        # Add model to folder
        folder_nodes[folder_path]["count"] += 1

        # Safe access to depends_on
        depends_on = node.get("depends_on", {})
        upstream_nodes = []
        if isinstance(depends_on, dict):
            upstream_nodes = depends_on.get("nodes", [])

        folder_nodes[folder_path]["models"].append(
            {
                "name": node.get("name"),
                "type": resource_type,
                "materialization": node.get("config", {}).get("materialized", "other"),
                "path": node.get("original_file_path"),
                "description": node.get("description", ""),
                "tags": node.get("tags", []),
                "upstream_count": len(upstream_nodes),
            }
        )

    # Build edges
    links = defaultdict(int)  # (source, target) -> count

    for node_id, node in nodes.items():  # Only models/snapshots have depends_on
        if node_id not in node_to_folder:
            continue

        target_folder = node_to_folder[node_id]

        depends_on = node.get("depends_on", {})
        if not isinstance(depends_on, dict):
            continue

        for source_node_id in depends_on.get("nodes", []):
            if source_node_id in node_to_folder:
                source_folder = node_to_folder[source_node_id]

                if source_folder != target_folder:
                    links[(source_folder, target_folder)] += 1

    # Format output
    output_nodes = sorted(list(folder_nodes.values()), key=lambda x: x["id"])
    output_links = [
        {"source": s, "target": t, "value": v} for (s, t), v in links.items()
    ]

    # Build struct_links (hierarchy based on shared prefixes)
    struct_links = []
    folder_ids = sorted(list(folder_nodes.keys()))

    for i in range(len(folder_ids)):
        for j in range(i + 1, len(folder_ids)):
            f1 = folder_ids[i]
            f2 = folder_ids[j]

            # Simple shared path depth
            p1 = f1.split("/")
            p2 = f2.split("/")
            depth = 0
            for k in range(min(len(p1), len(p2))):
                if p1[k] == p2[k]:
                    depth += 1
                else:
                    break

            if depth > 0:
                struct_links.append({"source": f1, "target": f2, "depth": depth})

    return {
        "nodes": output_nodes,
        "links": output_links,
        "struct_links": struct_links,
        "tiers": config["tiers"],
        "metadata": {
            "generated_from": str(manifest_path),
            "node_count": len(output_nodes),
            "link_count": len(output_links),
        },
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate viz_data.json from dbt manifest"
    )
    parser.add_argument("--manifest", required=True, help="Path to dbt manifest.json")
    parser.add_argument("--config", required=True, help="Path to config.yaml")
    parser.add_argument("--output", required=True, help="Path to output viz_data.json")

    args = parser.parse_args()

    try:
        config = load_config(args.config)
        viz_data = parse_manifest(args.manifest, config)

        # Ensure output directory exists
        os.makedirs(os.path.dirname(args.output), exist_ok=True)

        with open(args.output, "w") as f:
            json.dump(viz_data, f, indent=2)

        print(f"Successfully generated {args.output}")
        print(f"Nodes: {len(viz_data['nodes'])}, Links: {len(viz_data['links'])}")

    except Exception as e:
        print(f"Error generating viz data: {e}")
        sys.exit(1)
