"""Write a BigQuery DDL of the dbt models from target/catalog.json, for Cassis.

The pass Culture Cassis projects read each column's BigQuery description (written by dbt's persist_docs) from
their stored source schema. The main project has no warehouse connection, so data-ontology pushes that schema
from this DDL (its nightly cassis-schema-refresh workflow downloads the `cassis-schema-ddl` artifact).

Only what the dbt project already makes public goes in: table and column names, types, descriptions. No row
counts, sizes, owners or labels.

    python scripts/catalog_to_ddl.py target/catalog.json > target/cassis_schema.sql
"""

import json
import sys


def literal(text: str) -> str:
    return json.dumps(text, ensure_ascii=False)


def statement(node: dict) -> str:
    meta = node["metadata"]
    columns = sorted(node["columns"].values(), key=lambda c: c["index"])
    lines = []
    for col in columns:
        if "." in col["name"]:  # a STRUCT's sub-field: carried by its parent's type
            continue
        options = (
            f" OPTIONS(description={literal(col['comment'])})"
            if col.get("comment")
            else ""
        )
        lines.append(f"  `{col['name']}` {col['type']}{options}")
    options = (
        f"\nOPTIONS(description={literal(meta['comment'])})"
        if meta.get("comment")
        else ""
    )
    name = f"`{meta['database']}.{meta['schema']}.{meta['name']}`"
    return f"CREATE TABLE {name}\n(\n" + ",\n".join(lines) + f"\n){options};"


def main() -> int:
    catalog = json.load(open(sys.argv[1]))
    nodes = [
        n
        for key, n in catalog["nodes"].items()
        if key.startswith(("model.", "snapshot."))
    ]
    nodes.sort(key=lambda n: (n["metadata"]["schema"], n["metadata"]["name"]))
    print("\n\n".join(statement(n) for n in nodes if n["columns"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
