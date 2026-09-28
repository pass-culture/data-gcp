from pathlib import Path

import tomlkit
import yaml
from packaging.requirements import Requirement

CONSTRAINTS_CONFIG_PATH = (
    Path(__file__).resolve().parent.parent / "configs" / "constraint_dependencies.yaml"
)


def load_constraint_dependencies() -> list[dict]:
    """Load the shared org-wide constraint-dependencies list.

    Each entry is a dict with a `spec` (PEP 508 requirement string) and a
    `reason` (why the floor exists, rendered as a trailing TOML comment).
    """
    config = yaml.safe_load(CONSTRAINTS_CONFIG_PATH.read_text())
    return config["constraints"]


def package_name(spec: str) -> str:
    return Requirement(spec).name.lower()


def sync_one(path: Path, constraints: list[dict], dry_run: bool) -> list[str]:
    """Add any constraints missing from `path`'s [tool.uv] constraint-dependencies.

    Additive only: a package already listed (at any version) is left
    untouched. Preserves all existing formatting/comments via tomlkit.
    Returns the list of specs that were (or, if dry_run, would be) added.
    """
    doc = tomlkit.parse(path.read_text())

    tool_table = doc.get("tool")
    if tool_table is None:
        tool_table = tomlkit.table()
        doc["tool"] = tool_table

    uv_table = tool_table.get("uv")
    if uv_table is None:
        uv_table = tomlkit.table()
        tool_table["uv"] = uv_table

    existing = uv_table.get("constraint-dependencies")
    existing_names = set()
    if existing is not None:
        for item in existing:
            try:
                existing_names.add(package_name(str(item)))
            except Exception:
                continue
    else:
        existing = tomlkit.array()
        existing.multiline(True)

    added = []
    for constraint in constraints:
        name = package_name(constraint["spec"])
        if name in existing_names:
            continue
        existing.add_line(constraint["spec"], comment=constraint["reason"])
        added.append(constraint["spec"])

    if added:
        uv_table["constraint-dependencies"] = existing
        if not dry_run:
            path.write_text(tomlkit.dumps(doc))

    return added
