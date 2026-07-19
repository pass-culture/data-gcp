#!/usr/bin/env python3
"""Structural test-coverage audit for this monorepo.

Reports which source modules / dbt models have NO test at all — the fastest
way to surface coverage gaps before writing tests. This is a structural check
(file/model -> test existence), not line coverage; for Python line coverage run
pytest-cov as printed at the end of the Python report.

Usage:
    python audit_coverage.py <path>

The domain is auto-detected from <path>:
  - a dbt models tree (contains "data_gcp_dbt") -> dbt model audit
  - anything else                               -> Python pytest audit

Exit code is 0 on success regardless of gaps found (gaps are a report, not an
error); non-zero only on bad input or an unreadable tree.
"""

import sys
from pathlib import Path

# Directories that never contain source-under-test.
SKIP_DIRS = {
    ".venv",
    "__pycache__",
    ".ruff_cache",
    ".pytest_cache",
    "target",
    "build",
    "dist",
    ".git",
    "node_modules",
}
# __init__.py is almost always empty packaging glue, not behavior worth a test.
SKIP_PY_FILES = {"__init__.py"}


def _iter_files(root: Path, suffix: str):
    """Yield files with `suffix` under `root`, skipping vendored/build dirs."""
    for path in root.rglob(f"*{suffix}"):
        if any(part in SKIP_DIRS for part in path.relative_to(root).parts):
            continue
        yield path


# --------------------------------------------------------------------------- #
# Python microservices (pytest)
# --------------------------------------------------------------------------- #
def audit_python(root: Path) -> None:
    """Map source modules to test files under any tests/ dir.

    A module foo.py is considered covered if a test file named foo_test.py
    (ml_jobs style) or test_foo.py (etl_jobs style) exists anywhere under root.
    """
    test_stems = set()
    source_files = []
    for py in _iter_files(root, ".py"):
        if py.name in SKIP_PY_FILES:
            continue
        in_tests = "tests" in py.relative_to(root).parts
        if in_tests:
            stem = py.stem
            # Normalize both naming conventions to the module name.
            if stem.endswith("_test"):
                test_stems.add(stem[: -len("_test")])
            elif stem.startswith("test_"):
                test_stems.add(stem[len("test_") :])
        else:
            source_files.append(py)

    if not source_files:
        print(f"No Python source modules found under {root}.")
        return

    covered, gaps = [], []
    for src in sorted(source_files):
        (covered if src.stem in test_stems else gaps).append(src)

    total = len(source_files)
    print(f"\n=== Python coverage audit: {root} ===")
    print(
        f"Source modules: {total} | with a test file: {len(covered)} "
        f"| MISSING tests: {len(gaps)}"
    )
    if gaps:
        print("\nModules with NO test file (gaps, ranked by path):")
        for src in gaps:
            print(f"  - {src.relative_to(root)}")
    else:
        print("\nEvery source module has a matching test file.")

    print("\nFor line-level coverage, run from the service root:")
    print("  PYTHONPATH=. pytest tests --cov=. --cov-report=term-missing")


# --------------------------------------------------------------------------- #
# dbt models
# --------------------------------------------------------------------------- #
def _names_with_tests(node, documented: set, tested: set) -> None:
    """Recursively collect every `name:` entry and whether it carries tests.

    Walks parsed YAML (models/sources/seeds/snapshots and their columns). A node
    is 'tested' if it, or any of its columns, has a `data_tests` or `tests` key.
    """
    if isinstance(node, list):
        for item in node:
            _names_with_tests(item, documented, tested)
        return
    if not isinstance(node, dict):
        return

    name = node.get("name")
    if isinstance(name, str):
        documented.add(name)
        has_own = bool(node.get("data_tests") or node.get("tests"))
        cols = node.get("columns") or []
        has_col = any(
            isinstance(c, dict) and (c.get("data_tests") or c.get("tests"))
            for c in cols
        )
        if has_own or has_col:
            tested.add(name)
    for value in node.values():
        _names_with_tests(value, documented, tested)


def audit_dbt(root: Path) -> None:
    """Report dbt models that have no data test in any schema .yml."""
    try:
        import yaml  # provided by the dbt env; run via `uv run python`
    except ImportError:
        sys.exit(
            "PyYAML not available. Run from the dbt project with: "
            "uv run python <this script> <path>"
        )

    models = {p.stem for p in _iter_files(root, ".sql")}
    if not models:
        print(f"No dbt models (.sql) found under {root}.")
        return

    documented, tested = set(), set()
    for yml in list(_iter_files(root, ".yml")) + list(_iter_files(root, ".yaml")):
        try:
            parsed = yaml.safe_load(yml.read_text())
        except yaml.YAMLError as exc:
            print(f"  ! skipping unparseable yml {yml}: {exc}")
            continue
        _names_with_tests(parsed, documented, tested)

    untested = sorted(m for m in models if m not in tested)
    undocumented = sorted(m for m in models if m not in documented)

    print(f"\n=== dbt coverage audit: {root} ===")
    print(
        f"Models: {len(models)} | with >=1 data test: {len(models) - len(untested)} "
        f"| no test: {len(untested)} | no .yml entry at all: {len(undocumented)}"
    )
    if untested:
        print("\nModels with NO data test (gaps):")
        for m in untested:
            tag = " (also has no .yml entry)" if m in undocumented else ""
            print(f"  - {m}{tag}")
    else:
        print("\nEvery model has at least one data test.")

    print("\nTo run the tests you add: uv run dbt test -s <model_name>")


def main() -> None:
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    target = Path(sys.argv[1]).resolve()
    if not target.is_dir():
        sys.exit(f"Not a directory: {target}")

    if "data_gcp_dbt" in target.parts and "models" in target.parts:
        audit_dbt(target)
    else:
        audit_python(target)


if __name__ == "__main__":
    main()
