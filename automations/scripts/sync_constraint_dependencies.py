from pathlib import Path

import typer
from constraints import load_constraint_dependencies, sync_one

app = typer.Typer()

BASE_PATH = Path(__file__).resolve().parent.parent.parent
SKIP_PARTS = {".venv", "templates", "node_modules"}


def iter_pyproject_paths(glob_pattern: str) -> list[Path]:
    return sorted(
        p for p in BASE_PATH.glob(glob_pattern) if not SKIP_PARTS.intersection(p.parts)
    )


def resolve_module_path(module: str) -> Path:
    path = (BASE_PATH / module / "pyproject.toml").resolve()
    if not path.is_file():
        raise typer.BadParameter(f"No pyproject.toml found at {module}")
    return path


@app.command()
def sync(
    all_: bool = typer.Option(
        False, "--all", help="Sync every pyproject.toml in the repo."
    ),
    ml: bool = typer.Option(
        False, "--ml", help="Sync only jobs/ml_jobs/**/pyproject.toml."
    ),
    etl: bool = typer.Option(
        False, "--etl", help="Sync only jobs/etl_jobs/**/pyproject.toml."
    ),
    module: str = typer.Option(
        None,
        "--module",
        help="Sync only this one job, e.g. --module jobs/ml_jobs/finance.",
    ),
    dry_run: bool = typer.Option(
        True, help="Preview changes without writing any file."
    ),
) -> None:
    """Add any missing shared security constraints to job pyproject.toml files.

    Additive only: a package a job's own [tool.uv] constraint-dependencies
    already lists is left untouched, whatever version it pins — only
    packages missing from that list get the shared floor appended.

    Exactly one of --all, --ml, --etl, or --module must be given.
    """
    scopes = [
        name for name, on in (("--all", all_), ("--ml", ml), ("--etl", etl)) if on
    ]
    if module:
        scopes.append("--module")
    if len(scopes) != 1:
        raise typer.BadParameter(
            "Pass exactly one of --all, --ml, --etl, or --module "
            f"(got: {', '.join(scopes) or 'none'})."
        )

    if module:
        paths = [resolve_module_path(module)]
    elif ml:
        paths = iter_pyproject_paths("jobs/ml_jobs/**/pyproject.toml")
    elif etl:
        paths = iter_pyproject_paths("jobs/etl_jobs/**/pyproject.toml")
    else:
        paths = iter_pyproject_paths("**/pyproject.toml")

    constraints = load_constraint_dependencies()

    changed: list[tuple[Path, list[str]]] = []
    for path in paths:
        added = sync_one(path, constraints, dry_run)
        if added:
            changed.append((path, added))

    if not changed:
        print(
            "Nothing to sync — every pyproject.toml already covers the shared constraints."
        )
        return

    prefix = "[dry-run] " if dry_run else ""
    for path, added in changed:
        rel = path.relative_to(BASE_PATH)
        print(f"{prefix}{rel}: +{len(added)} constraint(s)")
        for spec in added:
            print(f"    {spec}")

    if dry_run:
        print(
            f"\n{len(changed)} file(s) would change. Re-run with --no-dry-run to apply."
        )
    else:
        print(f"\nUpdated {len(changed)} file(s).")


if __name__ == "__main__":
    app()
