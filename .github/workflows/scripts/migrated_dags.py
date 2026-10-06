import sys
from pathlib import Path

import typer
import yaml

app = typer.Typer()

MIGRATED_DAGS_KEY = "dag_in_paris"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _load_migrated_dags() -> list[str]:
    """Read the list of migrated DAGs from migrated_dags.yaml."""
    yaml_path = _repo_root() / "orchestration" / "scripts" / "migrated_dags.yaml"

    try:
        with open(yaml_path) as stream:
            config = yaml.safe_load(stream) or {}
    except FileNotFoundError:
        print(f"Error: File not found at {yaml_path}", file=sys.stderr)
        raise typer.Exit(code=1)
    except yaml.YAMLError as exc:
        print(f"YAML Error: {exc}", file=sys.stderr)
        raise typer.Exit(code=1)

    if MIGRATED_DAGS_KEY not in config:
        print(
            f"Error: '{MIGRATED_DAGS_KEY}' key not found in migrated_dags.yaml",
            file=sys.stderr,
        )
        raise typer.Exit(code=1)

    return config[MIGRATED_DAGS_KEY]


@app.command()
def get_migrated_paths(
    jobs_dir: str = typer.Option(
        "dags/jobs",
        help="Path to the DAG jobs directory to scan.",
    ),
):
    """
    Print the file paths of migrated DAGs found under jobs_dir.
    Only these files are synced to the EU bucket.
    """
    migrated = set(_load_migrated_dags())
    source_dir = Path(jobs_dir)

    migrated_paths = set()
    for path in source_dir.rglob("*.py"):
        if path.stem in migrated:
            migrated_paths.add(path.stem)
            print(path.as_posix())

    for missing in sorted(migrated - migrated_paths):
        print(
            f"Warning: migrated DAG '{missing}' has no file under {jobs_dir}",
            file=sys.stderr,
        )


if __name__ == "__main__":
    app()
