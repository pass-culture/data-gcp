import sys
from pathlib import Path

import typer
import yaml

app = typer.Typer()

MIGRATED_DAGS_KEY = "dags_in_paris"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _load_migrated_dags() -> dict[str, dict]:
    """Read the migrated DAGs mapping from migrated_dags.yaml.

    Returns a dict keyed by DAG name. Each value is the entry config, which may
    optionally declare a ``file`` path.
    """
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

    return {
        name: (cfg or {}) for name, cfg in (config[MIGRATED_DAGS_KEY] or {}).items()
    }


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
    migrated = _load_migrated_dags()
    source_dir = Path(jobs_dir)

    resolved_names: set[str] = set()
    printed_files: set[str] = set()

    def _emit(path: Path) -> None:
        posix = path.as_posix()
        if posix not in printed_files:
            printed_files.add(posix)
            print(posix)

    for name, cfg in migrated.items():
        explicit = cfg.get("file")
        if not explicit:
            continue
        file_path = Path(str(explicit))
        if not file_path.exists():
            print(
                f"Warning: migrated DAG '{name}' declares file '{explicit}' "
                "which does not exist",
                file=sys.stderr,
            )
            continue
        resolved_names.add(name)
        _emit(file_path)

    for path in source_dir.rglob("*.py"):
        if path.stem in migrated and path.stem not in resolved_names:
            resolved_names.add(path.stem)
            _emit(path)

    for missing in sorted(set(migrated) - resolved_names):
        print(
            f"Warning: migrated DAG '{missing}' has no file under {jobs_dir}",
            file=sys.stderr,
        )


if __name__ == "__main__":
    app()
