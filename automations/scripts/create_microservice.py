import re
import shutil
import subprocess
from enum import Enum
from pathlib import Path

import typer
import yaml
from constraints import load_constraint_dependencies, sync_one

app = typer.Typer()

SCAFFOLD_CONFIG_PATH = (
    Path(__file__).resolve().parent.parent / "configs" / "microservice_scaffold.yaml"
)


def is_snake_case(s: str) -> bool:
    return re.fullmatch(r"^[a-z0-9_]+$", s) is not None


def set_ruff_target_version(ruff_config: str, python_version: str) -> str:
    """Rewrite the template's target-version to match the scaffold's python_version."""
    ruff_target = "py" + python_version.replace(".", "")
    return re.sub(
        r"target-version = 'py\d+'", f"target-version = '{ruff_target}'", ruff_config
    )


def set_requires_python(pyproject_text: str, python_version: str) -> str:
    """Pin requires-python to the exact minor series: >=X.Y,<X.(Y+1).

    `uv init -p X.Y` only writes `>=X.Y` (no upper bound), which would let a
    future `uv lock` silently resolve onto a newer minor version.
    """
    major, minor, *_ = python_version.split(".")
    requires_python = f">={major}.{minor},<{major}.{int(minor) + 1}"
    return re.sub(
        r'requires-python = ".*"',
        f'requires-python = "{requires_python}"',
        pyproject_text,
        count=1,
    )


class MicroServiceType(str, Enum):
    ml = "ml"
    etl_external = "etl_external"
    etl_internal = "etl_internal"


def load_scaffold_config(ms_type: MicroServiceType, python_version: str = None) -> dict:
    config = yaml.safe_load(SCAFFOLD_CONFIG_PATH.read_text())
    entry = config[ms_type.value]
    entry["dependencies"] = config["common"]["dependencies"] + entry.pop(
        "ms_dependencies"
    )
    entry["python_version"] = python_version or config["common"]["python_version"]
    return entry


@app.command()
def create_micro_service(
    ms_name: str = typer.Option(),
    ms_type: MicroServiceType = typer.Option(case_sensitive=False),
    python_version: str = typer.Option(
        None,
        "--python-version",
        help="Overrides common.python_version from microservice_scaffold.yaml.",
    ),
):
    """
    Create a micro-service with the given name.

    Args:
        ms_name (str): The name of the micro-service. Must be in snake_case.
        ms_type (MicroServiceType): The type of the micro-service. Must be one of "ml", "etl_external", or "etl_internal".
        python_version (str): Python version for the new microservice's venv.
            Defaults to common.python_version in microservice_scaffold.yaml.

    Raises:
        ValueError: If the name is not in snake_case.

    Returns:
        None
    """

    # test if name is snake_case
    if not is_snake_case(ms_name):
        raise ValueError("ms_name must be snake_case")

    scaffold = load_scaffold_config(ms_type, python_version)
    destination_dir = Path(scaffold["destination"].format(ms_name=ms_name))

    # Copying template directory to destination directory
    ignore_patterns = "__pycache__", ".pytest_cache", ".vscode", ".ruff_cache"
    shutil.copytree(
        scaffold["template_dir"],
        destination_dir,
        ignore=shutil.ignore_patterns(*ignore_patterns),
    )

    subprocess.run(
        ["uv", "init", "--no-workspace", "-p", scaffold["python_version"]],
        cwd=destination_dir,
        check=True,
    )

    pyproject_path = destination_dir / "pyproject.toml"
    pyproject_path.write_text(
        set_requires_python(pyproject_path.read_text(), scaffold["python_version"])
    )

    subprocess.run(
        ["uv", "add", *scaffold["dependencies"]], cwd=destination_dir, check=True
    )
    subprocess.run(
        ["uv", "add", "--dev", *scaffold["dev_dependencies"]],
        cwd=destination_dir,
        check=True,
    )

    sync_one(pyproject_path, load_constraint_dependencies(), dry_run=False)

    ruff_template_path = destination_dir / "pyproject.toml.template"
    ruff_config = set_ruff_target_version(
        ruff_template_path.read_text(), scaffold["python_version"]
    )
    with pyproject_path.open("a") as pyproject_file:
        pyproject_file.write(ruff_config)
    ruff_template_path.unlink()

    subprocess.run(["uv", "sync"], cwd=destination_dir, check=True)

    # .gitignore excludes /data wholesale, and git won't re-include a file
    # inside an already-ignored parent directory — force-add the placeholder
    # once here so it stays tracked without anyone needing to remember `-f`.
    subprocess.run(
        ["git", "add", "-f", "data/.gitkeep"], cwd=destination_dir, check=True
    )


if __name__ == "__main__":
    app()
