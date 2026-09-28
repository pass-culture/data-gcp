import re
import shutil
import subprocess
from enum import Enum
from pathlib import Path

import typer
import yaml

app = typer.Typer()

SCAFFOLD_CONFIG_PATH = Path(__file__).parent / "microservice_scaffold.yaml"


def is_snake_case(s: str) -> bool:
    return re.fullmatch(r"^[a-z0-9_]+$", s) is not None


class MicroServiceType(str, Enum):
    ml = "ml"
    etl_external = "etl_external"
    etl_internal = "etl_internal"


def load_scaffold_config(ms_type: MicroServiceType) -> dict:
    config = yaml.safe_load(SCAFFOLD_CONFIG_PATH.read_text())
    return config[ms_type.value]


@app.command()
def create_micro_service(
    ms_name: str = typer.Option(),
    ms_type: MicroServiceType = typer.Option(case_sensitive=False),
):
    """
    Create a micro-service with the given name.

    Args:
        ms_name (str): The name of the micro-service. Must be in snake_case.
        ms_type (MicroServiceType): The type of the micro-service. Must be one of "ml", "etl_external", or "etl_internal".

    Raises:
        ValueError: If the name is not in snake_case.

    Returns:
        None
    """

    # test if name is snake_case
    if not is_snake_case(ms_name):
        raise ValueError("ms_name must be snake_case")

    scaffold = load_scaffold_config(ms_type)
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
    subprocess.run(
        ["uv", "add", *scaffold["dependencies"]], cwd=destination_dir, check=True
    )
    subprocess.run(
        ["uv", "add", "--dev", *scaffold["dev_dependencies"]],
        cwd=destination_dir,
        check=True,
    )

    pyproject_path = destination_dir / "pyproject.toml"
    ruff_template_path = destination_dir / "pyproject.toml.template"
    with pyproject_path.open("a") as pyproject_file:
        pyproject_file.write(ruff_template_path.read_text())
    ruff_template_path.unlink()

    subprocess.run(["uv", "sync"], cwd=destination_dir, check=True)


if __name__ == "__main__":
    app()
