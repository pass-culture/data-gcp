import re
import shutil
from enum import Enum
from typing import Annotated

import typer

app = typer.Typer()


def is_snake_case(s: str) -> bool:
    return re.fullmatch(r"^[a-z0-9_]+$", s) is not None


def append_to_makefile(service_name):
    with open("Makefile", "r") as file:
        lines = file.readlines()

    install_index = next(
        i for i, line in enumerate(lines) if line.startswith("install:")
    )

    # Find the end of the install target
    end_install_index = (
        next(
            (
                i
                for i, line in enumerate(
                    lines[install_index + 1 :], start=install_index + 1
                )
                if not line.startswith("\t") and line.strip()
            ),
            install_index + 1,
        )
        - 1
    )

    service_name_kebab = service_name.replace("_", "-")
    new_line = f"\tMICROSERVICE_PATH=jobs/ml_jobs/{service_name} PYTHON_VENV_VERSION=3.10.4 VENV_NAME=data-gcp-{service_name_kebab} REQUIREMENTS_NAME=requirements.txt RECREATE_VENV=$(CLEAN_INSTALL) make install_microservice\n"

    lines.insert(end_install_index, new_line)

    with open("Makefile", "w") as file:
        file.writelines(lines)


class MicroServiceType(str, Enum):
    ml = "ml"
    etl_external = "etl_external"
    etl_internal = "etl_internal"


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

    # Copying template
    ignore_patterns = "__pycache__", ".pytest_cache", ".vscode", ".ruff_cache"
    template_dir, destination_dir = get_template_and_destination_dir(ms_name, ms_type)

    # Copying template directory to destination directory
    shutil.copytree(
        template_dir,
        destination_dir,
        ignore=shutil.ignore_patterns(*ignore_patterns),
    )


def get_template_and_destination_dir(ms_name: str, ms_type: MicroServiceType):
    if ms_type == MicroServiceType.ml:
        template_dir = "jobs/ml_jobs/_template"
        destination_dir = f"jobs/ml_jobs/{ms_name}"
    elif ms_type == MicroServiceType.etl_external:
        template_dir = "jobs/etl_jobs/_template"
        destination_dir = f"jobs/etl_jobs/external/{ms_name}"
    elif ms_type == MicroServiceType.etl_internal:
        template_dir = "jobs/etl_jobs/_template"
        destination_dir = f"jobs/etl_jobs/internal/{ms_name}"

    return template_dir, destination_dir


if __name__ == "__main__":
    app()
