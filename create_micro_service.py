import re
import shutil

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

    new_line = f"\tMICROSERVICE_PATH=jobs/ml_jobs/{service_name} PHYTON_VENV_VERSION=3.10.4 VENV_NAME=data-gcp-{service_name} REQUIREMENTS_NAME=requirements.txt RECREATE_VENV=$(CLEAN_INSTALL) make install_microservice\n"

    lines.insert(end_install_index, new_line)

    with open("Makefile", "w") as file:
        file.writelines(lines)


@app.command()
def create_micro_service(ms_name: str = typer.Option()):
    """
    Create a micro-service with the given name.

    Args:
        ms_name (str): The name of the micro-service. Must be in snake_case.

    Raises:
        ValueError: If the name is not in snake_case.

    Returns:
        None
    """

    # test if name is snake_case
    if not is_snake_case(ms_name):
        raise ValueError("ms_name must be snake_case")

    # Copying template
    template_dir = "jobs/ml_jobs/_template"
    destination_dir = f"jobs/ml_jobs/{ms_name}"
    ignore_patterns = "__pycache__", ".pytest_cache", ".vscode"

    # Copying template directory to destination directory
    shutil.copytree(
        template_dir,
        destination_dir,
        ignore=shutil.ignore_patterns(*ignore_patterns),
    )

    # Appending line in Makefile
    append_to_makefile(ms_name)


if __name__ == "__main__":
    app()
