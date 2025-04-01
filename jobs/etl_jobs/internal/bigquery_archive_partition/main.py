import typer
from typing import Any
import json
from archive import Archive


def run(
    table: str = typer.Option(
        ...,
        help="Table name",
    ),
    config: str = typer.Option(
        ...,
        help="Config of the table",
    ),
    limit: int = typer.Option(
        1,
        help="Limit of partitions to export",
    ),
):
    try:
        config_dict: dict[str, Any] = json.loads(config)  # Use safe JSON parsing
    except json.JSONDecodeError as e:
        typer.echo(f"Error: Invalid JSON config - {e}", err=True)
        raise typer.Exit(code=1)

    archive = Archive(table, config_dict)
    archive.store_partitions_in_temp()
    archive.export_and_delete_partitions(limit)


if __name__ == "__main__":
    typer.run(run)
