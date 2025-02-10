import typer
from typing import Any
import json
from archive import store_partitions_in_temp, export_partitions, delete_partitions


def run(
    table: str = typer.Option(
        ...,
        help="Table name",
    ),
    config: str = typer.Option(
        ...,
        help="Config of the table",
    ),
):
    try:
        config_dict: dict[str, Any] = json.loads(config)  # Use safe JSON parsing
    except json.JSONDecodeError as e:
        typer.echo(f"Error: Invalid JSON config - {e}", err=True)
        raise typer.Exit(code=1)

    store_partitions_in_temp(table, config_dict)
    export_partitions(table, config_dict)
    delete_partitions(table, config_dict)


if __name__ == "__main__":
    typer.run(run)
