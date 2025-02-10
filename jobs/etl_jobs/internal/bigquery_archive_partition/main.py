import typer
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
    store_partitions_in_temp(table, config)
    export_partitions(table, config)
    delete_partitions(table, config)


if __name__ == "__main__":
    typer.run(run)
