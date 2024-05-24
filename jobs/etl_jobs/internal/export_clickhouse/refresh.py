import typer
from datetime import datetime
from utils import clickhouse_client, refresh_views


def run(
    table_name: str = typer.Option(
        ...,
        help="table_name",
    ),
):
    refresh_views(table_name)


if __name__ == "__main__":
    typer.run(run)
