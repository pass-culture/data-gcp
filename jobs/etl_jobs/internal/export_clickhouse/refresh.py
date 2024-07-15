import typer
from core.utils import CLICKHOUSE_CLIENT
from core.fs import load_sql


def run(
    table_name: str = typer.Option(
        ...,
        help="table_name",
    ),
    folder: str = typer.Option(
        "analytics",
        help="folder_name",
    ),
):
    sql_query = load_sql(table_name=table_name, folder=folder)
    print("Will Execute:")
    print(sql_query)
    print(f"Refresh {table_name}...")
    CLICKHOUSE_CLIENT.command(sql_query)


if __name__ == "__main__":
    typer.run(run)
