import typer

from core.fs import load_sql
from core.utils import CLICKHOUSE_CLIENT, ENV_SHORT_NAME


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
    sql_query = load_sql(
        table_name=table_name,
        folder=folder,
        extra_data={"env_short_name": ENV_SHORT_NAME},
    )
    print("Will Execute:")
    print(sql_query)
    print(f"Refresh {table_name}...")
    CLICKHOUSE_CLIENT.command(sql_query)


if __name__ == "__main__":
    typer.run(run)
