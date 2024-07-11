import typer
from core.utils import clickhouse_client
from core.fs import load_sql


def refresh_views(view_name: str, folder: str) -> None:
    """Create or refresh Clickhouse View."""
    sql_query = load_sql(table_name=view_name, folder=folder)
    print("Will Execute:")
    print(sql_query)
    print(f"Refresh View {view_name}...")
    clickhouse_client.command(sql_query)


def run(
    view_name: str = typer.Option(
        ...,
        help="view_name",
    ),
    folder: str = typer.Option(
        "analytics",
        help="folder_name",
    ),
):
    refresh_views(view_name, folder)


if __name__ == "__main__":
    typer.run(run)
