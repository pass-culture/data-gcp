from typing import Dict, List

import typer
from clickhouse_driver import Client

from core.fs import load_sql
from core.utils import CLICKHOUSE_CLIENT


def get_shards(clickhouse_client: Client, cluster_name: str) -> List[Dict[str, str]]:
    """
    Retrieves shard information (shard number, host, and port) for a specified ClickHouse cluster.

    Args:
        clickhouse_client (Client): An active ClickHouse client connection.
        cluster_name (str): The name of the cluster to retrieve shard details for.

    Returns:
        List[Dict[str, str]]: A list of dictionaries containing shard number, host address, and port.
    """
    shards_query = f"""
        SELECT shard_num, host_address, port
        FROM system.clusters
        WHERE cluster = '{cluster_name}'
        GROUP BY shard_num, host_address, port
        ORDER BY shard_num
    """
    result = clickhouse_client.execute(shards_query)
    return [
        {"shard_num": shard_num, "host": host_address, "port": port}
        for shard_num, host_address, port in result
    ]


def execute_on_each_shard(sql_query: str, shards: List[Dict[str, str]]) -> None:
    """
    Executes a given SQL query sequentially on each shard in a ClickHouse cluster.

    Args:
        sql_query (str): The SQL query to be executed on each shard.
        shards (List[Dict[str, str]]): A list of dictionaries containing shard information (shard_num, host, port).

    Returns:
        None
    """
    for shard in shards:
        print(
            f"Executing on shard {shard['shard_num']} at {shard['host']}:{shard['port']}"
        )
        shard_client = Client(host=shard["host"], port=shard["port"])
        shard_client.execute(sql_query)
        print(f"Executed on shard {shard['shard_num']}")

    print("Query executed on all shards successfully.")


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

    print(f"Will Execute:\n{sql_query}")

    # Define your ClickHouse cluster name
    cluster_name = "default"  # Replace with your actual cluster name

    shards = get_shards(CLICKHOUSE_CLIENT, cluster_name)

    # Execute the CREATE TABLE query on each shard manually
    execute_on_each_shard(sql_query, shards)


if __name__ == "__main__":
    typer.run(run)
