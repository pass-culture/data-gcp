from datetime import datetime
from utils import clickhouse_client, load_sql


def main_refresh(table_name, dataset_name):
    mv_table_name = f"{dataset_name}_{table_name}"
    clickhouse_client.command(
        f"DROP VIEW IF EXISTS views.{table_name} ON cluster default"
    )

    sql_query = load_sql(
        dataset_name=dataset_name, table_name=table_name, folder="views"
    )
    print(sql_query)
    print(f"Refresh Mat View {mv_table_name}...")
    clickhouse_client.command(sql_query)
