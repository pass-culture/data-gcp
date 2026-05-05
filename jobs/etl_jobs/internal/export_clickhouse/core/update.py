import logging
from datetime import datetime

import numpy as np
from clickhouse_connect.driver.client import Client as ClickhouseClient

from core.fs import load_sql

logger = logging.getLogger(__name__)


def update_incremental(
    client: ClickhouseClient, dataset_name: str, table_name: str, tmp_table_name: str
) -> None:
    partitions_to_update = client.query_df(
        f"SELECT distinct partition_date FROM tmp.{tmp_table_name}"
    )
    if len(partitions_to_update) > 0:
        partitions_to_update = [
            np.datetime64(date, "D").astype(datetime).strftime("%Y-%m-%d")
            for date in partitions_to_update["partition_date"].values
        ]
        logger.info(
            f"Will update {len(partitions_to_update)} partition of {dataset_name}.{table_name}. {partitions_to_update}"
        )
        for date in partitions_to_update:
            total_rows = (
                client.command(
                    f"SELECT count(*) FROM tmp.{tmp_table_name} WHERE partition_date = '{date}'"
                )
                or 0
            )
            previous_rows = (
                client.command(
                    f"SELECT count(*) FROM {dataset_name}.{table_name} WHERE partition_date = '{date}'"
                )
                or 0
            )
            if total_rows > 0:
                logger.info(
                    f"Updating partiton_date={date} for table {table_name}. Count {total_rows} (vs {previous_rows})"
                )
                update_sql = f""" ALTER TABLE {dataset_name}.{table_name} ON cluster default REPLACE PARTITION '{date}' FROM tmp.{tmp_table_name}"""
                logger.info(update_sql)
                client.command(update_sql)
    logger.info("Done updating. Removing temporary table.")
    client.command(f" DROP TABLE IF EXISTS tmp.{tmp_table_name} ON cluster default")


def remove_stale_partitions(
    client: ClickhouseClient, dataset_name: str, table_name: str, update_date: str
) -> None:
    previous_partitions = client.query_df(
        f"SELECT distinct update_date FROM {dataset_name}.{table_name}"
    )
    if len(previous_partitions) > 0:
        logger.info(
            f"Found {len(previous_partitions)} partitions, will remove old ones."
        )
        previous_partitions = [
            x for x in previous_partitions["update_date"].values if x != update_date
        ]
    else:
        previous_partitions = []

    for date in previous_partitions:
        logger.info(f"Removing partiton_date={date} for table {table_name}")
        client.command(
            f" ALTER TABLE {dataset_name}.{table_name} ON cluster default DROP PARTITION '{date}'"
        )


def update_overwrite(
    client: ClickhouseClient,
    dataset_name: str,
    table_name: str,
    tmp_table_name: str,
    update_date: str,
) -> None:
    """
    Overwrites data in `dataset_name.table_name` either by:
        - Replacing the specified partition (if the table is partitioned), or
        - Truncating and reloading the entire table (if it's single-partition).
    """
    logger.info(
        f"Will overwrite {dataset_name}.{table_name}. New update : {update_date}"
    )

    # 1) Inspect the table’s partition_key expression
    partition_key_expr = client.command(
        f"SELECT partition_key FROM system.tables "
        f"WHERE database='{dataset_name}' AND name='{table_name}'"
    ).strip()
    # 'tuple()' or empty means a single dummy partition
    num_parts = (
        0
        if not partition_key_expr or partition_key_expr == "tuple()"
        else partition_key_expr.count(",") + 1
    )

    if num_parts == 0:
        # Single-partition: drop and reload whole table
        client.command(f"TRUNCATE TABLE {dataset_name}.{table_name} ON CLUSTER default")
        client.command(
            f"INSERT INTO {dataset_name}.{table_name} "
            f"SELECT * FROM tmp.{tmp_table_name}"
        )
    else:
        # Multi-partition: replace only the date partition
        client.command(
            f"ALTER TABLE {dataset_name}.{table_name} ON CLUSTER default "
            f"REPLACE PARTITION '{update_date}' FROM tmp.{tmp_table_name}"
        )
        # Only here do we need to clear out old partitions
        remove_stale_partitions(client, dataset_name, table_name, update_date)

    # 2) Report final row count
    total_rows = (
        client.command(f"SELECT count(*) FROM {dataset_name}.{table_name}") or 0
    )
    logger.info(
        f"Done updating. Table contains {total_rows}. Removing temporary table."
    )

    # 3) Drop the temp table
    client.command(f"DROP TABLE tmp.{tmp_table_name} ON CLUSTER default")


def create_schema(client: ClickhouseClient, table_name: str, dataset_name: str) -> None:
    logger.info(
        f"Will create {dataset_name}.{table_name} schema on clickhouse cluster if new."
    )
    clickhouse_query = load_sql(
        table_name=table_name,
        extra_data={"dataset": dataset_name},
        folder=dataset_name,
    )
    client.command(clickhouse_query)
    logger.info("Done creating table schema.")


def create_tmp_schema(
    client: ClickhouseClient,
    sql_file_name: str,
    table_name: str,
    update_date: str,
    source_gs_path: str,
) -> None:
    client.command(f"DROP TABLE IF EXISTS tmp.{table_name} ON cluster default")

    sql_query = load_sql(
        table_name=sql_file_name,
        extra_data={
            "dataset": "tmp",
            "date": update_date,
            "tmp_table_name": table_name,
            "bucket_path": source_gs_path,
        },
        folder="tmp",
    )
    logger.info(sql_query)
    logger.info(f"Creating tmp.{table_name} table...")
    client.command(sql_query)
