from core.fs import load_sql
from core.utils import CLICKHOUSE_CLIENT


def update_incremental(dataset_name: str, table_name: str, tmp_table_name: str) -> None:
    partitions_to_update = CLICKHOUSE_CLIENT.query_df(
        f"SELECT distinct partition_date FROM tmp.{tmp_table_name}"
    )
    if len(partitions_to_update) > 0:
        partitions_to_update = [
            x for x in partitions_to_update["partition_date"].values
        ]
        print(
            f"Will update {len(partitions_to_update)} partition of {dataset_name}.{table_name}. {partitions_to_update}"
        )
        for date in partitions_to_update:
            total_rows = (
                CLICKHOUSE_CLIENT.command(
                    f"SELECT count(*) FROM tmp.{tmp_table_name} WHERE partition_date = '{date}'"
                )
                | 0
            )
            previous_rows = (
                CLICKHOUSE_CLIENT.command(
                    f"SELECT count(*) FROM {dataset_name}.{table_name} WHERE partition_date = '{date}'"
                )
                | 0
            )
            if total_rows > 0:
                print(
                    f"Updating partiton_date={date} for table {table_name}. Count {total_rows} (vs {previous_rows})"
                )
                update_sql = f""" ALTER TABLE {dataset_name}.{table_name} ON cluster default REPLACE PARTITION '{date}' FROM tmp.{tmp_table_name}"""
                print(update_sql)
                CLICKHOUSE_CLIENT.command(update_sql)
    print("Done updating. Removing temporary table.")
    CLICKHOUSE_CLIENT.command(
        f" DROP TABLE IF EXISTS tmp.{tmp_table_name} ON cluster default"
    )


def remove_stale_partitions(dataset_name, table_name, update_date) -> None:
    previous_partitions = CLICKHOUSE_CLIENT.query_df(
        f"SELECT distinct update_date FROM {dataset_name}.{table_name}"
    )
    if len(previous_partitions) > 0:
        print(f"Found {len(previous_partitions)} partitions, will remove old ones.")
        previous_partitions = [
            x for x in previous_partitions["update_date"].values if x != update_date
        ]
    else:
        previous_partitions = []

    for date in previous_partitions:
        print(f"Removing partiton_date={date} for table {table_name}")
        CLICKHOUSE_CLIENT.command(
            f" ALTER TABLE {dataset_name}.{table_name} ON cluster default DROP PARTITION '{date}'"
        )


def update_overwrite(
    dataset_name: str, table_name: str, tmp_table_name: str, update_date: str
) -> None:
    print(f"Will overwrite {dataset_name}.{table_name}. New update : {update_date}")
    CLICKHOUSE_CLIENT.command(
        f" ALTER TABLE {dataset_name}.{table_name} ON cluster default REPLACE PARTITION '{update_date}' FROM tmp.{tmp_table_name}"
    )

    remove_stale_partitions(dataset_name, table_name, update_date)
    total_rows = (
        CLICKHOUSE_CLIENT.command(f"SELECT count(*) FROM {dataset_name}.{table_name}")
        | 0
    )

    print(f"Done updating. Table contains {total_rows}. Removing temporary table.")
    CLICKHOUSE_CLIENT.command(f" DROP TABLE tmp.{tmp_table_name} ON cluster default")


def create_intermediate_schema(table_name: str, dataset_name: str) -> None:
    print(f"Will create intermediate.{table_name} schema on clickhouse cluster if new.")
    clickhouse_query = load_sql(
        table_name=table_name,
        extra_data={
            "dataset": "intermediate",
        },
        folder="intermediate",
    )
    CLICKHOUSE_CLIENT.command(clickhouse_query)
    print("Done creating table schema.")


def create_tmp_schema(
    sql_file_name: str, table_name: str, update_date: str, source_gs_path: str
) -> None:
    CLICKHOUSE_CLIENT.command(
        f"DROP TABLE IF EXISTS tmp.{table_name} ON cluster default"
    )

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
    print(sql_query)
    print(f"Creating tmp.{table_name} table...")
    CLICKHOUSE_CLIENT.command(sql_query)
