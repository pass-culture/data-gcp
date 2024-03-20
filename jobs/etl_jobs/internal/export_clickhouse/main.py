from utils import access_secret_data, PROJECT_NAME, ENV_SHORT_NAME
import typer
import time
import clickhouse_connect
from jinja2 import Template
from datetime import datetime
import uuid

BASE_DIR = "schema"

clickhouse_client = clickhouse_connect.get_client(
    host=access_secret_data(PROJECT_NAME, f"clickhouse_host_{ENV_SHORT_NAME}"),
    port=access_secret_data(PROJECT_NAME, f"clickhouse_port_{ENV_SHORT_NAME}"),
    username=access_secret_data(PROJECT_NAME, f"clickhouse_username_{ENV_SHORT_NAME}"),
    password=access_secret_data(PROJECT_NAME, f"clickhouse_password_{ENV_SHORT_NAME}"),
)


def load_sql(dataset_name: str, table_name: str, extra_data={}) -> str:
    with open(f"{BASE_DIR}/tmp/{dataset_name}_{table_name}.sql") as file:
        sql_template = file.read()
        return Template(sql_template).render(extra_data)


def update_incremental(
    dataset_name: str, table_name: str, tmp_table_name: str, update_date: str
) -> None:
    partitions_to_update = clickhouse_client.query_df(
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
                clickhouse_client.command(
                    f"SELECT count(*) FROM tmp.{tmp_table_name} WHERE partition_date = '{date}'"
                )
                | 0
            )
            previous_rows = (
                clickhouse_client.command(
                    f"SELECT count(*) FROM {dataset_name}.{table_name} WHERE partition_date = '{date}'"
                )
                | 0
            )
            if total_rows > 0:
                print(
                    f"Updating partiton_date={date} for table {table_name}. Count {total_rows} (vs {previous_rows})"
                )
                update_sql = f""" ALTER TABLE {dataset_name}.{table_name} REPLACE PARTITION '{date}' FROM tmp.{tmp_table_name}"""
                print(update_sql)
                clickhouse_client.command(update_sql)
    print(f"Done updating. Removing temporary table.")
    clickhouse_client.command(f" DROP TABLE tmp.{tmp_table_name}")


def remove_stale_partitions(dataset_name, table_name, update_date) -> None:
    previous_partitions = clickhouse_client.query_df(
        f"SELECT distinct update_date FROM {dataset_name}.{table_name}"
    )
    if len(previous_partitions) > 0:
        previous_partitions = [
            x for x in previous_partitions["update_date"].values if x != update_date
        ]
    else:
        previous_partitions = []

    for date in previous_partitions:
        print(f"Removing partiton_date={date} for table {table_name}")
        clickhouse_client.command(
            f" ALTER TABLE {dataset_name}.{table_name} DROP PARTITION '{date}'"
        )


def update_overwrite(
    dataset_name: str, table_name: str, tmp_table_name: str, update_date: str
) -> None:
    print(f"Will overwrite {dataset_name}.{table_name}. New update : {update_date}")
    clickhouse_client.command(
        f" ALTER TABLE {dataset_name}.{table_name} REPLACE PARTITION '{update_date}' FROM tmp.{tmp_table_name}"
    )

    remove_stale_partitions(dataset_name, table_name, update_date)

    print(f"Done updating. Removing temporary table.")
    clickhouse_client.command(f" DROP TABLE tmp.{tmp_table_name}")


def run(
    source_gs_path: str = typer.Option(
        ...,
        help="source_gs_path",
    ),
    table_name: str = typer.Option(
        ...,
        help="table_name",
    ),
    dataset_name: str = typer.Option(
        ...,
        help="dataset_name",
    ),
    update_date: str = typer.Option(
        ...,
        help="update_date",
    ),
    mode: str = typer.Option(
        "incremental",
        help="incremental / overwrite",
    ),
):
    _id = datetime.now().strftime("%Y%m%d%H%M%S")
    tmp_table_name = f"{table_name}_{_id}"

    # import table in a tmp
    clickhouse_client.command(f"DROP TABLE IF EXISTS tmp.{tmp_table_name}")

    sql_query = load_sql(
        dataset_name=dataset_name,
        table_name=table_name,
        extra_data={
            "dataset": "tmp",
            "date": update_date,
            "tmp_table_name": tmp_table_name,
            "bucket_path": source_gs_path,
        },
    )
    print(sql_query)
    print(f"Creating tmp.{tmp_table_name} table...")
    clickhouse_client.command(sql_query)
    # update tables
    if mode == "incremental":
        update_incremental(
            dataset_name=dataset_name,
            table_name=table_name,
            tmp_table_name=tmp_table_name,
            update_date=update_date,
        )
    elif mode == "overwrite":
        update_overwrite(
            dataset_name=dataset_name,
            table_name=table_name,
            tmp_table_name=tmp_table_name,
            update_date=update_date,
        )
    else:
        raise Exception(f"Mode unknown, got {mode}")


if __name__ == "__main__":
    typer.run(run)
