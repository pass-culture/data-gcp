from utils import access_secret_data, PROJECT_NAME, ENV_SHORT_NAME
import typer
import time
import clickhouse_connect
from jinjasql import JinjaSql


clickhouse_client = clickhouse_connect.get_client(
    host=access_secret_data(PROJECT_NAME, f"clickhouse_host_{ENV_SHORT_NAME}"),
    port=access_secret_data(PROJECT_NAME, f"clickhouse_port_{ENV_SHORT_NAME}"),
    username=access_secret_data(PROJECT_NAME, f"clickhouse_username_{ENV_SHORT_NAME}"),
    password=access_secret_data(PROJECT_NAME, f"clickhouse_password_{ENV_SHORT_NAME}"),
)
jinja_client = JinjaSql()


def load_sql(dataset_name, table_name, extra_data={}) -> str:
    with open(f"tmp/{dataset_name}_{table_name}.sql") as file:
        sql_template = file.read()
        return jinja_client.prepare_query(sql_template, extra_data)


def update_incremental(dataset_name: str, table_name: str, update_date: str) -> None:
    partitions_to_update = clickhouse_client.command(
        f"SELECT distinct partition_date FROM tmp.{table_name}_{update_date}"
    )
    print(
        f"Will update {len(partitions_to_update)} partition of {dataset_name}.{table_name}. [{partitions_to_update}]"
    )
    for date in partitions_to_update:
        total_rows = (
            clickhouse_client.command(
                f"SELECT count(*) FROM tmp.{table_name}_{update_date} WHERE partition_date = {date}"
            )
            | 0
        )
        previous_rows = (
            clickhouse_client.command(
                f"SELECT count(*) FROM {dataset_name}.{table_name} WHERE partition_date = {date}"
            )
            | 0
        )
        print(
            f"Updating partiton_date={date} for table {table_name}. Count {total_rows} (vs {previous_rows})"
        )

        clickhouse_client.command(
            f" ALTER TABLE {dataset_name}.{table_name} REPLACE PARTITION '{date}' FROM tmp.{table_name}_{date} WHERE partition_date = {date}"
        )
    print(f"Done updating. Removing temporary table.")
    clickhouse_client.command(f" DROP TABLE tmp.{table_name}_{update_date}")


def update_overwrite(dataset_name: str, table_name: str, update_date: str) -> None:
    previous_partitions = clickhouse_client.command(
        f"SELECT distinct update_date FROM {dataset_name}.{table_name}"
    )
    previous_partitions = [x for x in previous_partitions if x != update_date]
    print(f"Will overwrite {dataset_name}.{table_name}. New update : {update_date}")
    clickhouse_client.command(
        f" ALTER TABLE {dataset_name}.{table_name} REPLACE PARTITION '{update_date}' FROM tmp.{table_name}_{update_date}"
    )

    for date in previous_partitions:
        print(f"Removing partiton_date={date} for table {table_name}")
        clickhouse_client.command(
            f" DROP TABLE {dataset_name}.{table_name} DROP PARTITION '{date}'"
        )

    print(f"Done updating. Removing temporary table.")
    clickhouse_client.command(f" DROP TABLE tmp.{table_name}_{update_date}")


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

    # import table in a tmp
    sql_query = load_sql(
        table_name,
        data={
            "dataset": dataset_name,
            "date": update_date,
            "bucket_path": source_gs_path,
        },
    )
    clickhouse_client.command(sql_query)
    # update production tables
    if mode == "incremental":
        update_incremental(dataset_name, table_name, update_date)
    elif mode == "overwrite":
        update_overwrite(dataset_name, table_name, update_date)
    else:
        raise Exception(f"Mode unknown, got {mode}")


if __name__ == "__main__":
    typer.run(run)
