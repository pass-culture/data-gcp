from utils import (
    get_datasets_to_scan,
    get_last_update_date,
    get_table_schedule,
    schedule_mapping,
    table_name_contains_partition_date,
)
import re
import typer
import pandas as pd


def run():
    datasets_to_scan = get_datasets_to_scan()

    table_last_update_df = get_last_update_date(datasets_to_scan)

    table_schedule_df = get_table_schedule()

    df = table_last_update_df.merge(
        table_schedule_df, on=["table_schema", "table_name"], how="left"
    ).assign(
        full_table_name=lambda _df: _df["table_schema"] + "." + _df["table_name"],
        schedule_tag=lambda _df: _df["schedule_tag"].fillna("default"),
        last_modified_time=lambda _df: pd.to_datetime(
            _df["last_modified_time"]
        ).dt.tz_localize(None),
        is_partition_table=lambda _df: _df["table_name"].apply(
            table_name_contains_partition_date
        ),
    )

    warning_tables = df[
        df["last_modified_time"] < df["schedule_tag"].map(schedule_mapping)
    ]

    warning_tables = warning_tables[warning_tables.is_partition_table == False]

    warning_tables_list = warning_tables["full_table_name"].to_list()

    print(f"{warning_tables_list}")

    return f"{warning_tables_list}"


if __name__ == "__main__":
    typer.run(run)
