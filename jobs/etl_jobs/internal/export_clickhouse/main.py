from datetime import datetime

import typer

from core.update import (
    create_intermediate_schema,
    create_tmp_schema,
    update_incremental,
    update_overwrite,
)


def main_update(mode, source_gs_path, table_name, dataset_name, update_date):
    _id = datetime.now().strftime("%Y%m%d%H%M%S")
    tmp_table_name = f"{table_name}_{_id}"

    # import table in a tmp
    create_tmp_schema(
        sql_file_name=f"{dataset_name}_{table_name}",
        table_name=tmp_table_name,
        update_date=update_date,
        source_gs_path=source_gs_path,
    )

    # create table schema
    create_intermediate_schema(table_name, dataset_name)

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


def run(
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
    source_gs_path: str = typer.Option(
        None,
        help="source_gs_path",
    ),
):
    if mode in ("incremental", "overwrite"):
        if source_gs_path is None:
            raise Exception("source_gs_path should be specified")
        main_update(mode, source_gs_path, table_name, dataset_name, update_date)
    else:
        raise Exception(f"wrong specified mode, got {mode} ")


if __name__ == "__main__":
    typer.run(run)
