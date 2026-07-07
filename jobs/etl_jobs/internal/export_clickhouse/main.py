import json
import logging
from datetime import datetime
from typing import Optional

import typer
from clickhouse_connect.driver.client import Client as ClickhouseClient

from core.update import (
    create_schema,
    create_tmp_schema,
    update_incremental,
    update_overwrite,
)
from core.utils import get_clickhouse_client

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

app = typer.Typer()


def main_update(
    client: ClickhouseClient,
    mode: str,
    source_gs_path: str,
    table_name: str,
    dataset_name: str,
    update_date: str,
):
    _id = datetime.now().strftime("%Y%m%d%H%M%S")
    tmp_table_name = f"{table_name}_{_id}"

    # import table in a tmp
    create_tmp_schema(
        client=client,
        sql_file_name=f"{dataset_name}_{table_name}",
        table_name=tmp_table_name,
        update_date=update_date,
        source_gs_path=source_gs_path,
    )

    # create table schema
    create_schema(client, table_name, dataset_name)

    # update tables
    if mode == "incremental":
        update_incremental(
            client=client,
            dataset_name=dataset_name,
            table_name=table_name,
            tmp_table_name=tmp_table_name,
        )
    elif mode == "overwrite":
        update_overwrite(
            client=client,
            dataset_name=dataset_name,
            table_name=table_name,
            tmp_table_name=tmp_table_name,
            update_date=update_date,
        )
    else:
        raise ValueError(f"Mode unknown, got {mode!r}")


@app.command()
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
    ch_session_settings: Optional[str] = typer.Option(
        None, help="JSON string of ClickHouse session settings"
    ),
):
    parsed_settings = json.loads(ch_session_settings) if ch_session_settings else None

    if parsed_settings:
        logger.info(f"Using client session settings: {parsed_settings}")

    try:
        client = get_clickhouse_client(settings=parsed_settings)
    except Exception as e:
        logger.error(f"Failed to create ClickHouse client: {e}")
        raise RuntimeError("Failed to create ClickHouse client") from e

    if mode in ("incremental", "overwrite"):
        if source_gs_path is None:
            raise ValueError(
                "source_gs_path must be specified for incremental or overwrite mode"
            )
        main_update(client, mode, source_gs_path, table_name, dataset_name, update_date)
    else:
        raise ValueError(
            f"Invalid mode {mode!r}, expected 'incremental' or 'overwrite'"
        )


if __name__ == "__main__":
    app()
