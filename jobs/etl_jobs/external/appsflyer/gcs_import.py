import logging
from datetime import datetime

import polars as pl
import pyarrow.dataset as pyd
import typer

from core.utils import export_polars_to_bq

logger = logging.getLogger(__name__)


def run(
    gcs_base_path: str = typer.Option(
        None,
        help="GCS base path to the data",
    ),
    prefix_table_name: str = typer.Option(
        ...,
        help="Nom de la table à importer",
    ),
    date: str = typer.Option(
        ...,
        help="Execution date",
    ),
):
    try:
        for f in ["channel", "geo", "summary-file"]:
            output_table = f"{prefix_table_name}_{f.replace('-', '_')}"

            dset = pyd.dataset(f"gs://{gcs_base_path}/{f}/", format="parquet")
            df = (
                pl.scan_pyarrow_dataset(dset)
                .collect()
                .with_columns(
                    execution_date=pl.lit(datetime.strptime(date, "%Y-%m-%d"))
                )
            )
            print(f"Importing {f}, date {date} to {output_table}.")
            export_polars_to_bq(
                df,
                event_date=date,
                output_table=output_table,
                partition_date="execution_date",
            )
    except typer.Exit:
        raise
    except Exception as e:
        logger.exception(f"ETL job failed: {e}")
        raise typer.Exit(code=1) from e


if __name__ == "__main__":
    typer.run(run)
