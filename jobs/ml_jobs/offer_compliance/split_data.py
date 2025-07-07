import numpy as np
import typer

from constants import (
    STORAGE_PATH,
)
from utils.data_collect_queries import read_from_gcs


def split_data(
    clean_table_name: str = typer.Option(
        "compliance_clean_data", help="BigQuery table containing compliance clean data"
    ),
    training_table_name: str = typer.Option(
        "compliance_training_data",
        help="BigQuery table containing compliance training data",
    ),
    validation_table_name: str = typer.Option(
        "compliance_validation_data",
        help="BigQuery table containing compliance validation data",
    ),
):
    clean_data = read_from_gcs(storage_path=STORAGE_PATH, table_name=clean_table_name)
    train_data, eval_data, test_data = np.split(
        clean_data.sample(frac=1),
        [int(0.8 * len(clean_data)), int(0.9 * len(clean_data))],
    )

    train_data.to_parquet(f"{STORAGE_PATH}/{training_table_name}/data.parquet")
    eval_data.to_parquet(f"{STORAGE_PATH}/{validation_table_name}/data.parquet")


if __name__ == "__main__":
    typer.run(split_data)
