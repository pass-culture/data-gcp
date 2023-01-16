import pandas as pd
import typer
from loguru import logger

from utils import STORAGE_PATH, ENV_SHORT_NAME


def main(
    dataset: str = typer.Option(
        f"tmp_{ENV_SHORT_NAME}",
        help="BigQuery dataset in which the training table is located",
    ),
    date: str = typer.Option(None, help="Date of the run of the pipeline"),
    split: str = typer.Option(
        "training",
        help="BigQuery table containing the data we want to load",
    ),
) -> None:
    query = f"""
        SELECT
            *
        FROM `{dataset}.{date}_recommendation_{split}_data`
    """
    dataset = pd.read_gbq(query)
    logger.info(f"Dataset size: {dataset.shape[0]}")
    dataset.to_csv(f"{STORAGE_PATH}/recommendation_{split}_data.csv", index=False)


if __name__ == "__main__":
    typer.run(main)
