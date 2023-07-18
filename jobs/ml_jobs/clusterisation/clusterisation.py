import typer
import pandas as pd
import json
from loguru import logger
from tools.clusterisation_tools import clusterisation_from_prebuild_encoding
from tools.utils import CONFIGS_PATH, ENV_SHORT_NAME


def clusterization(
    input_table: str = typer.Option(..., help="Path to data"),
    output_table: str = typer.Option(..., help="Path to data"),
    config_file_name: str = typer.Option(
        "default-config",
        help="Config file name",
    ),
):
    with open(
        f"{CONFIGS_PATH}/{config_file_name}.json",
        mode="r",
        encoding="utf-8",
    ) as config_file:
        params = json.load(config_file)
    logger.info("Loading data: fetch item with pretained encoding")
    item_full_encoding_enriched = pd.read_gbq(
        f"SELECT * from `tmp_{ENV_SHORT_NAME}.{input_table}`"
    )
    # Perform clusterisation
    logger.info("Perform clusterisation...")
    clusterisation_from_prebuild_encoding(
        item_full_encoding_enriched, params["target_nbclusters"], output_table
    )

    return


if __name__ == "__main__":
    typer.run(clusterization)
