import typer
import pandas as pd
import json
from loguru import logger
from tools.preprocessing import (
    prepare_embedding,
    get_item_by_categories,
    get_item_by_group,
)
from tools.utils import ENV_SHORT_NAME, CONFIGS_PATH

import numpy as np


def preprocess(
    input_table: str = typer.Option(..., help="Path to data"),
    output_table: str = typer.Option(..., help="Path to data"),
    config_file_name: str = typer.Option(
        "default-config-offer",
        help="Config file name",
    ),
):
    with open(
        f"{CONFIGS_PATH}/{config_file_name}.json",
        mode="r",
        encoding="utf-8",
    ) as config_file:
        params = json.load(config_file)

    logger.info("Loading data: fetch items with metadata and pretained embedding")
    items = pd.read_gbq(f"SELECT * from `tmp_{ENV_SHORT_NAME}.{input_table}`")
    items = items.drop_duplicates(subset=["item_id"], keep="first")

    logger.info("Build item groups...")
    item_clean = (
        items[
            [
                "item_id",
                "category",
                "subcategory_id",
                "offer_type_label",
                "offer_sub_type_label",
            ]
        ]
        .fillna("")
        .replace("None", "")
    )
    item_by_category = get_item_by_categories(item_clean)
    item_by_group = get_item_by_group(item_by_category, params["group_config"])

    logger.info("Prepare pretrained embedding...")
    embedding_col = [
        col
        for col in items.columns.tolist()
        if params["pretained_embedding_name"] in col
    ][0]

    embedding_clean = prepare_embedding(items[embedding_col].tolist())
    item_embedding = pd.concat([items[["item_id"]], embedding_clean], axis=1)
    logger.info(f"item_embedding: {len(item_embedding)}")

    item_embedding_w_group = pd.merge(
        item_embedding, item_by_group, how="inner", on=["item_id"]
    )
    item_embedding_w_group = item_embedding_w_group.fillna(0)
    item_embedding_w_group.to_gbq(
        f"tmp_{ENV_SHORT_NAME}.{output_table}", if_exists="replace"
    )

    return


if __name__ == "__main__":
    typer.run(preprocess)
